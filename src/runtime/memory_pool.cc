// memory_pool.cc — Tiered memory pool implementation
#include "memory_pool.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <new>

namespace storage::runtime::adapt {

// ── Size class calculation ──
// Size classes: 8, 16, 32, 64, 128, 256 bytes
// Each class holds objects of that size, aligned to that size.
static constexpr size_t kSizeClasses[] = {8, 16, 32, 64, 128, 256};
static constexpr size_t kNumSizeClasses = sizeof(kSizeClasses) / sizeof(kSizeClasses[0]);
static constexpr size_t kSmallObjectMax = 256;
static constexpr size_t kMediumObjectMax = 4096;
static constexpr size_t kBlockSize = 1024 * 1024;  // 1MB blocks from OS
static constexpr size_t kBlockAlignment = 64;       // cache-line aligned blocks

static char* alloc_block() {
    return static_cast<char*>(::operator new(kBlockSize, std::align_val_t(kBlockAlignment)));
}

static void free_block(void* p) {
    ::operator delete(p, std::align_val_t(kBlockAlignment));
}

// Size classes are powers of 2: 8→0, 16→1, 32→2, 64→3, 128→4, 256→5.
// Round size up to the next power of 2, then map via ctz.
static size_t size_class_index(size_t size) {
    if (size <= 8) return 0;
    // Round up to the next power of 2 (for non-power-of-2 sizes)
    size_t s = size - 1;
    if constexpr (sizeof(size_t) == 8) {
        s |= s >> 1;  s |= s >> 2;  s |= s >> 4;
        s |= s >> 8;  s |= s >> 16; s |= s >> 32;
    } else {
        s |= s >> 1;  s |= s >> 2;  s |= s >> 4;
        s |= s >> 8;  s |= s >> 16;
    }
    s += 1;
    // ctz: 8=2^3→idx 0, 16=2^4→idx 1, ..., 256=2^8→idx 5
    const size_t idx = __builtin_ctzll(s) - 3;
    return idx < kNumSizeClasses ? idx : kNumSizeClasses;
}

// ── Lock-free MPMC bounded ring buffer ──
// Dmitry Vyukov-style bounded MPMC queue for void*.
// Capacity must be a power of two.
template<size_t Capacity = 256>
class MPMCRing {
    static_assert((Capacity & (Capacity - 1)) == 0,
                  "MPMCRing capacity must be a power of two");

    struct Cell {
        std::atomic<size_t> sequence{0};
        std::atomic<void*> data{nullptr};
    };

    static constexpr size_t kMask = Capacity - 1;
    std::array<Cell, Capacity> buffer_;
    alignas(64) std::atomic<size_t> head_{0};
    alignas(64) std::atomic<size_t> tail_{0};

public:
    MPMCRing() {
        for (size_t i = 0; i < Capacity; ++i) {
            buffer_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    // Attempt to enqueue ptr. Returns true on success, false if full.
    bool try_enqueue(void* ptr) noexcept {
        size_t tail = tail_.load(std::memory_order_relaxed);
        for (;;) {
            Cell& cell = buffer_[tail & kMask];
            size_t seq = cell.sequence.load(std::memory_order_acquire);
            intptr_t diff = static_cast<intptr_t>(seq) -
                            static_cast<intptr_t>(tail);
            if (diff < 0) return false;         // full
            if (diff != 0) {
                // Concurrent claimant won; reload tail
                tail = tail_.load(std::memory_order_relaxed);
                continue;
            }
            // We own this slot — try to claim it
            if (tail_.compare_exchange_weak(tail, tail + 1,
                    std::memory_order_relaxed,
                    std::memory_order_relaxed)) {
                cell.data.store(ptr, std::memory_order_release);
                cell.sequence.store(tail + 1, std::memory_order_release);
                return true;
            }
            // CAS failed — retry with fresh tail
        }
    }

    // Attempt to dequeue a pointer into ptr. Returns true on success.
    bool try_dequeue(void*& ptr) noexcept {
        size_t head = head_.load(std::memory_order_relaxed);
        for (;;) {
            Cell& cell = buffer_[head & kMask];
            size_t seq = cell.sequence.load(std::memory_order_acquire);
            intptr_t diff = static_cast<intptr_t>(seq) -
                            static_cast<intptr_t>(head + 1);
            if (diff < 0) return false;         // empty
            if (diff != 0) {
                head = head_.load(std::memory_order_relaxed);
                continue;
            }
            // We own this slot — try to claim it
            if (head_.compare_exchange_weak(head, head + 1,
                    std::memory_order_relaxed,
                    std::memory_order_relaxed)) {
                ptr = cell.data.load(std::memory_order_acquire);
                cell.sequence.store(head + Capacity, std::memory_order_release);
                return true;
            }
        }
    }
};

// ── Per-size-class free list (lock-free, CAS-based) ──
class SizeClassFreeList {
public:
    struct Node {
        Node* next{nullptr};
    };

    void push(void* ptr) {
        auto* node = static_cast<Node*>(ptr);
        auto* old = head_.load(std::memory_order_acquire);
        do {
            node->next = old;
        } while (!head_.compare_exchange_weak(old, node,
                std::memory_order_release,
                std::memory_order_acquire));
        count_.fetch_add(1, std::memory_order_relaxed);
    }

    void* pop() {
        auto* old = head_.load(std::memory_order_acquire);
        while (old) {
            if (head_.compare_exchange_weak(old, old->next,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                count_.fetch_sub(1, std::memory_order_relaxed);
                return old;
            }
            // CAS failed — old has been reloaded by compare_exchange_weak
        }
        return nullptr;
    }

    bool empty() const { return head_.load(std::memory_order_acquire) == nullptr; }
    size_t count() const { return count_.load(std::memory_order_relaxed); }

private:
    std::atomic<Node*> head_{nullptr};
    std::atomic<size_t> count_{0};
};

// ── Central free list for medium objects (up to 4KB), lock-free ──
class CentralFreeList {
public:
    void push(void* ptr, size_t size) {
        auto* node = static_cast<FreeNode*>(ptr);
        node->size = size;
        auto* old = head_.load(std::memory_order_acquire);
        do {
            node->next = old;
        } while (!head_.compare_exchange_weak(old, node,
                std::memory_order_release,
                std::memory_order_acquire));
    }

    // Drain the entire chain, find a node >= min_size,
    // then push remaining nodes back onto the list.
    void* pop(size_t min_size) {
        auto* chain = head_.exchange(nullptr, std::memory_order_acq_rel);
        if (!chain) return nullptr;

        FreeNode* found = nullptr;
        FreeNode* prev = nullptr;
        FreeNode* curr = chain;

        while (curr) {
            if (!found && curr->size >= min_size) {
                // Unlink this node
                found = curr;
                if (prev) {
                    prev->next = curr->next;
                } else {
                    chain = curr->next;
                }
            } else {
                prev = curr;
            }
            curr = curr->next;
        }

        // Push back the remaining chain
        if (chain) {
            // Find the tail of the remaining chain
            FreeNode* tail = chain;
            while (tail->next) tail = tail->next;

            auto* old = head_.load(std::memory_order_acquire);
            do {
                tail->next = old;
            } while (!head_.compare_exchange_weak(old, chain,
                    std::memory_order_release,
                    std::memory_order_acquire));
        }

        return found;
    }

private:
    struct FreeNode {
        size_t size;
        FreeNode* next;
    };
    std::atomic<FreeNode*> head_{nullptr};
};

// ── Thread-local cache ──
// Inline free-list for TLS: no atomic, no unique_ptr indirection.
class ThreadLocalCache {
public:
    struct LocalFreeList {
        SizeClassFreeList::Node* head{nullptr};
        size_t count{0};    // plain size_t — TLS is single-threaded, no atomic needed
    };

    ThreadLocalCache() = default;

    void* allocate(size_t size_class_idx) {
        if (size_class_idx < kNumSizeClasses) {
            auto& list = free_lists_[size_class_idx];
            if (!list.head) return nullptr;
            auto* node = list.head;
            list.head = list.head->next;
            list.count--;
            return node;
        }
        return nullptr;
    }

    void deallocate(void* ptr, size_t size_class_idx) {
        if (size_class_idx < kNumSizeClasses) {
            auto* node = static_cast<SizeClassFreeList::Node*>(ptr);
            auto& list = free_lists_[size_class_idx];
            node->next = list.head;
            list.head = node;
            list.count++;
        }
    }

    size_t list_count(size_t class_idx) const {
        if (class_idx < kNumSizeClasses) {
            return free_lists_[class_idx].count;
        }
        return 0;
    }

private:
    LocalFreeList free_lists_[kNumSizeClasses];  // inline array, no heap indirection
};

// ── QuantMemoryResource::Impl ──
class QuantMemoryResource::Impl {
public:
    explicit Impl(const SmallObjectConfig& cfg)
        : config_(cfg)
        , small_free_lists_{}
        , central_free_list_{}
    {
        for (size_t i = 0; i < kNumSizeClasses; ++i) {
            small_free_lists_[i] = std::make_unique<SizeClassFreeList>();
        }
        // Pre-allocate initial blocks
        constexpr size_t kInitialBlocks = 4;
        for (size_t i = 0; i < kInitialBlocks; ++i) {
            preallocated_blocks_.push_back(alloc_block());
        }
        // Initialize bump allocator from first block
        if (!preallocated_blocks_.empty()) {
            bump_block_ptr_ = preallocated_blocks_[0];
            bump_block_idx_ = 0;
            bump_offset_ = 0;
        }
    }

    ~Impl() {
        // Free pre-allocated blocks
        for (auto* block : preallocated_blocks_) {
            if (block) free_block(block);
        }
        // Free any individually allocated medium-object blocks
        for (auto* block : allocated_blocks_) {
            ::operator delete(block);
        }
        allocated_blocks_.clear();
    }

    void* allocate(size_t bytes, size_t alignment) {
        stats_.alloc_count.fetch_add(1, std::memory_order_relaxed);
        size_t alloc_size = bytes;

        // Align allocation size
        if (alignment > alignof(std::max_align_t)) {
            alloc_size = (alloc_size + alignment - 1) & ~(alignment - 1);
        }

        void* ptr = nullptr;

        // Small object path (<= 256 bytes)
        if (alloc_size <= kSmallObjectMax) {
            size_t class_idx = size_class_index(alloc_size);
            size_t class_size = kSizeClasses[class_idx];

            // Try thread-local cache first (no lock needed)
            thread_local ThreadLocalCache tls_cache;
            ptr = tls_cache.allocate(class_idx);
            if (ptr) {
                stats_.cache_hit_count.fetch_add(1, std::memory_order_relaxed);
                return ptr;
            }

            // Drain return ring → central freelist (batch-transfer to reduce CAS contention)
            void* drained;
            int drain_count = 0;
            while (return_rings_[class_idx].try_dequeue(drained)) {
                small_free_lists_[class_idx]->push(drained);
                if (++drain_count >= 16) break;  // starvation guard
            }

            // Try central free list (lock-free CAS pop)
            ptr = small_free_lists_[class_idx]->pop();
            if (ptr) {
                stats_.cache_hit_count.fetch_add(1, std::memory_order_relaxed);
                return ptr;
            }

            // Central cache miss — allocate from pre-allocated block
            stats_.cache_miss_count.fetch_add(1, std::memory_order_relaxed);
            ptr = allocate_from_block(class_size, alignment);
            if (ptr) return ptr;

            // Should not reach here (allocate_from_block always succeeds)
            return nullptr;
        }

        // Medium object path (<= 4KB)
        if (alloc_size <= kMediumObjectMax) {
            ptr = central_free_list_.pop(alloc_size);
            if (ptr) {
                stats_.cache_hit_count.fetch_add(1, std::memory_order_relaxed);
                return ptr;
            }
            stats_.cache_miss_count.fetch_add(1, std::memory_order_relaxed);
            ptr = ::operator new(alloc_size);
            allocated_blocks_.push_back(ptr);
            return ptr;
        }

        // Large object path — direct allocation
        // Not tracked in allocated_blocks_ since deallocate() frees directly
        stats_.cache_miss_count.fetch_add(1, std::memory_order_relaxed);
        ptr = ::operator new(alloc_size);
        return ptr;
    }

    void deallocate(void* ptr, size_t bytes, size_t alignment) {
        stats_.free_count.fetch_add(1, std::memory_order_relaxed);
        size_t alloc_size = bytes;

        if (alignment > alignof(std::max_align_t)) {
            alloc_size = (alloc_size + alignment - 1) & ~(alignment - 1);
        }

        // Small object — return to thread-local cache (or overflow to return ring)
        if (alloc_size <= kSmallObjectMax) {
            size_t class_idx = size_class_index(alloc_size);
            thread_local ThreadLocalCache tls_cache;
            // If TLS cache is getting full, spill to the lock-free return ring
            // to avoid hoarding too much memory per thread.
            if (tls_cache.list_count(class_idx) > 64) {
                return_rings_[class_idx].try_enqueue(ptr);
            } else {
                tls_cache.deallocate(ptr, class_idx);
            }
            return;
        }

        // Medium object — return to central free list (lock-free)
        if (alloc_size <= kMediumObjectMax) {
            central_free_list_.push(ptr, alloc_size);
            return;
        }

        // Large object — direct free
        ::operator delete(ptr);
    }

    void warmup(size_t total_bytes) {
        size_t blocks_needed = (total_bytes + kBlockSize - 1) / kBlockSize;
        for (size_t i = 0; i < blocks_needed; ++i) {
            void* block = alloc_block();
            preallocated_blocks_.push_back(static_cast<char*>(block));
        }
    }

    MemoryPoolStats stats() const noexcept {
        MemoryPoolStats s;
        s.alloc_count = stats_.alloc_count.load(std::memory_order_relaxed);
        s.free_count = stats_.free_count.load(std::memory_order_relaxed);
        s.cache_hit_count = stats_.cache_hit_count.load(std::memory_order_relaxed);
        s.cache_miss_count = stats_.cache_miss_count.load(std::memory_order_relaxed);
        s.total_allocated = stats_.total_allocated.load(std::memory_order_relaxed);
        s.total_freed = stats_.total_freed.load(std::memory_order_relaxed);
        s.current_in_use = s.total_allocated - s.total_freed;
        s.peak_usage = stats_.peak_usage.load(std::memory_order_relaxed);
        return s;
    }

    void reset() noexcept {
        // Don't free underlying blocks — just clear free lists
        for (size_t i = 0; i < kNumSizeClasses; ++i) {
            // Note: This is a simple implementation. Production would
            // track objects more carefully.
        }
    }

private:
    void* allocate_from_block(size_t size, size_t alignment = 16) {
        // Align bump offset to satisfy the requested alignment
        size_t align = std::max(alignment, size_t{16});
        bump_offset_ = (bump_offset_ + align - 1) & ~(align - 1);
        size = (size + align - 1) & ~(align - 1);

        // Try current bump block
        if (bump_block_ptr_ && bump_offset_ + size <= kBlockSize) {
            void* ptr = bump_block_ptr_ + bump_offset_;
            bump_offset_ += size;
            stats_.total_allocated.fetch_add(size, std::memory_order_relaxed);
            return ptr;
        }

        // Check if there's another pre-allocated block
        for (++bump_block_idx_; bump_block_idx_ < preallocated_blocks_.size(); ++bump_block_idx_) {
            auto* block = preallocated_blocks_[bump_block_idx_];
            if (block) {
                bump_block_ptr_ = block;
                bump_offset_ = size;
                stats_.total_allocated.fetch_add(size, std::memory_order_relaxed);
                return block;
            }
        }

        // Allocate a new block
        char* new_block = alloc_block();
        preallocated_blocks_.push_back(new_block);
        bump_block_ptr_ = new_block;
        bump_offset_ = size;
        bump_block_idx_ = preallocated_blocks_.size() - 1;
        stats_.total_allocated.fetch_add(size, std::memory_order_relaxed);
        return new_block;
    }

    SmallObjectConfig config_;
    std::array<std::unique_ptr<SizeClassFreeList>, kNumSizeClasses> small_free_lists_;
    MPMCRing<> return_rings_[kNumSizeClasses];
    std::vector<char*> preallocated_blocks_;
    char* bump_block_ptr_ = nullptr;
    size_t bump_offset_ = 0;
    size_t bump_block_idx_ = static_cast<size_t>(-1);
    CentralFreeList central_free_list_;
    std::vector<void*> allocated_blocks_;

    struct AtomicStats {
        std::atomic<uint64_t> alloc_count{0};
        std::atomic<uint64_t> free_count{0};
        std::atomic<uint64_t> cache_hit_count{0};
        std::atomic<uint64_t> cache_miss_count{0};
        std::atomic<size_t> total_allocated{0};
        std::atomic<size_t> total_freed{0};
        std::atomic<size_t> peak_usage{0};
    };
    AtomicStats stats_;
};

QuantMemoryResource::QuantMemoryResource(const SmallObjectConfig& cfg)
    : impl_(std::make_unique<Impl>(cfg)) {}

QuantMemoryResource::~QuantMemoryResource() = default;

void QuantMemoryResource::warmup(size_t total_bytes) {
    impl_->warmup(total_bytes);
}

MemoryPoolStats QuantMemoryResource::stats() const noexcept {
    return impl_->stats();
}

void QuantMemoryResource::reset() noexcept {
    impl_->reset();
}

void* QuantMemoryResource::allocate(size_t bytes, size_t alignment) {
    return impl_->allocate(bytes, alignment);
}

void QuantMemoryResource::deallocate(void* ptr, size_t bytes, size_t alignment) {
    impl_->deallocate(ptr, bytes, alignment);
}

QuantMemoryResource& global_memory_resource() {
    static QuantMemoryResource instance;
    return instance;
}

}  // namespace storage::runtime::adapt
