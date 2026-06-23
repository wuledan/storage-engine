// memory_pool.h — Tiered memory pool
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <coroutine>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace storage::runtime::adapt {

// ── Small object cache config ──
struct SmallObjectConfig {
    uint32_t max_object_size = 256;       // Objects larger go to central pool
    uint32_t slot_count = 8;             // Size class count (8/16/32/64/128/256)
    uint32_t per_thread_cache_lines = 64; // Thread-local cache lines
};

// ── Memory pool stats ──
struct MemoryPoolStats {
    size_t total_allocated;
    size_t total_freed;
    size_t current_in_use;
    size_t peak_usage;
    uint64_t alloc_count;
    uint64_t free_count;
    uint64_t cache_hit_count;
    uint64_t cache_miss_count;
    double cache_hit_rate() const noexcept {
        uint64_t total = cache_hit_count + cache_miss_count;
        return total == 0 ? 0.0 : static_cast<double>(cache_hit_count) / total;
    }
};

// ── Main memory pool ──
class QuantMemoryResource {  // ← no longer inherits std::pmr::memory_resource
public:
    explicit QuantMemoryResource(const SmallObjectConfig& cfg = {});
    ~QuantMemoryResource();

    // ── Sync interface (per-Worker, single-threaded, no contention) ──
    void* allocate(size_t bytes, size_t alignment = alignof(std::max_align_t));
    void deallocate(void* ptr, size_t bytes, size_t alignment = alignof(std::max_align_t));

    // ── Coroutine interface (per-NUMA shared, needs co_lock in future) ──
    // TODO: co_allocate/co_deallocate with AffinityMutex co_lock for NUMA pools
    // For now, these delegate to the sync path since per-Worker is single-threaded.

    // ── Warmup ──
    void warmup(size_t total_bytes);

    // ── Stats ──
    MemoryPoolStats stats() const noexcept;

    // ── Reset ──
    void reset() noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

// ── Global singleton ──
QuantMemoryResource& global_memory_resource();

// ── Convenience ──
template<typename T, typename... Args>
std::unique_ptr<T> make_quant(Args&&... args) {
    return std::make_unique<T>(std::forward<Args>(args)...);
}

}  // namespace storage::runtime::adapt
