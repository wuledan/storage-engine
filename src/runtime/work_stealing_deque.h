#pragma once
#include "chase_lev_deque.h"
#include "work_item.h"
#include <atomic>
#include <cstddef>

namespace storage::runtime {

// Thin adapter: re-exports the original Chase-Lev deque implementation
// as WorkStealingDeque for backward compatibility.
//
// The underlying ChaseLevDeque<WorkItem> uses atomic indices (top_/bottom_)
// with a plain vector<WorkItem> for storage.  This avoids std::atomic<WorkItem>
// which is not lock-free for 32-byte WorkItem.
//
// All semantics are identical to the classic Chase-Lev algorithm (SPAA 2005):
//   - Owner (single thread): push() and pop() at the bottom.
//   - Thieves (multiple threads): steal() from the top.
//
// Multi-producer extension: push() and pop() are serialized by a lightweight
// spinlock (both modify bottom_).  Steal() remains lock-free (CAS on top_).
class WorkStealingDeque {
public:
    static constexpr size_t kDefaultCapacity = 256;
    static constexpr size_t kMinCapacity = 64;

    explicit WorkStealingDeque(size_t capacity = kDefaultCapacity)
        : deque_(next_pow2(std::max(capacity, kMinCapacity))) {}

    ~WorkStealingDeque() = default;

    // ── Owner operations (single thread, non-atomic bottom) ──
    //
    // Note: push() and pop() are both serialized by a lightweight spinlock
    // because external threads may call push() concurrently with the owner's
    // pop().  Steal() remains lock-free.

    // Push item at the bottom. Grow if full.  Multi-producer safe.
    void push(WorkItem item) {
        while (push_pop_lock_.test_and_set(std::memory_order_acquire)) {
            __builtin_ia32_pause();
        }
        deque_.push(std::move(item));
        push_pop_lock_.clear(std::memory_order_release);
    }

    // Pop item from the bottom. Returns true if successful.  Multi-producer safe
    // (serializes with concurrent push() from external threads).
    [[nodiscard]] bool pop(WorkItem& out) {
        while (push_pop_lock_.test_and_set(std::memory_order_acquire)) {
            __builtin_ia32_pause();
        }
        bool ok = deque_.pop(out);
        push_pop_lock_.clear(std::memory_order_release);
        return ok;
    }

    // ── Thief operations (multi-thread safe, atomic top) ──

    // Steal item from the top. Returns true if successful.
    // Multiple thieves can call concurrently.  Lock-free.
    [[nodiscard]] bool steal(WorkItem& out) { return deque_.steal(out); }

    // ── Query ──

    size_t size() const noexcept { return deque_.size(); }
    bool empty() const noexcept { return deque_.empty(); }

private:
    static size_t next_pow2(size_t v) {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v |= v >> 32;
        v++;
        return v;
    }

    std::atomic_flag push_pop_lock_ = ATOMIC_FLAG_INIT;
    ChaseLevDeque<WorkItem> deque_;
};

}  // namespace storage::runtime
