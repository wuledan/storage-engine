#pragma once
#include <atomic>
#include <array>
#include <cstddef>
#include <cstdint>
#include <algorithm>
#include <type_traits>

namespace storage::runtime {

/**
 * 批量 SPSC 队列：生产者单线程、消费者单线程。
 * 生产者整批写入，一次 atomic store-release 发布。
 * 消费者一次 atomic load-acquire 读取一批。
 */
template <typename T, size_t Capacity = 4096>
class BatchedSPSCQueue {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of 2");
    static_assert(std::is_trivially_copyable_v<T> || std::is_move_constructible_v<T>,
                  "T must be trivially copyable or move constructible");

public:
    /**
     * 生产者调用：整批写入 count 个元素，然后一次原子发布。
     */
    void push_batch(const T* items, size_t count) noexcept {
        size_t write_idx = local_write_tail_;
        for (size_t i = 0; i < count; ++i) {
            ring_[write_idx & (Capacity - 1)] = items[i];
            ++write_idx;
        }
        local_write_tail_ = write_idx;
        write_head_.fetch_add(count, std::memory_order_release);
    }

    /**
     * 消费者调用：一次原子读取 head，然后批量拷贝到本地。
     */
    size_t try_dequeue_batch(T* items, size_t max) noexcept {
        size_t head = write_head_.load(std::memory_order_acquire);
        size_t available = head - local_tail_;
        size_t n = std::min(available, max);
        for (size_t i = 0; i < n; ++i) {
            items[i] = std::move(ring_[local_tail_ & (Capacity - 1)]);
            ++local_tail_;
        }
        return n;
    }

    /**
     * 可用任务数近似值（消费者侧视角）。
     */
    size_t approx_count() const noexcept {
        size_t head = write_head_.load(std::memory_order_acquire);
        return head - local_tail_;
    }

private:
    alignas(64) std::array<T, Capacity> ring_;
    alignas(64) std::atomic<size_t> write_head_{0};   // 仅生产者原子写
    alignas(64) size_t local_write_tail_{0};           // 仅生产者本地
    alignas(64) size_t local_tail_{0};                 // 仅消费者本地
};

}  // namespace storage::runtime
