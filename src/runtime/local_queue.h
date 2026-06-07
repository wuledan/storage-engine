#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace storage::runtime {

/**
 * 完全非原子的环形缓冲队列，仅允许单线程访问。
 * 无任何 std::atomic 操作，head_/tail_ 为平凡 size_t。
 */
template <typename T, size_t Capacity = 1024>
class LocalQueue {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of 2");
    static_assert(std::is_trivially_copyable_v<T> || std::is_move_constructible_v<T>,
                  "T must be trivially copyable or move constructible");

public:
    bool try_enqueue(T item) noexcept {
        const size_t current_tail = tail_;
        const size_t next_tail = (current_tail + 1) & (Capacity - 1);
        if (next_tail == head_) {
            return false; // 满
        }
        items_[current_tail] = std::move(item);
        tail_ = next_tail;
        return true;
    }

    bool try_dequeue(T& item) noexcept {
        if (head_ == tail_) {
            return false; // 空
        }
        item = std::move(items_[head_]);
        head_ = (head_ + 1) & (Capacity - 1);
        return true;
    }

    size_t try_dequeue_batch(T* items, size_t max) noexcept {
        size_t count = 0;
        while (count < max && head_ != tail_) {
            items[count] = std::move(items_[head_]);
            head_ = (head_ + 1) & (Capacity - 1);
            ++count;
        }
        return count;
    }

    size_t size() const noexcept {
        return (tail_ - head_) & (Capacity - 1);
    }

    bool empty() const noexcept {
        return head_ == tail_;
    }

private:
    alignas(64) std::array<T, Capacity> items_;
    size_t head_{0};
    size_t tail_{0};
};

}  // namespace storage::runtime
