// DPDK rte_ring 算法的 C++ 模板移植
// 参考: DPDK 23.11 lib/ring/rte_ring.h
// 许可: BSD-3-Clause (与 DPDK 一致)
#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>
#include <algorithm>

namespace storage::runtime {

template<typename T>
class alignas(64) MPMCRing {
    static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
    static constexpr size_t kCachelineSize = 64;

public:
    explicit MPMCRing(size_t capacity = 1024) {
        size_t cap = 1;
        while (cap < capacity) cap <<= 1;
        capacity_ = cap;
        mask_ = cap - 1;
        ring_.resize(cap);
        prod_.head.store(0, std::memory_order_relaxed);
        prod_.tail.store(0, std::memory_order_relaxed);
        cons_.head.store(0, std::memory_order_relaxed);
        cons_.tail.store(0, std::memory_order_relaxed);
    }

    // 多生产者入队 (DPDK: __rte_ring_do_enqueue)
    bool enqueue(T obj) noexcept {
        return enqueue_bulk(&obj, 1) == 1;
    }

    size_t enqueue_bulk(const T* objs, size_t n) noexcept {
        size_t prod_head, prod_next;
        size_t free_entries;
        bool success = false;

        do {
            prod_head = prod_.head.load(std::memory_order_acquire);
            free_entries = capacity_ + cons_.tail.load(std::memory_order_acquire) - prod_head;
            if (n > free_entries) { n = free_entries; }
            if (n == 0) return 0;
            prod_next = prod_head + n;
            success = prod_.head.compare_exchange_weak(prod_head, prod_next,
                        std::memory_order_release, std::memory_order_relaxed);
        } while (!success);

        // 写入数据
        for (size_t i = 0; i < n; ++i)
            ring_[(prod_head + i) & mask_] = objs[i];

        // 等待 prod.tail 追上 (多生产者同步)
        while (prod_.tail.load(std::memory_order_acquire) != prod_head)
            ;
        prod_.tail.store(prod_next, std::memory_order_release);
        return n;
    }

    // 单消费者出队 (DPDK: __rte_ring_do_dequeue, SC)
    bool dequeue(T& obj) noexcept {
        return dequeue_bulk(&obj, 1) == 1;
    }

    size_t dequeue_bulk(T* objs, size_t n) noexcept {
        size_t cons_head = cons_.head.load(std::memory_order_acquire);
        size_t entries = prod_.tail.load(std::memory_order_acquire) - cons_head;
        n = std::min(n, entries);
        if (n == 0) return 0;

        for (size_t i = 0; i < n; ++i)
            objs[i] = ring_[(cons_head + i) & mask_];

        // 单消费者: 直接更新
        cons_.head.store(cons_head + n, std::memory_order_release);
        cons_.tail.store(cons_head + n, std::memory_order_release);
        return n;
    }

    // 多消费者出队 (DPDK: __rte_ring_do_dequeue, MC)
    // 使用 CAS 在 cons_.head 上预留槽位，适用于多线程并发消费。
    bool dequeue_mc(T& obj) noexcept {
        return dequeue_bulk_mc(&obj, 1) == 1;
    }

    size_t dequeue_bulk_mc(T* objs, size_t n) noexcept {
        size_t cons_head, cons_next;
        size_t entries;

        do {
            cons_head = cons_.head.load(std::memory_order_acquire);
            entries = prod_.tail.load(std::memory_order_acquire) - cons_head;
            n = std::min(n, entries);
            if (n == 0) return 0;
            cons_next = cons_head + n;
        } while (!cons_.head.compare_exchange_weak(cons_head, cons_next,
                  std::memory_order_acq_rel, std::memory_order_acquire));

        // 拷贝数据 (槽位已预留，安全)
        for (size_t i = 0; i < n; ++i)
            objs[i] = ring_[(cons_head + i) & mask_];

        // 更新 tail (无需 CAS，当前消费者独占总槽位)
        cons_.tail.store(cons_next, std::memory_order_release);
        return n;
    }

    size_t count() const noexcept {
        return prod_.tail.load(std::memory_order_acquire)
             - cons_.head.load(std::memory_order_acquire);
    }
    size_t capacity() const noexcept { return capacity_; }

private:
    struct alignas(kCachelineSize) {
        std::atomic<size_t> head{0};
        std::atomic<size_t> tail{0};
    } prod_, cons_;

    size_t capacity_{0};
    size_t mask_{0};
    std::vector<T> ring_;
};

}  // namespace storage::runtime
