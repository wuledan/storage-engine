#pragma once
#include "runtime/batched_spsc_work_queue.h"
#include <thread>
#include <atomic>
#include <vector>
#include <chrono>
#include <functional>

namespace storage::runtime::test {

// 模拟 IO 完成事件轮询器
// 周期性向指定 worker 的 NetIO / DiskIO 队列投递完成事件
class MockIOPoller {
public:
    struct Config {
        std::chrono::microseconds poll_interval{100};   // 轮询间隔
        size_t completions_per_poll{8};                   // 每次轮询的完成事件数
    };

    explicit MockIOPoller() : cfg_(Config{}) {}
    explicit MockIOPoller(const Config& cfg) : cfg_(cfg) {}

    // 绑定到指定 worker 的 NetIO 队列
    void bind_net_io(BatchedSPSCWorkQueue* queue) { net_io_queue_ = queue; }
    void bind_disk_io(BatchedSPSCWorkQueue* queue) { disk_io_queue_ = queue; }

    // 设置 worker 通知回调（push 后唤醒可能 idle 的 worker）
    using NotifyFunc = std::function<void()>;
    void set_notify(NotifyFunc fn) { notify_fn_ = std::move(fn); }

    void start() {
        running_.store(true);
        thread_ = std::thread([this] {
            std::vector<WorkItem> batch(cfg_.completions_per_poll);
            while (running_.load(std::memory_order_acquire)) {
                std::this_thread::sleep_for(cfg_.poll_interval);
                if (net_io_queue_) {
                    for (size_t i = 0; i < cfg_.completions_per_poll; ++i) {
                        batch[i] = WorkItem::make_func(+[]() {});
                    }
                    net_io_queue_->push_batch(batch.data(), cfg_.completions_per_poll);
                    enqueued_net_.fetch_add(cfg_.completions_per_poll,
                                            std::memory_order_relaxed);
                    if (notify_fn_) notify_fn_();
                }
                if (disk_io_queue_) {
                    for (size_t i = 0; i < cfg_.completions_per_poll; ++i) {
                        batch[i] = WorkItem::make_func(+[]() {});
                    }
                    disk_io_queue_->push_batch(batch.data(), cfg_.completions_per_poll);
                    enqueued_disk_.fetch_add(cfg_.completions_per_poll,
                                             std::memory_order_relaxed);
                    if (notify_fn_) notify_fn_();
                }
            }
        });
    }

    void stop() {
        running_.store(false);
        if (thread_.joinable()) thread_.join();
    }

    uint64_t enqueued_net() const { return enqueued_net_.load(); }
    uint64_t enqueued_disk() const { return enqueued_disk_.load(); }

private:
    Config cfg_;
    BatchedSPSCWorkQueue* net_io_queue_{nullptr};
    BatchedSPSCWorkQueue* disk_io_queue_{nullptr};
    NotifyFunc notify_fn_{nullptr};
    std::thread thread_;
    std::atomic<bool> running_{false};
    std::atomic<uint64_t> enqueued_net_{0};
    std::atomic<uint64_t> enqueued_disk_{0};
};

}  // namespace storage::runtime::test
