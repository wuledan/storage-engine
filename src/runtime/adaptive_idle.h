#pragma once
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>

namespace storage::runtime {

enum class IdleLevel : uint8_t {
    kActive = 0,
    kSpin   = 1,
    kYield  = 2,
    kPark   = 3,
};

class AdaptiveIdle {
public:
    struct Config {
        uint32_t spin_iterations{100};    // ~10μs at ~100ns/pause
        uint32_t yield_rounds{5};
    };

    explicit AdaptiveIdle(const Config& cfg);
    AdaptiveIdle() : AdaptiveIdle(Config{}) {}

    // Worker 线程调用: 进入空闲循环，被 notify 唤醒后返回
    void enter_idle();

    // 任意线程调用: 通知有新任务，唤醒空闲中的 worker
    void notify();

    // 获取 eventfd (用于集成 epoll)
    int notify_fd() const noexcept { return event_fd_; }

    IdleLevel current_level() const noexcept {
        return level_.load(std::memory_order_acquire);
    }

private:
    void idle_spin();
    void idle_yield();
    void idle_park();

    Config cfg_;
    int event_fd_;
    std::mutex park_mutex_;
    std::condition_variable park_cv_;
    std::atomic<IdleLevel> level_{IdleLevel::kActive};
    std::atomic<uint64_t> park_generation_{0};
    // Separate flag to track notification that may have been consumed
    // by spin/yield phases before park could observe it
    std::atomic<bool> pending_notify_{false};
};

}  // namespace storage::runtime
