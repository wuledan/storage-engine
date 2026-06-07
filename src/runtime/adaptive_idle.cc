#include "adaptive_idle.h"
#include <sys/eventfd.h>
#include <unistd.h>
#include <thread>
#include <cstring>

namespace storage::runtime {

AdaptiveIdle::AdaptiveIdle(const Config& cfg) : cfg_(cfg) {
    event_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (event_fd_ < 0) {
        throw std::runtime_error("eventfd failed: " + std::string(strerror(errno)));
    }
}

void AdaptiveIdle::enter_idle() {
    if (level_.load(std::memory_order_acquire) == IdleLevel::kActive) {
        level_.store(IdleLevel::kSpin, std::memory_order_release);
    }

    switch (level_.load(std::memory_order_acquire)) {
        case IdleLevel::kSpin:
            idle_spin();
            level_.store(IdleLevel::kYield, std::memory_order_release);
            break;
        case IdleLevel::kYield:
            idle_yield();
            level_.store(IdleLevel::kPark, std::memory_order_release);
            break;
        case IdleLevel::kPark:
            idle_park();
            break;
        default:
            break;
    }
}

void AdaptiveIdle::idle_spin() {
    for (uint32_t i = 0; i < cfg_.spin_iterations; ++i) {
        // 检查是否有 notify
        uint64_t val;
        if (::read(event_fd_, &val, sizeof(val)) > 0) {
            level_.store(IdleLevel::kActive, std::memory_order_release);
            return;
        }
        __builtin_ia32_pause();
    }
}

void AdaptiveIdle::idle_yield() {
    for (uint32_t i = 0; i < cfg_.yield_rounds; ++i) {
        uint64_t val;
        if (::read(event_fd_, &val, sizeof(val)) > 0) {
            level_.store(IdleLevel::kActive, std::memory_order_release);
            return;
        }
        std::this_thread::yield();
    }
}

void AdaptiveIdle::idle_park() {
    std::unique_lock lock(park_mutex_);
    // 丢失唤醒防护：持有锁后重新检查
    uint64_t val;
    if (::read(event_fd_, &val, sizeof(val)) > 0) {
        level_.store(IdleLevel::kActive, std::memory_order_release);
        return;
    }

    uint64_t gen = park_generation_.load(std::memory_order_acquire);
    park_cv_.wait(lock, [this, gen]() {
        return park_generation_.load(std::memory_order_acquire) != gen;
    });
    level_.store(IdleLevel::kActive, std::memory_order_release);
}

void AdaptiveIdle::notify() {
    // 写 eventfd 唤醒 SPIN/YIELD 阶段
    uint64_t val = 1;
    ssize_t written = ::write(event_fd_, &val, sizeof(val));
    (void)written;  // eventfd 写入几乎不会失败，忽略返回值

    // 唤醒 PARK 阶段
    park_generation_.fetch_add(1, std::memory_order_acq_rel);
    park_cv_.notify_one();
}

}  // namespace storage::runtime
