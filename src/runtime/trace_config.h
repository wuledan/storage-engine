#pragma once
#include <atomic>
#include <cstdint>

namespace storage::runtime {

class TraceConfig {
public:
    // 全局开关
    static void set_enabled(bool v) {
        enabled_.store(v, std::memory_order_release);
    }
    static bool enabled() noexcept {
        return enabled_.load(std::memory_order_acquire);
    }

    // 采样率：1/N 的请求被 trace，0=禁用，1=全量
    static void set_sample_rate(uint32_t n) {
        sample_rate_.store(n, std::memory_order_release);
    }
    static uint32_t sample_rate() noexcept {
        return sample_rate_.load(std::memory_order_acquire);
    }

    // 判断某个请求是否需要 trace
    // trace_id=0 表示不 trace
    static bool should_trace(uint32_t trace_id) noexcept {
        if (trace_id == 0) return false;
        if (!enabled_.load(std::memory_order_acquire)) return false;
        uint32_t rate = sample_rate_.load(std::memory_order_acquire);
        if (rate == 0) return false;
        if (rate == 1) return true;
        return (trace_id % rate) == 0;
    }

    // 生成下一个 trace_id
    static uint32_t next_trace_id() noexcept {
        return next_id_.fetch_add(1, std::memory_order_relaxed);
    }

    // 便捷：启用全量 trace
    static void enable_all() {
        enabled_.store(true, std::memory_order_release);
        sample_rate_.store(1, std::memory_order_release);
    }

    // 便捷：禁用所有 trace
    static void disable() {
        enabled_.store(false, std::memory_order_release);
    }

private:
    inline static std::atomic<bool> enabled_{false};
    inline static std::atomic<uint32_t> sample_rate_{0};
    inline static std::atomic<uint32_t> next_id_{1};
};

}  // namespace storage::runtime
