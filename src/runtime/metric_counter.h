#pragma once
#include <atomic>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <mutex>
#include <functional>
#include <vector>

namespace storage::runtime::metric {

// =====================================================
// MetricCounter — 单调递增计数器 (Adder)
// =====================================================
class MetricCounter {
public:
    MetricCounter() = default;
    
    void operator<<(int64_t delta) {
        value_.fetch_add(delta, std::memory_order_relaxed);
    }
    
    int64_t value() const noexcept {
        return value_.load(std::memory_order_relaxed);
    }
    
    void reset() noexcept { value_.store(0); }

private:
    std::atomic<int64_t> value_{0};
};

// =====================================================
// MetricGauge — 可读写瞬时值 (Status)
// =====================================================
class MetricGauge {
public:
    MetricGauge() = default;
    
    void set(int64_t v) noexcept {
        value_.store(v, std::memory_order_relaxed);
    }
    
    int64_t value() const noexcept {
        return value_.load(std::memory_order_relaxed);
    }

private:
    std::atomic<int64_t> value_{0};
};

// =====================================================
// MetricPeak — 峰值追踪 (Maxer)
// =====================================================
class MetricPeak {
public:
    MetricPeak() = default;
    
    void operator<<(int64_t v) noexcept {
        int64_t prev = value_.load(std::memory_order_relaxed);
        while (v > prev) {
            if (value_.compare_exchange_weak(prev, v, std::memory_order_relaxed))
                break;
        }
    }
    
    int64_t value() const noexcept { return value_.load(std::memory_order_relaxed); }
    void reset() noexcept { value_.store(0); }

private:
    std::atomic<int64_t> value_{0};
};

// =====================================================
// MetricRegistry — 变量树 (brpc bvar 模式)
// =====================================================
// 全局单例, 按 "worker/0/tasks" 等路径注册变量。
// 支持 JSON 导出, Prometheus 导出。
class MetricRegistry {
public:
    using JsonFunc = std::function<std::string()>;

    static MetricRegistry& instance();

    // 注册变量（自动生成 counter/gauge/peak/latency 的 JSON 表示）
    void register_counter(const std::string& path, MetricCounter* c);
    void register_gauge(const std::string& path, MetricGauge* g);
    void register_peak(const std::string& path, MetricPeak* p);
    void register_custom(const std::string& path, JsonFunc fn);

    template<typename T>
    static T& counter(const std::string& path);

    template<typename T>
    static T& gauge(const std::string& path);

    // 导出
    std::string to_json() const;
    std::string to_prometheus() const;

private:
    MetricRegistry() = default;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, JsonFunc> entries_;
};

// =====================================================
// Implementation
// =====================================================

inline MetricRegistry& MetricRegistry::instance() {
    static MetricRegistry reg;
    return reg;
}

inline void MetricRegistry::register_counter(const std::string& path, MetricCounter* c) {
    std::lock_guard<std::mutex> lk(mutex_);
    entries_[path] = [c] { return std::to_string(c->value()); };
}

inline void MetricRegistry::register_gauge(const std::string& path, MetricGauge* g) {
    std::lock_guard<std::mutex> lk(mutex_);
    entries_[path] = [g] { return std::to_string(g->value()); };
}

inline void MetricRegistry::register_peak(const std::string& path, MetricPeak* p) {
    std::lock_guard<std::mutex> lk(mutex_);
    entries_[path] = [p] { return std::to_string(p->value()); };
}

inline void MetricRegistry::register_custom(const std::string& path, JsonFunc fn) {
    std::lock_guard<std::mutex> lk(mutex_);
    entries_[path] = std::move(fn);
}

inline std::string MetricRegistry::to_json() const {
    std::lock_guard<std::mutex> lk(mutex_);
    std::string out = "{";
    bool first = true;
    for (auto& [path, fn] : entries_) {
        if (!first) out += ",";
        out += "\"" + path + "\":" + fn();
        first = false;
    }
    out += "}";
    return out;
}

inline std::string MetricRegistry::to_prometheus() const {
    std::lock_guard<std::mutex> lk(mutex_);
    std::string out;
    for (auto& [path, fn] : entries_) {
        // Prometheus format: metric_name{path} value
        std::string name = path;
        for (auto& c : name) if (c == '/') c = '_';
        out += "# HELP " + name + " auto-generated\n";
        out += "# TYPE " + name + " gauge\n";
        out += name + " " + fn() + "\n";
    }
    return out;
}

}  // namespace storage::runtime::metric
