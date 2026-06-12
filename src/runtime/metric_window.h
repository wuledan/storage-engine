#pragma once
#include "metric_counter.h"
#include <atomic>
#include <cstdint>
#include <chrono>
#include <deque>
#include <mutex>

namespace storage::runtime::metric {

// ============================================================
// MetricWindow<T> — sliding time window over a metric
// ============================================================
template<typename T>
class MetricWindow {
public:
    using Clock = std::chrono::steady_clock;
    using Duration = Clock::duration;

    // window_dur: sliding window duration (e.g. 60s)
    MetricWindow(Duration window_dur = std::chrono::seconds(60))
        : window_dur_(window_dur) {}

    // Record a sample with the current value of the source metric
    void sample(int64_t value) {
        auto now = Clock::now();
        std::lock_guard<std::mutex> lk(mutex_);
        samples_.push_back({now, value});
        // Evict old samples
        while (!samples_.empty() && samples_.front().ts + window_dur_ < now)
            samples_.pop_front();
    }

    // Returns: {min, max, avg} over the window
    struct Stats { int64_t min; int64_t max; double avg; };
    Stats value() const {
        std::lock_guard<std::mutex> lk(mutex_);
        if (samples_.empty()) return {0, 0, 0};
        int64_t mn = samples_.front().value;
        int64_t mx = samples_.front().value;
        double sum = 0;
        for (auto& s : samples_) {
            if (s.value < mn) mn = s.value;
            if (s.value > mx) mx = s.value;
            sum += s.value;
        }
        return {mn, mx, sum / (double)samples_.size()};
    }

    std::string to_json() const {
        auto s = value();
        char buf[128];
        snprintf(buf, sizeof(buf), "{\"min\":%ld,\"max\":%ld,\"avg\":%.1f,\"samples\":%zu}",
                 s.min, s.max, s.avg, samples_.size());
        return buf;
    }

private:
    struct Sample { Clock::time_point ts; int64_t value; };
    mutable std::mutex mutex_;
    std::deque<Sample> samples_;
    Duration window_dur_;
};

// ============================================================
// MetricRate — per-second rate derived from a counter
// ============================================================
class MetricRate {
public:
    MetricRate() = default;

    // Call periodically (e.g. every 1s) with current counter value
    void update(int64_t current_value) {
        auto now = Clock::now();
        std::lock_guard<std::mutex> lk(mutex_);
        if (last_ts_.time_since_epoch().count() > 0) {
            auto elapsed = std::chrono::duration<double>(now - last_ts_).count();
            if (elapsed > 0) {
                rate_ = (current_value - last_value_) / elapsed;
            }
        }
        last_value_ = current_value;
        last_ts_ = now;
    }

    double value() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return rate_;
    }

    std::string to_json() const {
        char buf[32];
        snprintf(buf, sizeof(buf), "%.1f", value());
        return buf;
    }

private:
    using Clock = std::chrono::steady_clock;
    mutable std::mutex mutex_;
    int64_t last_value_{0};
    Clock::time_point last_ts_;
    double rate_{0};
};

// ============================================================
// MetricExporter — periodic sampling + Prometheus export
// ============================================================
class MetricExporter {
public:
    // Start a background thread that samples rates/windows every interval
    static void start(std::chrono::milliseconds interval = std::chrono::seconds(1));
    static void stop();

    // Register a rate to be auto-sampled
    static void register_rate(const std::string& name, MetricRate* rate);

    // Full export with all registered metrics + auto-sampled rates
    static std::string to_json();
    static std::string to_prometheus();

private:
    static MetricExporter& instance();
    MetricExporter() = default;

    std::mutex mutex_;
    std::unordered_map<std::string, MetricRate*> rates_;
    std::atomic<bool> running_{false};
};

}  // namespace storage::runtime::metric
