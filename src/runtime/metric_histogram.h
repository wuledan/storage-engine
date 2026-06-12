#pragma once
#include "metric_counter.h"
#include <atomic>
#include <cstdint>
#include <cmath>
#include <algorithm>
#include <string>
#include <sstream>

namespace storage::runtime::metric {

// Power-of-2 bucketed latency histogram.
// Buckets: [0..1us), [1..2us), [2..4us), [4..8us), ... up to [2^31, inf).
// Thread-safe via std::atomic per bucket.
// Percentile computation: O(buckets) linear scan, good for <100 buckets.
class MetricLatency {
public:
    static constexpr size_t kBuckets = 32;  // covers 0ns to >2^31ns

    MetricLatency() {
        for (size_t i = 0; i < kBuckets; ++i)
            buckets_[i].store(0, std::memory_order_relaxed);
    }

    // Record a latency sample (ns). Thread-safe.
    void operator<<(uint64_t latency_ns) noexcept {
        size_t idx = bucket_index(latency_ns);
        buckets_[idx].fetch_add(1, std::memory_order_relaxed);
        total_count_.fetch_add(1, std::memory_order_relaxed);
        total_sum_.fetch_add(latency_ns, std::memory_order_relaxed);
    }

    // Percentile: 0.5 = P50, 0.99 = P99, 0.999 = P999
    uint64_t percentile(double p) const noexcept {
        uint64_t total = total_count_.load(std::memory_order_acquire);
        if (total == 0) return 0;
        uint64_t target = static_cast<uint64_t>(total * p);
        uint64_t cumulative = 0;
        uint64_t lo_ns = 0;  // lower bound of current bucket (ns)
        for (size_t i = 0; i < kBuckets; ++i) {
            uint64_t n = buckets_[i].load(std::memory_order_acquire);
            cumulative += n;
            if (cumulative >= target) {
                // Linear interpolation within bucket i
                uint64_t hi_ns;
                if (i == 0) {
                    hi_ns = 1000;  // [0, 1us)
                } else {
                    hi_ns = lo_ns * 2;  // each power-of-2 bucket doubles
                }
                uint64_t prev_cum = cumulative - n;
                if (n == 0) break;  // shouldn't happen
                double frac = static_cast<double>(target - prev_cum) / n;
                return lo_ns + static_cast<uint64_t>((hi_ns - lo_ns) * frac);
            }
            // Advance lo_ns to lower bound of next bucket
            if (i == 0) {
                lo_ns = 1000;  // bucket 1 starts at 1us
            } else {
                lo_ns *= 2;
            }
        }
        return lo_ns;
    }

    uint64_t avg_ns() const noexcept {
        uint64_t n = total_count_.load(std::memory_order_acquire);
        return n > 0 ? total_sum_.load(std::memory_order_acquire) / n : 0;
    }

    uint64_t p50() const noexcept  { return percentile(0.50); }
    uint64_t p90() const noexcept  { return percentile(0.90); }
    uint64_t p99() const noexcept  { return percentile(0.99); }
    uint64_t p999() const noexcept { return percentile(0.999); }
    uint64_t count() const noexcept { return total_count_.load(); }

    void reset() noexcept {
        for (auto& b : buckets_) b.store(0, std::memory_order_relaxed);
        total_count_.store(0, std::memory_order_relaxed);
        total_sum_.store(0, std::memory_order_relaxed);
    }

    // JSON with summary (count, avg, p50, p90, p99, p999)
    std::string to_json() const {
        std::ostringstream oss;
        oss << "{\"count\":" << count()
            << ",\"avg_ns\":" << avg_ns()
            << ",\"p50_ns\":" << p50()
            << ",\"p90_ns\":" << p90()
            << ",\"p99_ns\":" << p99()
            << ",\"p999_ns\":" << p999()
            << "}";
        return oss.str();
    }

    // Register with MetricRegistry for export
    void register_with(const std::string& path) {
        MetricLatency* self = this;
        MetricRegistry::instance().register_custom(path, [self] { return self->to_json(); });
    }

private:
    static size_t bucket_index(uint64_t ns) noexcept {
        if (ns == 0) return 0;
        uint64_t us = ns / 1000;
        if (us == 0) return 0;  // < 1us
        // __builtin_clzll: count leading zeros
        int log2 = 63 - __builtin_clzll(us);
        size_t idx = static_cast<size_t>(log2 + 1);
        // Clamp to last bucket for values >= 2^31 us
        return idx >= kBuckets ? kBuckets - 1 : idx;
    }

    std::atomic<uint64_t> buckets_[kBuckets];
    std::atomic<uint64_t> total_count_{0};
    std::atomic<uint64_t> total_sum_{0};
};

}  // namespace storage::runtime::metric
