#pragma once
#include <cstdint>

namespace storage::runtime {

// ── Histogram ──
//
// Power-of-2 bucket histogram with O(1) lock-free recording.
// 64 buckets cover values from 1 to 2^63. Ideal for latency distributions.
class Histogram {
public:
    static constexpr size_t kNumBuckets = 64;

    // O(1) branch-free record: compute bucket via leading-zero count
    void record(uint64_t value) noexcept {
        if (value == 0) value = 1;
        int b = 63 - __builtin_clzll(value);
        buckets_[b]++;
        count_++;
        sum_ += value;
    }

    // Estimate p-th percentile (0.0 ~ 1.0)
    double percentile(double p) const noexcept {
        if (count_ == 0) return 0;
        uint64_t target = static_cast<uint64_t>(count_ * p);
        uint64_t accum = 0;
        for (size_t i = 0; i < kNumBuckets; ++i) {
            accum += buckets_[i];
            if (accum >= target) {
                return static_cast<double>(1ULL << i);
            }
        }
        return static_cast<double>(1ULL << (kNumBuckets - 1));
    }

    uint64_t count() const noexcept { return count_; }
    uint64_t sum() const noexcept { return sum_; }

private:
    uint64_t buckets_[kNumBuckets]{};
    uint64_t count_{0};
    uint64_t sum_{0};
};

}  // namespace storage::runtime
