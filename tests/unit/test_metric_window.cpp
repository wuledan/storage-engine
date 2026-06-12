#include <gtest/gtest.h>
#include "runtime/metric_window.h"
#include "runtime/metric_counter.h"

using namespace storage::runtime::metric;

TEST(MetricRate, Basic) {
    MetricRate r;
    r.update(0);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    r.update(100);  // 100 ops in ~10ms ≈ 10K ops/s
    EXPECT_GT(r.value(), 0);
    EXPECT_LT(r.value(), 50000);  // sanity check
}

TEST(MetricWindow, Basic) {
    MetricWindow<int> window(std::chrono::seconds(1));
    window.sample(10);
    window.sample(20);
    auto s = window.value();
    EXPECT_EQ(s.min, 10);
    EXPECT_EQ(s.max, 20);
    EXPECT_GT(s.avg, 0);
}

TEST(MetricWindow, Eviction) {
    MetricWindow<int> window(std::chrono::milliseconds(10));
    window.sample(100);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    window.sample(200);
    auto s = window.value();
    EXPECT_EQ(s.max, 200);  // latest
}
