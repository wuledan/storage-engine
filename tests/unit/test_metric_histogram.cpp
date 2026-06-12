#include <gtest/gtest.h>
#include "runtime/metric_histogram.h"

using namespace storage::runtime::metric;

TEST(MetricLatency, BasicRecord) {
    MetricLatency h;
    h << 1000;  // 1us
    h << 2000;  // 2us
    h << 3000;  // 3us
    EXPECT_EQ(h.count(), 3);
    EXPECT_GT(h.avg_ns(), 0);
}

TEST(MetricLatency, Percentiles) {
    MetricLatency h;
    // 100 samples: 10 at 1us, 20 at 10us, 70 at 100us
    for (int i = 0; i < 10; ++i)  h << 1000;
    for (int i = 0; i < 20; ++i)  h << 10000;
    for (int i = 0; i < 70; ++i)  h << 100000;

    // P50 should be around 100us
    uint64_t p50 = h.p50();
    uint64_t p90 = h.p90();
    EXPECT_GE(p50, 50000u);   // at least 50us
    EXPECT_LE(p90, 150000u);  // at most 150us
}

TEST(MetricLatency, Concurrent) {
    MetricLatency h;
    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&] {
            for (int i = 0; i < 1000; ++i) h << (i * 10);
        });
    }
    for (auto& t : threads) t.join();
    EXPECT_EQ(h.count(), 4000u);
}

TEST(MetricLatency, Reset) {
    MetricLatency h;
    h << 5000;
    EXPECT_EQ(h.count(), 1);
    h.reset();
    EXPECT_EQ(h.count(), 0);
    EXPECT_EQ(h.avg_ns(), 0);
}

TEST(MetricLatency, Json) {
    MetricLatency h;
    h << 1000;
    h << 5000;
    std::string json = h.to_json();
    EXPECT_NE(json.find("\"count\":2"), std::string::npos);
    EXPECT_NE(json.find("\"p50_ns\""), std::string::npos);
}
