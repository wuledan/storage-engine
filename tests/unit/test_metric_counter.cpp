#include <gtest/gtest.h>
#include "runtime/metric_counter.h"
#include <thread>

using namespace storage::runtime::metric;

TEST(MetricCounter, Basic) {
    MetricCounter c;
    c << 1;
    c << 2;
    EXPECT_EQ(c.value(), 3);
}

TEST(MetricCounter, Concurrent) {
    MetricCounter c;
    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&] { for (int i = 0; i < 10000; ++i) c << 1; });
    }
    for (auto& t : threads) t.join();
    EXPECT_EQ(c.value(), 40000);
}

TEST(MetricGauge, SetGet) {
    MetricGauge g;
    g.set(42);
    EXPECT_EQ(g.value(), 42);
    g.set(0);
    EXPECT_EQ(g.value(), 0);
}

TEST(MetricPeak, Peak) {
    MetricPeak p;
    p << 10;
    p << 5;
    p << 20;
    p << 15;
    EXPECT_EQ(p.value(), 20);
}

TEST(MetricRegistry, Json) {
    MetricCounter c; c << 42;
    MetricGauge g; g.set(100);
    MetricPeak p; p << 99;
    
    MetricRegistry::instance().register_counter("test/counter", &c);
    MetricRegistry::instance().register_gauge("test/gauge", &g);
    MetricRegistry::instance().register_peak("test/peak", &p);
    
    std::string json = MetricRegistry::instance().to_json();
    EXPECT_NE(json.find("\"test/counter\":42"), std::string::npos);
    EXPECT_NE(json.find("\"test/gauge\":100"), std::string::npos);
    EXPECT_NE(json.find("\"test/peak\":99"), std::string::npos);
}
