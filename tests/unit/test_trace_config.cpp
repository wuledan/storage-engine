#include <gtest/gtest.h>
#include "runtime/trace_config.h"

using namespace storage::runtime;

// 测试1: 默认禁用
TEST(TraceConfigTest, DisabledByDefault) {
    EXPECT_FALSE(TraceConfig::enabled());
    EXPECT_EQ(TraceConfig::sample_rate(), 0);
}

// 测试2: trace_id=0 永不 trace
TEST(TraceConfigTest, TraceIdZeroNeverTraces) {
    TraceConfig::enable_all();
    EXPECT_FALSE(TraceConfig::should_trace(0));
    TraceConfig::disable();
}

// 测试3: 禁用时不 trace 任何请求
TEST(TraceConfigTest, DisabledNoTrace) {
    TraceConfig::disable();
    EXPECT_FALSE(TraceConfig::should_trace(42));
    EXPECT_FALSE(TraceConfig::should_trace(100));
}

// 测试4: sample_rate=1 时全量 trace
TEST(TraceConfigTest, SampleRateOneTracesAll) {
    TraceConfig::enable_all();
    EXPECT_TRUE(TraceConfig::should_trace(1));
    EXPECT_TRUE(TraceConfig::should_trace(2));
    EXPECT_TRUE(TraceConfig::should_trace(100));
    TraceConfig::disable();
}

// 测试5: 采样率边界（每 N 个请求 trace 一个）
TEST(TraceConfigTest, SampleRateN) {
    TraceConfig::set_enabled(true);
    TraceConfig::set_sample_rate(10);
    // trace_id 能被 10 整除的会被 trace
    EXPECT_TRUE(TraceConfig::should_trace(10));
    EXPECT_TRUE(TraceConfig::should_trace(20));
    EXPECT_FALSE(TraceConfig::should_trace(11));
    EXPECT_FALSE(TraceConfig::should_trace(9));
    TraceConfig::disable();
}

// 测试6: next_trace_id 递增
TEST(TraceConfigTest, NextTraceIdIncrementing) {
    auto a = TraceConfig::next_trace_id();
    auto b = TraceConfig::next_trace_id();
    auto c = TraceConfig::next_trace_id();
    EXPECT_LT(a, b);
    EXPECT_LT(b, c);
}

// 测试7: enable_all/disable 切换
TEST(TraceConfigTest, EnableAllDisableCycle) {
    TraceConfig::enable_all();
    EXPECT_TRUE(TraceConfig::should_trace(1));
    TraceConfig::disable();
    EXPECT_FALSE(TraceConfig::should_trace(1));
    TraceConfig::enable_all();
    EXPECT_TRUE(TraceConfig::should_trace(1));
    TraceConfig::disable();
}
