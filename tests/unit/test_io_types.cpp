#include <gtest/gtest.h>
#include "io/io_request.h"
#include "io/io_backend.h"
#include "io/io_engine.h"

using namespace storage::io;

// ============================================================================
// 测试1: IORequest 默认值
// ============================================================================
TEST(IOTypesTest, DefaultRequest) {
    IORequest req;
    EXPECT_EQ(req.op, IORequest::kRead);
    EXPECT_EQ(req.fd, -1);
    EXPECT_EQ(req.len, 0);
}

// ============================================================================
// 测试2: IOCompletion 默认值
// ============================================================================
TEST(IOTypesTest, DefaultCompletion) {
    IOCompletion comp;
    EXPECT_EQ(comp.result, 0);
    EXPECT_EQ(comp.user_data, 0);
}

// ============================================================================
// 测试3: IOEngine 未知类型抛出异常
// ============================================================================
TEST(IOEngineTest, UnknownTypeThrows) {
    IOBackendConfig cfg;
    cfg.type = "unknown";
    EXPECT_THROW(IOEngine::create(cfg), std::runtime_error);
}

// ============================================================================
// 测试4: IOEngine 已知类型名创建成功（io_uring 已实现）
// ============================================================================
TEST(IOEngineTest, KnownTypesDoNotThrowFromConfig) {
    IOBackendConfig cfg;
    cfg.type = "io_uring";
    auto backend = IOEngine::create(cfg);
    ASSERT_NE(backend, nullptr);
    EXPECT_EQ(backend->name(), "io_uring");
}
