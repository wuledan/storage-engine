#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include "io/io_engine.h"
#include "io/io_uring_backend.h"

using namespace storage::io;

// 测试1: io_uring 后端通过 factory 创建（libaio/spdk 尚未实现）
TEST(IOBackendTest, CreateAllBackends) {
    auto uring = IOEngine::create({"io_uring", 64}, nullptr);
    ASSERT_NE(uring, nullptr);
    EXPECT_EQ(uring->name(), "io_uring");

    // libaio/spdk 尚未实现
    EXPECT_THROW(IOEngine::create({"libaio", 64}, nullptr), std::runtime_error);
    EXPECT_THROW(IOEngine::create({"spdk", 64}, nullptr), std::runtime_error);
}
