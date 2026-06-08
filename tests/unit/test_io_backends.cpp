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

// 测试1: 三种后端通过 factory 创建
TEST(IOBackendTest, CreateAllBackends) {
    auto uring = IOEngine::create({"io_uring", 64}, nullptr);
    ASSERT_NE(uring, nullptr);
    EXPECT_EQ(uring->name(), "io_uring");

    auto aio = IOEngine::create({"libaio", 64}, nullptr);
    ASSERT_NE(aio, nullptr);
    EXPECT_EQ(aio->name(), "libaio");

    auto spdk = IOEngine::create({"spdk", 64}, nullptr);
    ASSERT_NE(spdk, nullptr);
    EXPECT_EQ(spdk->name(), "spdk");
}
