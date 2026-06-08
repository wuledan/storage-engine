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

// ============================================================================
// 测试1: IOUringBackend 创建成功
// ============================================================================
TEST(IOUringTest, CreateBackend) {
    IOBackendConfig cfg;
    cfg.type = "io_uring";
    cfg.queue_depth = 64;
    auto backend = IOEngine::create(cfg, nullptr);
    ASSERT_NE(backend, nullptr);
    EXPECT_EQ(backend->name(), "io_uring");
}

// ============================================================================
// 测试2: 基本读写（需要临时文件）
// ============================================================================
TEST(IOUringTest, ReadWriteFile) {
    auto backend = IOEngine::create({"io_uring", 64}, nullptr);
    ASSERT_NE(backend, nullptr);

    // 创建临时文件
    const char* path = "/tmp/test_io_uring_write";
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(fd, 0);

    // 写入
    const char* test_str = "hello io_uring!";
    char write_buf[64];
    std::strncpy(write_buf, test_str, sizeof(write_buf));
    IORequest write_req;
    write_req.op = IORequest::kWrite;
    write_req.fd = fd;
    write_req.offset = 0;
    write_req.buf = write_buf;
    write_req.len = std::strlen(test_str);
    std::atomic<bool> write_done{false};
    IOCompletion write_comp;
    write_req.callback = [&write_done, &write_comp](IOCompletion c) {
        write_comp = c;
        write_done.store(true);
    };
    backend->submit(write_req);

    // Poll 直到完成
    IOCompletion comps[64];
    while (!write_done.load()) {
        size_t n = backend->poll(comps, 64);
        for (size_t i = 0; i < n; ++i) {
            comps[i].callback(comps[i]);
        }
    }
    EXPECT_EQ(write_comp.result, (int64_t)std::strlen(test_str));

    // 读取
    char read_buf[64] = {0};
    IORequest read_req;
    read_req.op = IORequest::kRead;
    read_req.fd = fd;
    read_req.offset = 0;
    read_req.buf = read_buf;
    read_req.len = sizeof(read_buf);
    std::atomic<bool> read_done{false};
    IOCompletion read_comp;
    read_req.callback = [&read_done, &read_comp](IOCompletion c) {
        read_comp = c;
        read_done.store(true);
    };
    backend->submit(read_req);

    while (!read_done.load()) {
        size_t n = backend->poll(comps, 64);
        for (size_t i = 0; i < n; ++i) {
            comps[i].callback(comps[i]);
        }
    }
    EXPECT_EQ(read_comp.result, (int64_t)std::strlen(test_str));
    EXPECT_STREQ(read_buf, test_str);

    close(fd);
    unlink(path);
}

// ============================================================================
// 测试3: 大量 IO 提交+完成（吞吐）
// ============================================================================
TEST(IOUringTest, ManyIOOps) {
    auto backend = IOEngine::create({"io_uring", 256}, nullptr);

    const char* path = "/tmp/test_io_uring_many";
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(fd, 0);

    constexpr size_t N = 1000;
    char buf[64] = "test data";
    std::atomic<size_t> completed{0};
    std::atomic<size_t> total_bytes{0};

    for (size_t i = 0; i < N; ++i) {
        IORequest req;
        req.op = IORequest::kWrite;
        req.fd = fd;
        req.offset = static_cast<uint64_t>(i) * 64;
        req.buf = buf;
        req.len = 64;
        req.callback = [&completed, &total_bytes](IOCompletion c) {
            if (c.result > 0) total_bytes.fetch_add(static_cast<size_t>(c.result));
            completed.fetch_add(1);
        };
        backend->submit(req);
    }

    IOCompletion comps[64];
    while (completed.load() < N) {
        size_t n = backend->poll(comps, 64);
        for (size_t i = 0; i < n; ++i) {
            comps[i].callback(comps[i]);
        }
    }

    EXPECT_EQ(completed.load(), N);
    EXPECT_EQ(total_bytes.load(), N * 64);

    close(fd);
    unlink(path);
}
