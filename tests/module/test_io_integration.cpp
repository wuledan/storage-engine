#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include "runtime/online_worker.h"
#include "io/io_engine.h"

using namespace storage::runtime;
using namespace storage::io;

// ── Global bridge for captureless callbacks ──
static std::atomic<bool> g_io_done{false};
static std::atomic<int64_t> g_io_result{0};
static void io_done_callback(IOCompletion c) {
    g_io_result.store(c.result);
    g_io_done.store(true);
}

// ============================================================================
// 测试1: Worker + IO Backend 创建
// ============================================================================
TEST(IOIntegrationTest, WorkerWithIOBackend) {
    Worker::Config cfg;
    OnlineWorker w(cfg);

    IOBackendConfig io_cfg;
    io_cfg.type = "io_uring";
    io_cfg.queue_depth = 64;
    w.init_io_backend(io_cfg);

    EXPECT_NE(w.io_backend(), nullptr);
}

// ============================================================================
// 测试2: Scheduler 集成 — IO poll 在调度循环中调用
// ============================================================================
TEST(IOIntegrationTest, SchedulerPollsIO) {
    Worker::Config cfg;
    OnlineWorker w(cfg);

    IOBackendConfig io_cfg;
    io_cfg.type = "io_uring";
    io_cfg.queue_depth = 64;
    w.init_io_backend(io_cfg);

    w.start();

    const char* path = "/tmp/test_scheduler_io_poll";
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(fd, 0);

    // 直接通过 io_backend 提交（不经过协程）
    g_io_done = false;
    g_io_result = 0;
    IORequest req;
    char buf[] = "direct_io_test";
    req.op = IORequest::kWrite;
    req.fd = fd;
    req.offset = 0;
    req.buf = buf;
    req.len = strlen(buf);
    req.callback = io_done_callback;
    w.io_backend()->submit(std::move(req));

    // Scheduler 的 poll 循环会自动收割 IO 完成
    while (!g_io_done.load()) {
        w.notify();  // 唤醒可能 idle 的 scheduler
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    EXPECT_EQ(g_io_result.load(), (int64_t)strlen("direct_io_test"));

    w.stop();
    w.join();
    close(fd);
    unlink(path);
}

// ============================================================================
// 测试3: 多 IO 并发提交
// ============================================================================
TEST(IOIntegrationTest, MultipleConcurrentIO) {
    Worker::Config cfg;
    OnlineWorker w(cfg);

    IOBackendConfig io_cfg;
    io_cfg.type = "io_uring";
    io_cfg.queue_depth = 256;
    w.init_io_backend(io_cfg);
    w.start();

    const char* path = "/tmp/test_concurrent_io";
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(fd, 0);

    constexpr size_t N = 500;
    std::atomic<size_t> completed{0};
    char buf[32] = "batch_io_data";

    for (size_t i = 0; i < N; ++i) {
        IORequest req;
        req.op = IORequest::kWrite;
        req.fd = fd;
        req.offset = static_cast<uint64_t>(i) * 32;
        req.buf = buf;
        req.len = 32;
        req.callback = [&completed](IOCompletion) { completed.fetch_add(1); };
        w.io_backend()->submit(std::move(req));
    }

    while (completed.load() < N) {
        w.notify();  // 唤醒可能 idle 的 scheduler，确保 IO poll 持续进行
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    w.stop();
    w.join();
    EXPECT_EQ(completed.load(), N);
    close(fd);
    unlink(path);
}
