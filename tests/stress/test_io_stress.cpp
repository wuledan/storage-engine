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

// ============================================================================
// 测试1: 持续高 IOPS 10s，无崩溃/无内存泄漏
// ============================================================================
TEST(IOStressTest, SustainedHighIOPS_60s) {
    Worker::Config cfg;
    OnlineWorker w(cfg);
    w.init_io_backend({"io_uring", 256});
    w.start();

    const char* path = "/tmp/test_io_stress_sustained";
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(fd, 0);

    const auto duration = std::chrono::seconds(10);
    auto start = std::chrono::steady_clock::now();
    std::atomic<uint64_t> completed{0};
    std::atomic<bool> stop{false};

    // 提交线程
    std::thread submitter([&]() {
        char buf[64] = "stress_data";
        while (!stop.load()) {
            for (size_t i = 0; i < 64; ++i) {
                IORequest req;
                req.op = IORequest::kWrite;
                req.fd = fd;
                req.offset = (completed.load() + i) * 64;
                req.buf = buf;
                req.len = 64;
                req.callback = [&completed](IOCompletion) { completed.fetch_add(1); };
                w.io_backend()->submit(std::move(req));
                w.notify();  // 保持 scheduler 活跃
            }
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    });

    while (std::chrono::steady_clock::now() - start < duration) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    stop.store(true);
    submitter.join();

    // 等待剩余 IO 完成
    auto wait_start = std::chrono::steady_clock::now();
    uint64_t prev = 0;
    while (std::chrono::steady_clock::now() - wait_start < std::chrono::seconds(2)) {
        uint64_t cur = completed.load();
        if (cur == prev && cur > 0) break;
        prev = cur;
        w.notify();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    w.stop();
    w.join();
    close(fd);
    unlink(path);

    uint64_t total = completed.load();
    std::cout << "[IO Stress] Sustained 10s IOPS: "
              << (total / 10.0) << " ops/s, total=" << total << std::endl;
    EXPECT_GT(total, 100);
}

// ============================================================================
// 测试2: 突发大量 IO 提交（超越 queue_depth）
// ============================================================================
TEST(IOStressTest, BurstSubmission) {
    Worker::Config cfg;
    OnlineWorker w(cfg);
    w.init_io_backend({"io_uring", 64});  // 小队列深度
    w.start();

    const char* path = "/tmp/test_io_stress_burst";
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(fd, 0);

    const size_t N = 10000;  // 远超 queue_depth
    std::atomic<size_t> completed{0};
    char buf[64] = "burst";

    for (size_t i = 0; i < N; ++i) {
        IORequest req;
        req.op = IORequest::kWrite;
        req.fd = fd;
        req.offset = static_cast<uint64_t>(i) * 64;
        req.buf = buf;
        req.len = 64;
        req.callback = [&completed](IOCompletion) { completed.fetch_add(1); };
        w.io_backend()->submit(std::move(req));
    }

    // 等待全部完成
    while (completed.load() < N) {
        w.notify();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    w.stop();
    w.join();
    close(fd);
    unlink(path);

    EXPECT_EQ(completed.load(), N);
}

// ============================================================================
// 测试3: io_uring 正确性
// ============================================================================
TEST(IOStressTest, IoUringCorrectness) {
    Worker::Config cfg;
    OnlineWorker w(cfg);
    w.init_io_backend({"io_uring", 64});
    w.start();

    const char* path = "/tmp/test_stress_iouring";
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(fd, 0);

    const size_t N = 2000;
    std::atomic<size_t> done{0};
    char buf[64] = "correctness";

    for (size_t i = 0; i < N; ++i) {
        IORequest req;
        req.op = IORequest::kWrite;
        req.fd = fd;
        req.offset = static_cast<uint64_t>(i) * 64;
        req.buf = buf;
        req.len = 64;
        req.callback = [&done](IOCompletion c) {
            ASSERT_EQ(c.result, 64);
            done.fetch_add(1);
        };
        w.io_backend()->submit(std::move(req));
    }

    while (done.load() < N) {
        w.notify();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    w.stop();
    w.join();
    close(fd);
    unlink(path);
    EXPECT_EQ(done.load(), N);
}
