#include <gtest/gtest.h>
#include <atomic>
#include <thread>
#include <chrono>
#include "io/io_engine.h"
#include "io/io_uring_backend.h"
#include "runtime/online_worker.h"

using namespace storage::io;
using namespace storage::runtime;

// ============================================================================
// C1: BatchExactQD — 精确匹配 queue_depth 的批量提交
// ============================================================================
TEST(BatchSubmitTest, BatchExactQD) {
    auto backend = IOEngine::create({"io_uring", 128}, nullptr);
    ASSERT_NE(backend, nullptr);

    std::atomic<size_t> completed{0};
    std::vector<IORequest> batch;
    for (int i = 0; i < 128; ++i) {
        IORequest req;
        req.op = IORequest::kWrite;
        req.fd = 0; req.buf = nullptr; req.len = 0;
        req.callback = [&completed](IOCompletion) { completed.fetch_add(1); };
        batch.push_back(std::move(req));
    }
    backend->submit_batch(std::move(batch));
    backend->flush_pending();

    // Poll completions
    IOCompletion comps[128];
    size_t total = 0;
    while (total < 128) {
        size_t n = backend->poll(comps, 128);
        for (size_t i = 0; i < n; ++i) {
            if (comps[i].callback) comps[i].callback(comps[i]);
        }
        total += n;
        if (n == 0) std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    EXPECT_EQ(completed.load(), 128u);
    EXPECT_EQ(backend->pending_count(), 0u);
}

// ============================================================================
// C2: BatchPartialUnderrun — 小于 max_batch_size，靠 aging flush
// ============================================================================
TEST(BatchSubmitTest, BatchPartialUnderrun) {
    auto backend = IOEngine::create({"io_uring", 128}, nullptr);
    ASSERT_NE(backend, nullptr);
    IOUringBackend* uring = static_cast<IOUringBackend*>(backend.get());
    uring->set_buffer_config({128, 3, 0.5});

    for (int i = 0; i < 30; ++i) {
        IORequest req;
        req.op = IORequest::kWrite;
        req.fd = 0; req.buf = nullptr; req.len = 0;
        backend->submit(std::move(req));
    }
    EXPECT_GT(backend->pending_count(), 0u);

    // 3 次 flush_pending 后应强制提交（aging）
    for (int i = 0; i < 3; ++i) backend->flush_pending();
    EXPECT_EQ(backend->pending_count(), 0u);
}

// ============================================================================
// C3: BatchOverflow — 超过 max_batch_size，立即 flush
// ============================================================================
TEST(BatchSubmitTest, BatchOverflow) {
    auto backend = IOEngine::create({"io_uring", 128}, nullptr);
    ASSERT_NE(backend, nullptr);
    IOUringBackend* uring = static_cast<IOUringBackend*>(backend.get());
    uring->set_buffer_config({128, 3, 0.5});

    // 先缓冲 30
    for (int i = 0; i < 30; ++i) {
        IORequest req;
        req.op = IORequest::kWrite;
        req.fd = 0; req.buf = nullptr; req.len = 0;
        backend->submit(std::move(req));
    }
    EXPECT_EQ(backend->pending_count(), 30u);

    // 再投 100 → 30+100=130 > 128 → flush_pending 应全部提交
    for (int i = 0; i < 100; ++i) {
        IORequest req;
        req.op = IORequest::kWrite;
        req.fd = 0; req.buf = nullptr; req.len = 0;
        backend->submit(std::move(req));
    }
    backend->flush_pending();
    EXPECT_EQ(backend->pending_count(), 0u);
}

// ============================================================================
// C5: SingleRequest — 单个请求走缓冲
// ============================================================================
TEST(BatchSubmitTest, SingleRequest) {
    auto backend = IOEngine::create({"io_uring", 128}, nullptr);
    ASSERT_NE(backend, nullptr);

    std::atomic<bool> done{false};
    IORequest req;
    req.op = IORequest::kWrite;
    req.fd = 0; req.buf = nullptr; req.len = 0;
    req.callback = [&done](IOCompletion) { done.store(true); };
    backend->submit(std::move(req));

    // 单个请求还在 buffer 中，pending_count == 1
    EXPECT_EQ(backend->pending_count(), 1u);

    // flush_pending 后 aging 还没到（age=0），不提交
    backend->flush_pending();
    EXPECT_EQ(backend->pending_count(), 1u);

    // flush_pending 3 次后 aging 触发
    for (int i = 0; i < 3; ++i) backend->flush_pending();
    EXPECT_EQ(backend->pending_count(), 0u);

    // Poll 完成
    IOCompletion comps[1];
    int timeout = 1000;
    while (!done.load() && --timeout > 0) {
        size_t n = backend->poll(comps, 1);
        for (size_t i = 0; i < n; ++i) {
            if (comps[i].callback) comps[i].callback(comps[i]);
        }
        if (n == 0) std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    EXPECT_TRUE(done.load());
}
