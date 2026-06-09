#include <gtest/gtest.h>
#include "io/io_engine.h"
#include "io/io_uring_backend.h"
#include <fcntl.h>
#include <atomic>

using namespace storage::io;

TEST(BatchSubmitTest, BatchExactQD) {
    auto backend = IOEngine::create({"io_uring", 128}, nullptr);
    ASSERT_NE(backend, nullptr);
    IOUringBackend* uring = static_cast<IOUringBackend*>(backend.get());
    uring->set_buffer_config({128, 3});

    std::atomic<size_t> completed{0};
    std::vector<IORequest> batch;
    for (int i = 0; i < 128; ++i) {
        IORequest req{IORequest::kWrite, 0, 0, nullptr, 0, 0,
            [&completed](IOCompletion) { completed.fetch_add(1); }};
        batch.push_back(std::move(req));
    }
    backend->submit_batch(std::move(batch));
    EXPECT_EQ(backend->pending_count(), 0);

    IOCompletion comps[128];
    size_t total = 0;
    while (total < 128) {
        size_t n = backend->poll(comps, 128);
        for (size_t i = 0; i < n; ++i) comps[i].callback(comps[i]);
        total += n;
    }
    EXPECT_EQ(completed.load(), 128);
}

TEST(BatchSubmitTest, BatchPartialBuffers) {
    auto backend = IOEngine::create({"io_uring", 256}, nullptr);
    IOUringBackend* uring = static_cast<IOUringBackend*>(backend.get());
    uring->set_buffer_config({256, 3});  // min_batch_depth=256

    std::atomic<size_t> completed{0};
    std::vector<IORequest> batch;
    for (int i = 0; i < 30; ++i) {
        batch.push_back({IORequest::kWrite, 0, 0, nullptr, 0, 0,
            [&completed](IOCompletion) { completed.fetch_add(1); }});
    }
    backend->submit_batch(std::move(batch));
    EXPECT_GT(backend->pending_count(), 0);  // 缓冲中

    // 3 轮后强制提交
    for (int i = 0; i < 3; ++i) backend->flush_pending();
    EXPECT_EQ(backend->pending_count(), 0);

    IOCompletion comps[64];
    size_t total = 0;
    while (total < 30) {
        size_t n = backend->poll(comps, 64);
        for (size_t i = 0; i < n; ++i) comps[i].callback(comps[i]);
        total += n;
    }
    EXPECT_EQ(completed.load(), 30);
}

TEST(BatchSubmitTest, SubmitDirectNoBuffer) {
    auto backend = IOEngine::create({"io_uring", 64}, nullptr);
    std::atomic<bool> done{false};
    IORequest req{IORequest::kWrite, 0, 0, nullptr, 0, 0,
        [&done](IOCompletion) { done.store(true); }};
    backend->submit(std::move(req));
    EXPECT_EQ(backend->pending_count(), 0);  // submit 直通，不缓冲

    IOCompletion comps[1];
    while (!done.load()) {
        size_t n = backend->poll(comps, 1);
        for (size_t i = 0; i < n; ++i) comps[i].callback(comps[i]);
    }
    EXPECT_TRUE(done.load());
}
