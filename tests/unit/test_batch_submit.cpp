#include <gtest/gtest.h>
#include "io/io_engine.h"
#include "io/io_uring_backend.h"
#include <fcntl.h>
#include <atomic>

using namespace storage::io;

namespace {

// Helper: captureless callback increments an atomic counter
struct IncCtx { std::atomic<size_t>* counter; };
void inc_callback(void* ctx, IOCompletion) {
    static_cast<IncCtx*>(ctx)->counter->fetch_add(1);
}

// Helper: captureless callback sets a done flag
struct DoneCtx { std::atomic<bool>* done; };
void done_callback(void* ctx, IOCompletion) {
    static_cast<DoneCtx*>(ctx)->done->store(true);
}

}  // anonymous namespace

TEST(BatchSubmitTest, BatchExactQD) {
    auto backend = IOEngine::create({"io_uring", 128});
    ASSERT_NE(backend, nullptr);
    IOUringBackend* uring = static_cast<IOUringBackend*>(backend.get());
    uring->set_buffer_config({128, 3});

    std::atomic<size_t> completed{0};
    IncCtx ctx{&completed};
    std::vector<IORequest> batch;
    for (int i = 0; i < 128; ++i) {
        IORequest req;
        req.op = IORequest::kWrite;
        req.callback_fn = inc_callback;
        req.callback_ctx = &ctx;
        batch.push_back(std::move(req));
    }
    backend->submit_batch(std::move(batch));
    EXPECT_EQ(backend->pending_count(), 0);

    IOCompletion comps[128];
    while (completed.load() < 128) {
        backend->poll(comps, 128);
        // Callbacks invoked inside poll() — no dispatch needed
    }
    EXPECT_EQ(completed.load(), 128);
}

TEST(BatchSubmitTest, BatchPartialBuffers) {
    auto backend = IOEngine::create({"io_uring", 256});
    IOUringBackend* uring = static_cast<IOUringBackend*>(backend.get());
    uring->set_buffer_config({256, 3});  // min_batch_depth=256

    std::atomic<size_t> completed{0};
    IncCtx ctx{&completed};
    std::vector<IORequest> batch;
    for (int i = 0; i < 30; ++i) {
        IORequest req;
        req.op = IORequest::kWrite;
        req.callback_fn = inc_callback;
        req.callback_ctx = &ctx;
        batch.push_back(std::move(req));
    }
    backend->submit_batch(std::move(batch));
    EXPECT_GT(backend->pending_count(), 0);  // 缓冲中

    // 3 轮后强制提交
    for (int i = 0; i < 3; ++i) backend->flush_pending();
    EXPECT_EQ(backend->pending_count(), 0);

    IOCompletion comps[64];
    while (completed.load() < 30) {
        backend->poll(comps, 64);
        // Callbacks invoked inside poll()
    }
    EXPECT_EQ(completed.load(), 30);
}

TEST(BatchSubmitTest, SubmitDirectNoBuffer) {
    auto backend = IOEngine::create({"io_uring", 64});
    std::atomic<bool> done{false};
    DoneCtx ctx{&done};
    IORequest req;
    req.op = IORequest::kWrite;
    req.callback_fn = done_callback;
    req.callback_ctx = &ctx;
    backend->submit(std::move(req));
    EXPECT_EQ(backend->pending_count(), 0);  // submit 直通，不缓冲

    IOCompletion comps[1];
    while (!done.load()) {
        backend->poll(comps, 1);
        // Callback invoked inside poll()
    }
    EXPECT_TRUE(done.load());
}
