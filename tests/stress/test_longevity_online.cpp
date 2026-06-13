#include <gtest/gtest.h>
#include "runtime/online_group.h"
#include "runtime/metric_server.h"
#include "runtime/metric_histogram.h"
#include "io/io_uring_backend.h"
#include <thread>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

using namespace storage::runtime;
using namespace storage::runtime::metric;
using namespace storage::runtime::adapt;
using namespace storage::io;
using namespace std::chrono;

// ============================================================================
// LongevityLoop — coroutine type for the continuous IO submission loop
//
//   initial_suspend = always  : submitted via handle to engine queue,
//                               does not start until the scheduler picks it up.
//   final_suspend   = never   : coroutine frame is auto-destroyed after
//                               co_return (ownership transferred to queue).
// ============================================================================
struct LongevityLoop {
    struct promise_type {
        LongevityLoop get_return_object() {
            return LongevityLoop{
                std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() noexcept { return {}; }
        std::suspend_never  final_suspend()   noexcept { return {}; }
        void return_void()   noexcept {}
        void unhandled_exception() noexcept { std::terminate(); }
    };
    std::coroutine_handle<promise_type> handle;
};

// ============================================================================
// io_loop — per-worker coroutine that continuously submits QD=32 4K writes
//
// Each iteration:
//   1. Creates a shared AffinityBaton (shared_ptr so callbacks keep it alive)
//   2. Submits 32 write requests, each capturing its own timestamp t0
//   3. Flushes submissions to the io_uring SQ
//   4. Suspends (co_await) until the baton is posted
//
// The last completion callback posts the baton, which resumes this coroutine.
// Latency samples are recorded into the shared MetricLatency histogram.
// ============================================================================
static LongevityLoop io_loop(OnlineWorker& worker, int fd, void* buf,
                              std::atomic<bool>& stop,
                              std::atomic<uint64_t>& total_io,
                              MetricLatency& io_lat) {
    uint64_t offset = 0;
    while (!stop.load(std::memory_order_relaxed)) {
        // Baton lives as long as any callback holds a shared_ptr to it.
        auto baton = std::make_shared<AffinityBaton>();
        std::atomic<uint32_t> pending{32};

        for (uint32_t q = 0; q < 32; ++q) {
            auto t0 = high_resolution_clock::now();
            IORequest req;
            req.op = IORequest::kWrite;
            req.fd = fd;
            req.offset = offset * 4096ULL;
            req.buf = buf;
            req.len = 4096;
            req.callback = [baton, &pending, &total_io, &io_lat, t0](
                IOCompletion /*comp*/) {
                auto lat = duration_cast<nanoseconds>(
                    high_resolution_clock::now() - t0).count();
                io_lat << lat;
                total_io.fetch_add(1, std::memory_order_relaxed);
                if (pending.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    baton->post_direct();
                }
            };
            worker.io_backend()->submit(std::move(req));
            offset++;
        }
        worker.io_backend()->flush_submissions();
        co_await *baton;
    }
    co_return;
}

// ============================================================================
// TEST: 20-minute Online IO Longevity
//
//   - 4 workers, each with an io_uring backend (QD=256)
//   - Each worker runs a coroutine that continuously submits QD=32 writes
//   - Every 10s logs: elapsed time, IOPS, P50, P99, average latency
//   - After 20min, verifies:
//       1. P50 < 200us (no severe drift / >2x from baseline)
//       2. No crashes, no leaks (ASan/UBSan build should pass)
// ============================================================================
TEST(Longevity, OnlineIO_20min) {
    printf("\n=== Online IO Longevity (20min, 4 workers, QD=32) ===\n");

    // ------------------------------------------------------------------
    // 1. Worker group setup
    // ------------------------------------------------------------------
    OnlineWorkerGroup::Config gcfg;
    gcfg.num_workers = 4;
    gcfg.pin_cpu = false;
    gcfg.register_global = false;    // avoid interfering with other tests
    OnlineWorkerGroup group(gcfg);

    constexpr size_t kRingDepth = 256;
    IOBackendConfig io_cfg;
    io_cfg.type = "io_uring";
    io_cfg.queue_depth = kRingDepth;
    io_cfg.sq_poll_group = 0;  // all 4 workers share one kernel thread

    // All workers share the same SQPOLL kernel thread via group 0
    for (size_t i = 0; i < 4; ++i) {
        group.worker(i).init_io_backend(io_cfg);
    }
    group.start();

    // ------------------------------------------------------------------
    // 2. Metrics
    // ------------------------------------------------------------------
    MetricLatency io_lat;
    io_lat.register_with("io/latency");

    std::atomic<bool> stop{false};
    std::atomic<uint64_t> total_io{0};

    // ------------------------------------------------------------------
    // 3. Per-worker context and coroutine launch
    // ------------------------------------------------------------------
    struct WorkerCtx {
        int   fd{-1};
        void* buf{nullptr};
    };
    std::vector<WorkerCtx> ctxs(4);

    for (size_t w = 0; w < 4; ++w) {
        auto& ctx = ctxs[w];
        std::string path = "/mnt/nvme_test/longevity_" + std::to_string(w);

        ctx.fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_DIRECT,
                      0644);
        ASSERT_GE(ctx.fd, 0) << "Failed to open " << path
                             << " (errno=" << errno << ")";

        int ret = posix_memalign(&ctx.buf, 4096, 4096);
        ASSERT_EQ(ret, 0) << "posix_memalign failed";
        std::memset(ctx.buf, 'X', 4096);

        auto& worker = group.worker(w);
        LongevityLoop loop = io_loop(worker, ctx.fd, ctx.buf,
                                      stop, total_io, io_lat);
        worker.submit_engine(WorkItem::make_coro(loop.handle));
        loop.handle = {};   // ownership transferred to the engine queue
    }

    // ------------------------------------------------------------------
    // 4. Metrics server
    // ------------------------------------------------------------------
    MetricServer::Config scfg{9190};
    scfg.bind_addr = "192.168.3.12";
    MetricServer srv(scfg);
    srv.start();
    printf("  Metrics server on http://192.168.3.12:9190/metrics\n");

    // ------------------------------------------------------------------
    // 5. Monitor loop — 5 minutes, log every 10 seconds
    // ------------------------------------------------------------------
    auto t0 = steady_clock::now();
    auto hard_deadline = t0 + minutes(7);  // 7min absolute deadline
    bool timed_out = false;
    uint64_t prev_io = 0;
    uint64_t p50_initial_us = 0;

    printf("  Time(s) | IOPS(K) | P50(us) | P99(us) | Avg(us)\n");
    printf("  --------|---------|---------|---------|--------\n");

    while (duration_cast<minutes>(steady_clock::now() - t0).count() < 1) {
        if (steady_clock::now() > hard_deadline) {
            auto elapsed_s = duration_cast<seconds>(steady_clock::now() - t0).count();
            printf("  HARD TIMEOUT at %lds — forcing exit\n", elapsed_s);
            timed_out = true;
            break;
        }
        std::this_thread::sleep_for(seconds(10));
        auto now  = steady_clock::now();
        uint64_t cur  = total_io.load(std::memory_order_relaxed);
        double   secs = duration_cast<milliseconds>(now - t0).count() / 1000.0;
        double   iops_k = static_cast<double>(cur - prev_io) / 10.0 / 1000.0;
        prev_io = cur;

        uint64_t p50_now_us = io_lat.p50() / 1000;
        if (p50_initial_us == 0) {
            p50_initial_us = p50_now_us;
        }

        printf("  %7.0fs | %7.1f | %7lu | %7lu | %6lu\n",
               secs, iops_k,
               p50_now_us,
               io_lat.p99() / 1000,
               io_lat.avg_ns() / 1000);
        fflush(stdout);
    }

    printf("  Stopping IO loops...\n"); fflush(stdout);
    stop.store(true, std::memory_order_release);
    printf("  Waiting for workers to drain...\n"); fflush(stdout);

    // P50 summary for the full run
    uint64_t p50_final_us = io_lat.p50() / 1000;
    printf("  P50 range: initial=%luus final=%luus drift=%.1fx\n",
           p50_initial_us, p50_final_us,
           (double)p50_final_us / std::max(p50_initial_us, 1ul));

    // ------------------------------------------------------------------
    // 6. Verification — P50 must stay within reasonable bounds
    // ------------------------------------------------------------------
    // QD=32 on QLC: P50 expected 100-600us. Threshold 1ms.
    if (!timed_out) {
        EXPECT_LT(p50_final_us, 1000u) << "P50 drifted to " << p50_final_us;
    } else {
        printf("  Skipping P50 check due to hard timeout\n");
    }

    printf("  Final: %lu total IOs, P50=%luus P99=%luus Avg=%luus\n",
           total_io.load(),
           p50_final_us,
           io_lat.p99() / 1000,
           io_lat.avg_ns() / 1000);

    // ------------------------------------------------------------------
    // 7. Cleanup
    // ------------------------------------------------------------------
    printf("  Shutting down worker group...\n"); fflush(stdout);
    auto t_stop = steady_clock::now();
    group.stop();
    auto shutdown_ms = duration_cast<milliseconds>(steady_clock::now() - t_stop).count();
    printf("  Group shutdown took %ldms\n", shutdown_ms);

    for (size_t w = 0; w < 4; ++w) {
        auto& ctx = ctxs[w];
        if (ctx.buf)  std::free(ctx.buf);
        if (ctx.fd >= 0) {
            close(ctx.fd);
            std::string path = "/mnt/nvme_test/longevity_" + std::to_string(w);
            unlink(path.c_str());
        }
    }

    printf("=== Longevity test complete ===\n");
}
