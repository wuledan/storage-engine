#include <gtest/gtest.h>
#include "runtime/online_worker.h"
#include "runtime/adapt/affinity_baton.h"
#include "io/io_engine.h"
#include <thread>
#include <chrono>
#include <atomic>
#include <vector>
#include <algorithm>
#include <numeric>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <x86intrin.h>

using namespace storage::runtime;
using namespace storage::io;
using namespace storage::runtime::adapt;

// ── SimpleCoro: 轻量级协程，suspend_always 初始挂起，enqueue_affine 投递 ──
struct SimpleCoro {
    struct promise_type {
        SimpleCoro get_return_object() {
            return SimpleCoro{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() noexcept { std::terminate(); }
    };
    std::coroutine_handle<promise_type> handle;
};

// ── BenchCtx: 全局上下文，Worker 上 bench_producer 访问 ──
struct BenchCtx {
    OnlineWorker* w;
    int fd;
    void* buf;
    int qd;
    size_t total_ops;
    std::atomic<size_t>* next_op;
    std::vector<uint64_t>* lats;
    std::atomic<size_t>* complete;
};
static BenchCtx g_ctx;

// ── bench_producer: 在 Worker 上通过 submit_engine 执行，创建 QD 个子协程 ──
//  生产者协程自己 flush_submissions，IO poll 协程只负责收割 CQE
static void bench_producer() {
    auto& ctx = g_ctx;

    // 在 Worker 上创建 QD 个子协程（suspend_always → 不立即执行）
    std::vector<SimpleCoro> children;
    children.reserve(ctx.qd);
    for (int q = 0; q < ctx.qd; ++q) {
        auto child = [&ctx]() -> SimpleCoro {
            auto route = ctx.w->make_route_func();
            while (true) {
                size_t op = ctx.next_op->fetch_add(1);
                if (op >= ctx.total_ops) break;

                AffinityBaton baton;
                uint64_t ts = __builtin_ia32_rdtsc();

                IORequest req{IORequest::kWrite, ctx.fd, op * 4096ULL, ctx.buf, 4096, 0,
                    [&baton, &route](IOCompletion) { baton.post(route); }};
                ctx.w->io_backend()->submit(std::move(req));
                co_await baton;

                (*ctx.lats)[op] = __builtin_ia32_rdtsc() - ts;
            }
            ctx.complete->fetch_add(1);
        }();
        children.push_back(std::move(child));
    }

    // 子协程投递到 P0 + 生产者自己 flush
    for (auto& c : children) ctx.w->enqueue_affine(c.handle);
    ctx.w->io_backend()->flush_submissions();  // ← 生产者自己 flush
}

// ===== CoroutinePipeline: QD 个子协程在 Worker 上并发 IO，全程 Scheduler 驱动 =====
TEST(BenchmarkIO, CoroutinePipeline) {
    std::vector<int> qds = {1, 4, 16, 64, 128};
    for (const auto& type : {"io_uring", "libaio"}) {
        Worker::Config cfg; cfg.cpu_id = 1;
        OnlineWorker w(cfg);
        IOBackendConfig io_cfg; io_cfg.type = type; io_cfg.queue_depth = 256;
        try { w.init_io_backend(io_cfg); } catch(...) { std::cout << "SKIP " << type << "\n"; continue; }
        w.start();

        std::string path = "/mnt/nvme_test/bench_c_" + std::string(type);
        int fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_DIRECT, 0644);
        if (fd < 0) { w.stop(); w.join(); continue; }
        void* f; posix_memalign(&f, 4096, 4096); memset(f,0,4096);
        pwrite(fd, f, 4096, 0); pwrite(fd, f, 4096, 1024UL*1024*1024-4096); free(f);
        void* buf; posix_memalign(&buf, 4096, 4096); memset(buf, 'X', 4096);

        std::cout << "\n=== " << type << " ===" << std::endl;
        std::cout << "  QD | IOPS(K)  | P50(us)  | P99(us)  | BW(MB/s)" << std::endl;

        for (int qd : qds) {
            const size_t N = std::max(2000UL, (size_t)qd * 20);
            std::vector<uint64_t> lats(N);
            std::atomic<size_t> next{0}, complete{0};

            g_ctx = {&w, fd, buf, qd, N, &next, &lats, &complete};

            auto t0 = std::chrono::steady_clock::now();
            w.submit_engine(WorkItem::make_func(bench_producer));
            w.notify();

            while (complete.load() < (size_t)qd)
                std::this_thread::sleep_for(std::chrono::microseconds(50));

            auto t1 = std::chrono::steady_clock::now();
            double us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
            std::sort(lats.begin(), lats.end());
            double ghz = 3.0; size_t n = lats.size();
            printf("  %-3d | %7.1f | %7.2f | %7.2f | %6.0f\n",
                   qd, N/(us/1e6)/1000.0, (lats[n/2]/ghz/1000.0), (lats[n*99/100]/ghz/1000.0),
                   N*4096.0/us*1e6/1024.0/1024.0);
        }
        w.stop(); w.join(); close(fd); unlink(path.c_str()); free(buf);
    }
}
