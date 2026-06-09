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

// ── 单协程 Producer（batch 模式） ──
// 每次产出 QD 个 IO 请求，共享一个 baton + pending 计数器
// 最后一个 CQE 回调 post baton → 生产协程恢复 → 产出下一批
// 生产者自行 flush_submissions

struct BenchCtx {
    OnlineWorker* w;
    int fd;
    void* buf;
    int qd;
    size_t total_ops;
    std::atomic<size_t>* next_batch;
    std::vector<uint64_t>* lats;
    std::atomic<bool>* done;
};
static BenchCtx g_ctx;

struct ProducerCoro {
    struct promise_type {
        ProducerCoro get_return_object() {
            return ProducerCoro{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() noexcept { std::terminate(); }
    };
    std::coroutine_handle<promise_type> handle;
};

// Worker 上由 submit_engine 调用的入口 (void(*)())
static void bench_producer() {
    auto& ctx = g_ctx;

    // 单协程：在 Worker 上原位创建，suspend_never 使其立即开始执行
    auto producer = [&ctx]() -> ProducerCoro {
        while (true) {
            // 一批 QD 个 IO
            AffinityBaton baton;
            std::atomic<size_t> pending{0};
            std::vector<uint64_t> t0s(ctx.qd);

            size_t batch_start = ctx.next_batch->fetch_add(ctx.qd);

            for (int q = 0; q < ctx.qd; ++q) {
                size_t op = batch_start + q;
                if (op >= ctx.total_ops) break;

                pending.fetch_add(1, std::memory_order_relaxed);
                t0s[q] = __builtin_ia32_rdtsc();

                IORequest req{
                    IORequest::kWrite,
                    ctx.fd,
                    op * 4096ULL,
                    ctx.buf,
                    4096,
                    0,
                    [&baton, &pending, &ctx, &t0s, q, op](IOCompletion) {
                        uint64_t delta = __builtin_ia32_rdtsc() - t0s[q];
                        if (op < ctx.lats->size()) {
                            (*ctx.lats)[op] = delta;
                        }
                        if (pending.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                            // last one — wake up producer
                            // 每次创建新 route（OnlineWorker 生命周期覆盖全测试）
                            baton.post(ctx.w->make_route_func());
                        }
                    }};
                ctx.w->io_backend()->submit(std::move(req));
            }

            if (pending.load(std::memory_order_acquire) == 0) break;  // no more ops

            ctx.w->io_backend()->flush_submissions();  // ← 生产者自己 flush
            co_await baton;
        }
        ctx.done->store(true);
    }();

    // 协程已通过 suspend_never 启动，handle 不再需要
    (void)producer;
}

// ===== CoroutinePipeline: 单协程 batch 模式 =====
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
        void* f; posix_memalign(&f, 4096, 4096); memset(f, 0, 4096);
        pwrite(fd, f, 4096, 0);
        pwrite(fd, f, 4096, 1024UL * 1024 * 1024 - 4096);
        free(f);
        void* buf; posix_memalign(&buf, 4096, 4096); memset(buf, 'X', 4096);

        std::cout << "\n=== " << type << " ===" << std::endl;
        std::cout << "  QD | IOPS(K)  | P50(us)  | P99(us)  | BW(MB/s) | P999(us) " << std::endl;

        for (int qd : qds) {
            const size_t N = std::max((size_t)qd * 10, 2000UL);
            std::vector<uint64_t> lats(N);
            std::atomic<size_t> next_batch{0};
            std::atomic<bool> done{false};

            g_ctx = {&w, fd, buf, qd, N, &next_batch, &lats, &done};

            auto t0 = std::chrono::steady_clock::now();
            w.submit_engine(WorkItem::make_func(bench_producer));
            w.notify();

            while (!done.load())
                std::this_thread::sleep_for(std::chrono::microseconds(50));

            auto t1 = std::chrono::steady_clock::now();
            double us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
            std::sort(lats.begin(), lats.end());
            double ghz = 3.0;
            size_t n = lats.size();
            printf("  %-3d | %7.1f | %7.2f | %7.2f | %6.0f | %7.2f\n",
                   qd,
                   N / (us / 1e6) / 1000.0,
                   (lats[n / 2] / ghz / 1000.0),
                   (lats[n * 99 / 100] / ghz / 1000.0),
                   N * 4096.0 / us * 1e6 / 1024.0 / 1024.0,
                   (lats[n * 999 / 1000] / ghz / 1000.0));
        }
        w.stop(); w.join(); close(fd); unlink(path.c_str()); free(buf);
    }
}

// 测量 Scheduler 决策开销
TEST(BenchmarkIO, SchedulerOverhead) {
    Worker::Config cfg; cfg.cpu_id = 1;
    OnlineWorker w(cfg);
    IOBackendConfig io_cfg; io_cfg.type = "io_uring"; io_cfg.queue_depth = 256;
    try { w.init_io_backend(io_cfg); } catch(...) { return; }
    w.start();

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto stats = w.stats();
    uint64_t polls = stats.total_polls;
    uint64_t tasks = stats.tasks_executed;
    
    std::cout << "\n=== Scheduler Overhead ===" << std::endl;
    std::cout << "  total_polls: " << polls << std::endl;
    std::cout << "  tasks_exec:  " << tasks << std::endl;
    
    w.stop(); w.join();
}
