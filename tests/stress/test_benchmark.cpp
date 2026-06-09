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
    // Stage timing accumulators (rdtsc, QD=1 only)
    uint64_t t_submit{0};
    uint64_t t_flush{0};
    uint64_t t_iowait{0};
    size_t t_count{0};
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

    auto producer = [&ctx]() -> ProducerCoro {
        while (true) {
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
                    IORequest::kWrite, ctx.fd, op * 4096ULL, ctx.buf, 4096, 0,
                    [&baton, &pending, &ctx, &t0s, q, op](IOCompletion) {
                        uint64_t delta = __builtin_ia32_rdtsc() - t0s[q];
                        if (op < ctx.lats->size()) (*ctx.lats)[op] = delta;
                        if (pending.fetch_sub(1, std::memory_order_acq_rel) == 1)
                            baton.post(ctx.w->make_route_func());
                    }};
                uint64_t t_sub0 = __builtin_ia32_rdtsc();
                ctx.w->io_backend()->submit(std::move(req));
                uint64_t t_sub1 = __builtin_ia32_rdtsc();
                ctx.t_submit += t_sub1 - t_sub0;
            }

            if (pending.load(std::memory_order_acquire) == 0) break;

            uint64_t t_fl0 = __builtin_ia32_rdtsc();
            ctx.w->io_backend()->flush_submissions();
            uint64_t t_fl1 = __builtin_ia32_rdtsc();
            ctx.t_flush += t_fl1 - t_fl0;
            ctx.t_count++;

            co_await baton;
        }
        ctx.done->store(true);
    }();

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
        std::cout << "  QD | RIOP(K) | LIOP(K) | P50(us) | P90(us) | P99(us) | P999   | P9999  | BW(MB/s)" << std::endl;
        std::cout << "  ---|---------|---------|---------|---------|---------|--------|--------|---------" << std::endl;

        for (int qd : qds) {
            const size_t N = std::max((size_t)qd * 2000, 50000UL);  // 更长运行时间
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
            double wall_sec = std::chrono::duration<double>(t1 - t0).count();

            double real_iops = N / wall_sec;
            double real_bw   = N * 4096.0 / wall_sec;  // bytes/sec

            std::sort(lats.begin(), lats.end());
            double ghz = 3.0;
            size_t n = lats.size();
            uint64_t avg_ns = std::accumulate(lats.begin(), lats.end(), 0ULL) / n / ghz;
            double lat_iops = 1e9 / (avg_ns > 0 ? (double)avg_ns : 1) * qd;

            printf("  %-3d | %7.1f | %7.1f | %7.2f | %7.2f | %7.2f | %7.2f | %7.2f | %6.0f\n",
                   qd,
                   real_iops/1000.0,                    // RIOP (K)
                   lat_iops/1000.0,                     // LIOP (K)
                   (lats[n/2]/ghz/1000.0),              // P50
                   (lats[n*90/100]/ghz/1000.0),         // P90
                   (lats[n*99/100]/ghz/1000.0),         // P99
                   (lats[n*999/1000]/ghz/1000.0),       // P999
                   (lats[n*9999/10000]/ghz/1000.0),     // P9999
                   real_bw/1024/1024);                  // BW (MB/s)
        }
        // 输出 QD=1 的阶段计时
        if (true) {
            printf("\n  Stage breakdown (rdtsc): submit=%lu/flush=%lu/io=%lu cycles  count=%zu\n",
                   g_ctx.t_submit / (g_ctx.t_count ? g_ctx.t_count : 1),
                   g_ctx.t_flush / (g_ctx.t_count ? g_ctx.t_count : 1),
                   g_ctx.t_iowait,
                   g_ctx.t_count);
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
