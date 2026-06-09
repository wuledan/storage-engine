#include <gtest/gtest.h>
#include "runtime/online_worker.h"
#include "runtime/adapt/affinity_baton.h"
#include "io/io_engine.h"
#include "io/io_uring_backend.h"
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
    // Producer-internal wall clock (first IO → last IO, aligning fio)
    int64_t wall_t0_ns{-1};
    int64_t wall_t1_ns{-1};
    // Stage timing accumulators (rdtsc, QD=1 only)
    uint64_t t_submit{0};
    uint64_t t_flush{0};
    uint64_t t_iowait{0};
    size_t t_count{0};
    // 精确开销探针
    uint64_t t_producer{0};
    uint64_t t_resume_gap{0};
    uint64_t t_last_resume{0};
    uint64_t baton_rt{0};
    // co_await 耗时 (挂起→恢复, 含IO等待)
    uint64_t t_coro_suspend{0};
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
            // 测量 resume → loop 间隙
            uint64_t t_now = __builtin_ia32_rdtsc();
            if (ctx.t_last_resume > 0)
                ctx.t_resume_gap += t_now - ctx.t_last_resume;
            uint64_t t_loop = t_now;

            AffinityBaton baton;
            std::atomic<size_t> pending{0};
            std::vector<uint64_t> t0s(ctx.qd);

            size_t batch_start = ctx.next_batch->fetch_add(ctx.qd);

            for (int q = 0; q < ctx.qd; ++q) {
                size_t op = batch_start + q;
                if (op >= ctx.total_ops) break;

                pending.fetch_add(1, std::memory_order_relaxed);
                t0s[q] = __builtin_ia32_rdtsc();

                if (ctx.wall_t0_ns < 0) {
                    ctx.wall_t0_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now().time_since_epoch()).count();
                }

                IORequest req{
                    IORequest::kWrite, ctx.fd, op * 4096ULL, ctx.buf, 4096, 0,
                    [&baton, &pending, &ctx, &t0s, q, op](IOCompletion) {
                        uint64_t delta = __builtin_ia32_rdtsc() - t0s[q];
                        if (op < ctx.lats->size()) (*ctx.lats)[op] = delta;
                        if (pending.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                            ctx.baton_rt = __builtin_ia32_rdtsc();
                            baton.post(ctx.w->make_route_func());
                        }
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

            uint64_t t_coro0 = __builtin_ia32_rdtsc();
            co_await baton;
            uint64_t t_resume = __builtin_ia32_rdtsc();
            ctx.t_coro_suspend += t_resume - t_coro0;
            ctx.t_producer += t_resume - t_loop;

            uint64_t t_pre = ctx.baton_rt;
            ctx.baton_rt = 0;
            if (t_pre > 0) {
                ctx.t_resume_gap += t_resume - t_pre;
            }
        }
        ctx.wall_t1_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        ctx.done->store(true);
    }();

    (void)producer;
}

// ===== CoroutinePipeline: 单协程 batch 模式 =====
TEST(BenchmarkIO, CoroutinePipeline) {
    std::vector<int> qds = {1};
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

            g_ctx = {&w, fd, buf, qd, N, &next_batch, &lats, &done}; auto s0 = w.scheduler().stats().total_polls;
            w.scheduler().reset_probe();

            auto t0 = std::chrono::steady_clock::now();
            w.submit_engine(WorkItem::make_func(bench_producer));
            w.notify();

            while (!done.load())
                std::this_thread::sleep_for(std::chrono::microseconds(50));

            auto t1 = std::chrono::steady_clock::now();
            auto s1 = w.scheduler().stats().total_polls;
            size_t sched_iters = s1 - s0;

            double wall_sec = (g_ctx.wall_t1_ns > 0 && g_ctx.wall_t0_ns > 0)
                ? (g_ctx.wall_t1_ns - g_ctx.wall_t0_ns) / 1e9 : 0.001;
            double real_iops = (wall_sec > 0) ? N / wall_sec : 0;
            double real_bw   = (wall_sec > 0) ? N * 4096.0 / wall_sec : 0;
            if (qd == 1 || qd == 128)
                printf("       → %zu sched iters, %.1f/op, %zu ns/iter\n",
                       sched_iters, (double)sched_iters/N,
                       sched_iters>0? (size_t)(wall_sec*1e9/sched_iters):0);

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
            printf("\n  Probe (ns): submit=%.0f flush=%.0f co_await=%.0f baton_rt=%.0f producer=%.0f (N=%zu)\n",
                   g_ctx.t_submit/(double)g_ctx.t_count/3.0,
                   g_ctx.t_flush/(double)g_ctx.t_count/3.0,
                   g_ctx.t_coro_suspend/(double)g_ctx.t_count/3.0,
                   g_ctx.t_resume_gap/(double)g_ctx.t_count/3.0,
                   g_ctx.t_producer/(double)g_ctx.t_count/3.0,
                   g_ctx.t_count);
        }
        // Scheduler 探针
        auto& p = w.scheduler().probe;
        double ghz = 3.0;
        printf("\n  Scheduler probe (avg ns/iter, %zu iters): poll=%.0f drain0=%.0f snap=%.0f drain0b=%.0f total=%.0f\n",
               w.scheduler().probe_count,
               p.io_poll/ghz/(double)w.scheduler().probe_count,
               p.drain_p0/ghz/(double)w.scheduler().probe_count,
               p.snapshot/ghz/(double)w.scheduler().probe_count,
               p.drain_p0b/ghz/(double)w.scheduler().probe_count,
               p.iter/ghz/(double)w.scheduler().probe_count);

        w.stop(); w.join(); close(fd); unlink(path.c_str()); free(buf);
    }
}

// ===== FioAlignedPipeline: io_uring_submit_and_wait per batch =====
// Aligns with fio's io_uring pattern: one syscall = submit + kernel wait for CQE.
// CQEs harvested inline in producer coroutine. No baton, no Scheduler IO poll.
TEST(BenchmarkIO, FioAlignedPipeline) {
    // Only io_uring — the point is comparing our io_uring_submit pattern with fio's
    Worker::Config cfg; cfg.cpu_id = 1;
    OnlineWorker w(cfg);
    IOBackendConfig io_cfg; io_cfg.type = "io_uring"; io_cfg.queue_depth = 256;
    try { w.init_io_backend(io_cfg); } catch(...) { std::cout << "SKIP io_uring\n"; return; }
    w.start();

    std::string path = "/mnt/nvme_test/bench_fl";
    int fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_DIRECT, 0644);
    assert(fd >= 0);
    void* prem; posix_memalign(&prem, 4096, 4096); memset(prem, 0, 4096);
    pwrite(fd, prem, 4096, 0);
    pwrite(fd, prem, 4096, 1024UL * 1024 * 1024 - 4096);
    free(prem);
    void* buf; posix_memalign(&buf, 4096, 4096); memset(buf, 'X', 4096);

    const int qd = 1;
    const size_t N = 50000;
    auto* backend = static_cast<IOUringBackend*>(w.io_backend());
    std::vector<uint64_t> lats(N);
    std::atomic<size_t> next_batch{0};
    std::atomic<bool> done{false};

    auto run_producer = [&]() {
        int64_t wall_t0 = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();

        std::vector<uint64_t> t0s(qd);
        std::atomic<size_t> pending{0};

        uint64_t t_submit_total = 0, t_wait_total = 0, t_proc_total = 0;
        size_t t_count = 0;

        while (true) {
            size_t batch_start = next_batch.fetch_add(qd, std::memory_order_relaxed);
            bool has_work = false;

            for (int q = 0; q < qd; ++q) {
                size_t op = batch_start + q;
                if (op >= N) break;
                has_work = true;
                t0s[q] = __builtin_ia32_rdtsc();
                pending.fetch_add(1, std::memory_order_relaxed);
                IORequest req{IORequest::kWrite, fd, op * 4096ULL, buf, 4096, 0,
                    [&lats, &t0s, q, op](IOCompletion) {
                        lats[op] = __builtin_ia32_rdtsc() - t0s[q];
                    }};
                backend->submit(std::move(req));
            }
            if (pending.load(std::memory_order_acquire) == 0) break;

            // 分步测量: submit + kernel wait, 对齐 fio 的 io_uring_submit_and_wait
            struct io_uring *r = backend->raw_ring();
            uint64_t t_s0 = __builtin_ia32_rdtsc();
            int ret = io_uring_submit(r);
            uint64_t t_s1 = __builtin_ia32_rdtsc();
            (void)ret;

            struct io_uring_cqe *cqe = nullptr;
            uint64_t t_w0 = __builtin_ia32_rdtsc();
            io_uring_wait_cqe(r, &cqe);
            uint64_t t_w1 = __builtin_ia32_rdtsc();

            t_submit_total += t_s1 - t_s0;
            t_wait_total += t_w1 - t_w0;

            // process CQE
            uint64_t idx = reinterpret_cast<uint64_t>(io_uring_cqe_get_data(cqe));
            if (idx < N) {
                lats[idx] = t_w1 - t0s[0];
            }
            io_uring_cqe_seen(r, cqe);
            t_proc_total += __builtin_ia32_rdtsc() - t_w1;
            t_count++;
            pending.store(0, std::memory_order_release);
        }

        int64_t wall_t1 = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        double wall_sec = (wall_t1 - wall_t0) / 1e9;
        double riop = (wall_sec > 0) ? N / wall_sec : 0;

        std::sort(lats.begin(), lats.end());
        double ghz = 3.0;
        size_t n = lats.size();

        printf("\n=== fio-aligned io_uring_submit + io_uring_wait_cqe ===\n");
        printf("  QD=%d N=%zu\n", qd, N);
        printf("  P50=%.2fus  P90=%.2fus  P99=%.2fus  P999=%.2fus\n",
               lats[n/2]/ghz/1000.0,
               lats[n*90/100]/ghz/1000.0,
               lats[n*99/100]/ghz/1000.0,
               lats[n*999/1000]/ghz/1000.0);
        printf("  RIOP=%.1fK  BW=%.0fMB/s\n",
               riop/1000.0,
               N * 4096.0 / wall_sec / 1024 / 1024);
        printf("  submit=%luns  wait=%luns  proc=%luns (N=%zu)\n",
               (unsigned long)(t_submit_total / t_count / 3.0),
               (unsigned long)(t_wait_total / t_count / 3.0),
               (unsigned long)(t_proc_total / t_count / 3.0),
               t_count);

        done.store(true);
    };
    run_producer();

    while (!done.load())
        std::this_thread::sleep_for(std::chrono::microseconds(50));

    w.stop(); w.join(); close(fd); unlink(path.c_str()); free(buf);
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

TEST(BenchmarkIO, IterRate) {
    Worker::Config c; c.cpu_id = 1;
    OnlineWorker w(c);
    IOBackendConfig io; io.type = "io_uring"; io.queue_depth = 256;
    try { w.init_io_backend(io); } catch(...) { return; }
    w.start();
    auto s0 = w.scheduler().stats().total_polls;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    auto s1 = w.scheduler().stats().total_polls;
    auto iters = s1 - s0;
    printf("Scheduler: %lu iters/sec → %.0f ns/iter\n", iters, 1e9/(double)iters);
    w.stop(); w.join();
}

// 精确测量单 IO 各阶段
TEST(BenchmarkIO, SingleIOTimeline) {
    Worker::Config c; c.cpu_id = 1;
    OnlineWorker w(c);
    IOBackendConfig io; io.type = "io_uring"; io.queue_depth = 256;
    try { w.init_io_backend(io); } catch(...) { return; }
    w.start();
    std::string p = "/mnt/nvme_test/tl"; int fd = open(p.c_str(), O_RDWR|O_CREAT|O_TRUNC|O_DIRECT, 0644);
    void* b; posix_memalign(&b, 4096, 4096); memset(b, 'X', 4096);
    void* f; posix_memalign(&f, 4096, 4096); memset(f,0,4096); pwrite(fd, f, 4096, 0); free(f);
    
    const int N = 200;
    std::vector<uint64_t> lats(N);
    std::atomic<size_t> next{0}, complete{0};
    
    struct Ctx { OnlineWorker* w; int fd; void* b; std::vector<uint64_t>* l; std::atomic<size_t>* n; std::atomic<size_t>* c; };
    Ctx ctx{&w, fd, b, &lats, &next, &complete};
    
    auto t_start = std::chrono::steady_clock::now();
    
    // submit_engine → producer → N IOs
    // 用 rdtsc 测量 submit→flush→co_await→resume 的完整链路
    
    // 简化为: 直接循环测 P50
    for (int i = 0; i < N; ++i) {
        std::atomic<bool> done{false};
        uint64_t t0 = __builtin_ia32_rdtsc();
        IORequest rq{IORequest::kWrite, fd, (uint64_t)i*4096, b, 4096, 0,
            [&done](IOCompletion) { done.store(true); }};
        w.io_backend()->submit(std::move(rq));
        w.io_backend()->flush_submissions();
        while (!done.load()) _mm_pause();
        lats[i] = __builtin_ia32_rdtsc() - t0;
    }
    
    auto t_end = std::chrono::steady_clock::now();
    double wall_us = std::chrono::duration_cast<std::chrono::microseconds>(t_end - t_start).count();
    std::sort(lats.begin(), lats.end());
    double ghz = 3.0;
    printf("io_uring DIRECT (no Scheduler, no baton):\n");
    printf("  P50=%.1fus P99=%.1fus avg=%.1fus\n", lats[N/2]/ghz/1000.0, lats[N*99/100]/ghz/1000.0,
           std::accumulate(lats.begin(),lats.end(),0ULL)/N/ghz/1000.0);
    printf("  RIOP(wall)=%.1fK LIOP(lat)=%.1fK\n", N/(wall_us/1e6)/1000.0, 1e9/(std::accumulate(lats.begin(),lats.end(),0ULL)/N/ghz)*1/1000.0);
    
    w.stop(); w.join(); close(fd); unlink(p.c_str()); free(b);
}

static std::atomic<uint64_t> g_cb_to_resume{0};
static std::atomic<uint64_t> g_cb_count{0};

TEST(BenchmarkIO, BatonRoundTrip) {
    Worker::Config c; c.cpu_id = 1;
    OnlineWorker w(c);
    IOBackendConfig io; io.type = "io_uring"; io.queue_depth = 256;
    try { w.init_io_backend(io); } catch(...) { return; }
    w.start();
    std::string p = "/mnt/nvme_test/br"; int fd = open(p.c_str(), O_RDWR|O_CREAT|O_TRUNC|O_DIRECT, 0644);
    void* b; posix_memalign(&b, 4096, 4096); memset(b, 'X', 4096);
    void* f; posix_memalign(&f, 4096, 4096); memset(f,0,4096); pwrite(fd, f, 4096, 0); free(f);
    
    const int N = 1000;
    g_cb_to_resume.store(0); g_cb_count.store(0);
    
    for (int i = 0; i < N; ++i) {
        struct BatonCoro {
            struct promise_type {
                BatonCoro get_return_object() { return BatonCoro{std::coroutine_handle<promise_type>::from_promise(*this)}; }
                std::suspend_never initial_suspend() noexcept { return {}; }
                std::suspend_never final_suspend() noexcept { return {}; }
                void return_void() noexcept {}
                void unhandled_exception() noexcept { std::terminate(); }
            };
            std::coroutine_handle<promise_type> handle;
        };
        auto child = [&]() -> BatonCoro {
            auto route = w.make_route_func();
            AffinityBaton baton;
            IORequest rq{IORequest::kWrite, fd, (uint64_t)i*4096, b, 4096, 0,
                [&baton, &route](IOCompletion c) { baton.post(route); }};
            w.io_backend()->submit(std::move(rq));
            w.io_backend()->flush_submissions();
            uint64_t t0 = __builtin_ia32_rdtsc();
            co_await baton;
            uint64_t t1 = __builtin_ia32_rdtsc();
            g_cb_to_resume.fetch_add(t1 - t0);
            g_cb_count.fetch_add(1);
        }();
        w.enqueue_affine(child.handle);
    }
    while (g_cb_count.load() < N) std::this_thread::sleep_for(std::chrono::microseconds(50));
    double ghz = 3.0;
    printf("io_uring baton round-trip (co_await→resume): avg=%.1fus\n", g_cb_to_resume.load()/(double)g_cb_count.load()/ghz/1000.0);
    w.stop(); w.join(); close(fd); unlink(p.c_str()); free(b);
}

static struct io_uring* g_raw_ring{nullptr};
static std::atomic<size_t> g_raw_count{0};
static std::atomic<uint64_t> g_raw_total{0};

static void raw_submit_probe_fn() {
    uint64_t t0 = __builtin_ia32_rdtsc();
    io_uring_submit(g_raw_ring);
    uint64_t t1 = __builtin_ia32_rdtsc();
    g_raw_total.fetch_add(t1 - t0, std::memory_order_relaxed);
    g_raw_count.fetch_add(1, std::memory_order_relaxed);
}

TEST(BenchmarkIO, RawSubmitOnSchedulerThread) {
    Worker::Config cfg; cfg.cpu_id = 1;
    OnlineWorker w(cfg);
    IOBackendConfig io; io.type = "io_uring"; io.queue_depth = 256;
    try { w.init_io_backend(io); } catch(...) { return; }
    g_raw_ring = static_cast<IOUringBackend*>(w.io_backend())->raw_ring();
    g_raw_count.store(0); g_raw_total.store(0);
    w.start();

    const int N = 10000;
    for (int i = 0; i < N; ++i)
        w.submit_engine(WorkItem::make_func(raw_submit_probe_fn));

    while (g_raw_count.load() < (size_t)N)
        std::this_thread::sleep_for(std::chrono::microseconds(50));

    uint64_t sched_total = g_raw_total.load();
    size_t sched_count = g_raw_count.load();

    uint64_t test_total = 0;
    auto* r = static_cast<IOUringBackend*>(w.io_backend())->raw_ring();
    for (int i = 0; i < N; ++i) {
        uint64_t t0 = __builtin_ia32_rdtsc();
        io_uring_submit(r);
        uint64_t t1 = __builtin_ia32_rdtsc();
        test_total += t1 - t0;
    }

    double ghz = 3.0;
    printf("io_uring_submit (no pending SQEs, N=%d):\n", N);
    printf("  Scheduler thread: %.0fns avg (%zu calls)\n", sched_total/(double)sched_count/ghz, sched_count);
    printf("  Test thread:      %.0fns avg\n", test_total/(double)N/ghz);

    w.stop(); w.join();
}
