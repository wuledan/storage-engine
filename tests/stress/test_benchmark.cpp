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

namespace {
struct SimpleCoro {
    struct promise_type {
        SimpleCoro get_return_object() {
            return SimpleCoro{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() noexcept { std::terminate(); }
    };
    std::coroutine_handle<promise_type> handle;
};

std::function<void()> g_bench_fn;
std::mutex g_bench_mutex;
void run_bench_fn() { std::function<void()> f; { std::lock_guard lk(g_bench_mutex); f = std::move(g_bench_fn); } if (f) f(); }
}  // namespace

// ===== 协程流水线 — Worker 上原位创建 SimpleCoro，全程在 Scheduler 驱动下 =====
TEST(BenchmarkIO, CoroutinePipeline) {
    for (const auto& type : {"io_uring", "libaio"}) {
        Worker::Config cfg; cfg.cpu_id = 1;
        OnlineWorker w(cfg);
        IOBackendConfig io_cfg; io_cfg.type = type; io_cfg.queue_depth = 256;
        try { w.init_io_backend(io_cfg); } catch(...) { std::cout << "[Bench] " << type << " NOT AVAIL\n"; continue; }
        w.start();

        std::string path = "/mnt/nvme_test/bench_c_" + std::string(type);
        int fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_DIRECT, 0644);
        if (fd < 0) { w.stop(); w.join(); continue; }
        void* f; posix_memalign(&f, 4096, 4096); memset(f, 0, 4096);
        pwrite(fd, f, 4096, 0); pwrite(fd, f, 4096, 1024UL*1024*1024-4096); free(f);
        void* buf; posix_memalign(&buf, 4096, 4096); memset(buf, 'X', 4096);

        std::cout << "\n=== " << type << " ===" << std::endl;
        std::cout << "  QD | IOPS(K)  | P50(us)  | P99(us)  | BW(MB/s)" << std::endl;

        for (int qd : {1, 4, 16, 64, 128}) {
            const size_t N = std::max(2000UL, (size_t)qd * 20);
            std::vector<uint64_t> lats(N);
            std::atomic<size_t> next{0}, complete{0};

            auto t0 = std::chrono::steady_clock::now();

            // Worker 上原位创建协程
            {
                std::lock_guard<std::mutex> lk(g_bench_mutex);
                g_bench_fn = [&]() {
                    for (int q = 0; q < qd; ++q) {
                        auto child = [&]() -> SimpleCoro {
                            auto r = w.make_route_func();
                            while (true) {
                                size_t op = next.fetch_add(1);
                                if (op >= N) break;
                                AffinityBaton b; uint64_t ts = __builtin_ia32_rdtsc();
                                IORequest rq{IORequest::kWrite, fd, op * 4096ULL, buf, 4096, 0,
                                    [&b, &r](IOCompletion) { b.post(r); }};
                                w.io_backend()->submit(std::move(rq));
                                co_await b;
                                lats[op] = __builtin_ia32_rdtsc() - ts;
                            }
                            complete.fetch_add(1);
                        }();
                    }
                };
            }
            w.submit_engine(WorkItem::make_func(run_bench_fn));
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
