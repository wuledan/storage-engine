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
#include <random>

using namespace storage::runtime;
using namespace storage::io;
using namespace storage::runtime::adapt;

namespace {

struct SimpleCoro {
    struct promise_type {
        std::atomic<bool>* done{nullptr};
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

}  // namespace

// ===== 协程流水线 — Worker Scheduler 上 QD 协程 + 栈 AffinityBaton =====
TEST(BenchmarkIO, CoroutinePipeline) {
    std::vector<int> qds = {1, 4, 16, 64, 128};

    for (const auto& type : {"io_uring", "libaio"}) {
        Worker::Config cfg; cfg.cpu_id = 1;
        OnlineWorker w(cfg);
        IOBackendConfig io_cfg; io_cfg.type = type; io_cfg.queue_depth = 256;
        try { w.init_io_backend(io_cfg); } catch(...) { std::cout << "[Bench] " << type << " not available\n"; continue; }
        w.start();

        std::string path = "/mnt/nvme_test/bench_coro_" + std::string(type);
        int fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_DIRECT, 0644);
        if (fd < 0) { w.stop(); w.join(); continue; }
        void* fill; posix_memalign(&fill, 4096, 4096); memset(fill, 0, 4096);
        pwrite(fd, fill, 4096, 0); pwrite(fd, fill, 4096, 1024UL*1024*1024-4096); free(fill);
        void* buf; posix_memalign(&buf, 4096, 4096); memset(buf, 'X', 4096);

        std::cout << "\n=== " << type << " Pipeline (Worker Scheduler + Stack Baton) ===" << std::endl;
        std::cout << "  QD    |  IOPS(K)  |  P50(us)  |  P99(us)  |  BW(MB/s)" << std::endl;
        std::cout << "  ------|-----------|-----------|-----------|----------" << std::endl;

        for (int qd : qds) {
            const size_t total_ops = (qd <= 16) ? 2000 : 10000;
            std::vector<uint64_t> latencies(total_ops);
            std::atomic<size_t> next_op{0};
            std::atomic<bool> done{false};

            auto t_start = std::chrono::steady_clock::now();

            // 主协程在 Worker 上创建 QD 个子协程
            auto master = [&]() -> SimpleCoro {
                for (int q = 0; q < qd; ++q) {
                    [&]() -> SimpleCoro {
                        auto route = w.make_route_func();
                        while (true) {
                            size_t op = next_op.fetch_add(1);
                            if (op >= total_ops) break;

                            AffinityBaton baton;
                            uint64_t t0 = __builtin_ia32_rdtsc();

                            IORequest req;
                            req.op = IORequest::kWrite; req.fd = fd;
                            req.offset = op * 4096ULL; req.buf = buf; req.len = 4096;
                            req.callback = [&baton, &route](IOCompletion) { baton.post(route); };
                            w.io_backend()->submit(std::move(req));
                            co_await baton;

                            latencies[op] = __builtin_ia32_rdtsc() - t0;
                        }
                    }();
                }
                done.store(true);
            };

            auto coro = master();
            coro.handle.promise().done = &done;
            w.enqueue_affine(coro.handle);
            w.notify();

            while (!done.load()) std::this_thread::sleep_for(std::chrono::microseconds(50));

            auto t_end = std::chrono::steady_clock::now();
            double elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(t_end - t_start).count();
            std::sort(latencies.begin(), latencies.end());
            double ghz = 3.0; size_t n = latencies.size();
            double iops = total_ops / (elapsed_us / 1e6);
            double bw = iops * 4096 / (1024.0 * 1024.0);
            printf("  %-4d  |  %7.1f  |  %7.2f  |  %7.2f  |  %6.0f\n",
                   qd, iops/1000.0, (latencies[n/2]/ghz/1000.0), (latencies[n*99/100]/ghz/1000.0), bw);
        }
        w.stop(); w.join(); close(fd); unlink(path.c_str()); free(buf);
    }
}
