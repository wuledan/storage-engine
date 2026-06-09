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
        std::suspend_always initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() noexcept { std::terminate(); }
    };
    std::coroutine_handle<promise_type> handle;
    bool enqueued{false};  // drain_p0: true → single resume, false → double resume
};

std::function<void()> g_bench_fn;
std::mutex g_bench_mutex;
void run_bench_fn() { std::function<void()> f; { std::lock_guard lk(g_bench_mutex); f = std::move(g_bench_fn); } if (f) f(); }
}  // namespace

// ===== 协程流水线 — Worker 上原位创建 SimpleCoro，全程在 Scheduler 驱动下 =====
static std::atomic<size_t> g_count{0};
static std::function<void()> g_work;
static std::mutex g_mtx;
static void bench_entry() {
    std::function<void()> fn;
    { std::lock_guard lk(g_mtx); fn = std::move(g_work); }
    if (fn) fn();
}

TEST(BenchmarkIO, CoroutinePipeline) {
    Worker::Config cfg; cfg.cpu_id = 1;
    OnlineWorker w(cfg);
    w.start();

    g_count.store(0);

    // submit_engine 在 Worker 线程上执行 bench_entry
    // bench_entry 读取 g_work 并调用 (g_work 中创建 SimpleCoro 子协程)
    {
        std::lock_guard lk(g_mtx);
        OnlineWorker* pw = &w;
        g_work = [pw]() {
            auto child = []() -> SimpleCoro {
                g_count.fetch_add(1);
            }();
            pw->enqueue_affine(child.handle);
        };
    }
    w.submit_engine(WorkItem::make_func(bench_entry));
    w.notify();

    while (g_count.load() < 1)
        std::this_thread::sleep_for(std::chrono::microseconds(50));

    EXPECT_EQ(g_count.load(), 1);
    w.stop(); w.join();
    std::cout << "PASS" << std::endl;
}
