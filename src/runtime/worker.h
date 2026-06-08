#pragma once
#include "scheduler.h"
#include "adaptive_idle.h"
#include "scheduling_policy.h"
#include "policy_factory.h"
#include "worker_perf.h"
#include "adapt/object_pool.h"
#include "adapt/memory_pool.h"
#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace storage::runtime {

struct WorkerStats {
    uint64_t tasks_executed{0};
    uint64_t total_exec_ns{0};
    uint64_t total_polls{0};
    uint64_t total_idles{0};
};

class Worker {
public:
    struct Config {
        uint32_t cpu_id{1};    // 默认绑核 0, 0=不绑定
        uint32_t numa_node{0};
        PolicyConfig policy_cfg{"strict_priority", {}, 64};
        AdaptiveIdle::Config idle_cfg;
        size_t max_batch_size{64};
    };

    explicit Worker(Config cfg);

    size_t add_queue(std::unique_ptr<WorkQueue> q);
    void set_policy(std::unique_ptr<SchedulingPolicy> p);

    void start();
    void stop();
    void join();
    void notify();

    WorkerStats stats() const;
    size_t worker_id() const noexcept { return id_; }

    // 公开队列访问，供外部轮询器和测试使用
    WorkQueue* get_queue(size_t idx) const { return scheduler_.get_queue(idx); }

    // ── 性能计数 ──
    WorkerPerf& perf() { return perf_; }
    void set_perf_level(PerfLevel lv) { perf_.set_level(lv); }

protected:
    Scheduler& scheduler() { return scheduler_; }
    AdaptiveIdle& idle() { return *idle_; }
    const Config& config() const { return cfg_; }

    // 子类覆写：在 start 的线程入口处自定义初始化
    virtual void on_worker_start() {}

    // 向本 worker 的 affine queue 投递协程句柄（外部线程可调用）
    virtual void enqueue_affine(std::coroutine_handle<> h) {
        (void)h;  // 默认空实现，子类可覆写
    }

    // ── 内存池访问 ──
    adapt::QuantMemoryResource& memory_resource() { return *mem_pool_; }
    adapt::ObjectPool<WorkItem>& work_item_pool() { return *work_item_pool_; }

private:
    void worker_loop();

    static std::atomic<size_t> next_id_;

    size_t id_;
    Config cfg_;
    std::unique_ptr<AdaptiveIdle> idle_;
    std::unique_ptr<SchedulingPolicy> policy_;
    Scheduler scheduler_;
    std::thread thread_;

    // 每 worker 独立的共享 nothing 内存池
    std::unique_ptr<adapt::QuantMemoryResource> mem_pool_;
    std::unique_ptr<adapt::ObjectPool<WorkItem>> work_item_pool_;

    WorkerPerf perf_;  // 性能计数（初始化时传递 id_）
};

}  // namespace storage::runtime
