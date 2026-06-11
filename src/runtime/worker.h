#pragma once
#include "scheduler.h"
#include "adaptive_idle.h"
#include "scheduling_policy.h"
#include "policy_factory.h"
#include "worker_perf.h"
#include "adapt/object_pool.h"
#include "adapt/memory_pool.h"
#include <folly/Executor.h>
#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace storage::runtime {

class Worker;
class OnlineWorker;

Worker* current_worker();
OnlineWorker* current_online_worker();

struct WorkerStats {
    uint64_t tasks_executed{0};
    uint64_t total_exec_ns{0};
    uint64_t total_polls{0};
    uint64_t total_idles{0};
};

class Worker : public folly::Executor {
public:
    struct Config {
        uint32_t cpu_id{1};
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

    WorkQueue* get_queue(size_t idx) const { return scheduler_.get_queue(idx); }

    WorkerPerf& perf() { return perf_; }
    void set_perf_level(PerfLevel lv) { perf_.set_level(lv); }

    // ── folly::Executor ──
    // quant::WorkStealingExecutor 模式: Task final_suspend 调度回 worker
    void add(folly::Func func) override;

    // 由子类设置 affine 队列索引
    void set_affine_q_idx(size_t idx) { affine_q_idx_ = idx; }

    Scheduler& scheduler() { return scheduler_; }

protected:
    AdaptiveIdle& idle() { return *idle_; }
    const Config& config() const { return cfg_; }

    virtual void on_worker_start() {}

    virtual void enqueue_affine(std::coroutine_handle<> h) {
        (void)h;
    }

    // 重载：接受函数指针回调（Executor 路径）
    virtual void enqueue_affine(void(*fn)(void*), void* arg) {
        fn(arg);
        delete static_cast<folly::Func*>(arg);
    }

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
    size_t affine_q_idx_{SIZE_MAX};

    std::unique_ptr<adapt::QuantMemoryResource> mem_pool_;
    std::unique_ptr<adapt::ObjectPool<WorkItem>> work_item_pool_;

    WorkerPerf perf_;  // 性能计数（初始化时传递 id_）
};

}  // namespace storage::runtime
