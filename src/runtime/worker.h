#pragma once
#include "scheduler.h"
#include "adaptive_idle.h"
#include "scheduling_policy.h"
#include "policy_factory.h"
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
        uint32_t cpu_id{0};
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

protected:
    Scheduler& scheduler() { return scheduler_; }
    AdaptiveIdle& idle() { return *idle_; }
    const Config& config() const { return cfg_; }
    WorkQueue* get_queue(size_t idx) const { return scheduler_.get_queue(idx); }

    // 子类覆写：在 start 的线程入口处自定义初始化
    virtual void on_worker_start() {}

private:
    void worker_loop();

    static std::atomic<size_t> next_id_;

    size_t id_;
    Config cfg_;
    std::unique_ptr<AdaptiveIdle> idle_;
    std::unique_ptr<SchedulingPolicy> policy_;
    Scheduler scheduler_;
    std::thread thread_;
};

}  // namespace storage::runtime
