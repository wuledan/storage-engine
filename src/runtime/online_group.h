#pragma once
#include "online_worker.h"
#include "work_item.h"
#include "worker_registry.h"
#include <cstddef>
#include <memory>
#include <string>
#include <vector>
#include <cstdint>

namespace storage::runtime {

struct OnlineGroupStats {
    size_t worker_count{0};
    uint64_t total_tasks_executed{0};
    uint64_t total_exec_ns{0};
};

class OnlineWorkerGroup {
public:
    struct Config {
        size_t num_workers{0};                   // 0 = auto (1 worker per physical core)
        uint32_t base_cpu_id{0};                 // 起始 CPU 编号（0=hwloc自动分配）
        std::string name_prefix{"storage-online"};
        
        // NUMA-aware distribution
        bool auto_discover_numa{true};           // 自动发现 NUMA 拓扑
        std::vector<size_t> per_numa_workers{};  // 手动指定每 NUMA 节点 worker 数
        
        // CPU pinning
        bool pin_cpu{true};                      // 绑核
        bool use_ht_siblings{false};             // 是否使用超线程
        bool bind_memory{true};                  // NUMA 内存绑定
        
        bool register_global{true};              // 注册到 WorkerRegistry
    };

    explicit OnlineWorkerGroup(const Config& cfg);
    ~OnlineWorkerGroup();

    // Non-copyable, movable
    OnlineWorkerGroup(const OnlineWorkerGroup&) = delete;
    OnlineWorkerGroup& operator=(const OnlineWorkerGroup&) = delete;

    void start();
    void stop();

    // 按 key 哈希路由到 worker
    size_t route_by_hash(uint64_t key) const noexcept;
    size_t route_by_numa_hash(uint64_t key, int numa_node) const noexcept;
    
    // 按 key 投递任务
    void submit_to(uint64_t key, WorkItem item);
    void submit_to_numa(int numa_node, WorkItem item);  // 投递到 NUMA 本地 worker

    // 获取特定 worker
    OnlineWorker& worker(size_t index);
    size_t worker_count() const noexcept { return workers_.size(); }
    
    // NUMA 信息
    int numa_node_count() const noexcept { return static_cast<int>(numa_worker_ranges_.size()); }
    const std::vector<OnlineWorker*>& get_numa_workers(int numa_node) const noexcept;

    OnlineGroupStats stats() const;

private:
    void discover_topology();

    std::vector<std::unique_ptr<OnlineWorker>> workers_;
    Config cfg_;
    std::vector<std::vector<OnlineWorker*>> numa_worker_lists_;  // per-NUMA worker pointers
    std::vector<size_t> numa_worker_ranges_;  // per-NUMA: start index in workers_
};

}  // namespace storage::runtime
