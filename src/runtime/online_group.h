#pragma once
#include "online_worker.h"
#include "work_item.h"
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
        size_t num_workers{4};
        uint32_t base_cpu_id{0};     // 起始 CPU 编号
        std::string name_prefix{"storage-online"};
    };

    explicit OnlineWorkerGroup(const Config& cfg);

    void start();
    void stop();

    // 按 key 哈希路由到 worker
    size_t route_by_hash(uint64_t key) const noexcept;
    // 按 key 投递任务
    void submit_to(uint64_t key, WorkItem item);

    // 获取特定 worker
    OnlineWorker& worker(size_t index);
    size_t worker_count() const noexcept { return workers_.size(); }

    OnlineGroupStats stats() const;

private:
    std::vector<std::unique_ptr<OnlineWorker>> workers_;
    Config cfg_;
};

}  // namespace storage::runtime
