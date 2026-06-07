#pragma once
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <atomic>

namespace storage::runtime {
namespace adapt {
class WorkStealingExecutor;
}

struct WorkItem;

struct OfflineGroupStats {
    uint64_t tasks_submitted{0};
    uint64_t tasks_completed{0};
    uint64_t local_pops{0};
    uint64_t local_steals{0};
    uint64_t failed_steals{0};
    uint64_t park_count{0};
    double utilization{0.0};
};

class OfflineWorkerGroup {
public:
    struct Config {
        size_t num_workers{2};
        std::string name_prefix{"storage-offline"};
    };

    explicit OfflineWorkerGroup(const Config& cfg);
    ~OfflineWorkerGroup();

    void start();
    void stop();

    void submit(WorkItem item);
    void submit_to_worker(size_t worker_id, WorkItem item);

    size_t worker_count() const noexcept { return cfg_.num_workers; }
    OfflineGroupStats stats() const;
    bool is_running() const noexcept;

private:
    Config cfg_;
    std::unique_ptr<adapt::WorkStealingExecutor> executor_;
};

}  // namespace storage::runtime
