#pragma once
#include "schedule_types.h"
#include <cstdint>
#include <string_view>
#include <vector>

namespace storage::runtime {

struct SchedulerStats {
    uint64_t total_polls{0};
    uint64_t total_idles{0};
    uint64_t total_tasks_executed{0};
    uint64_t total_exec_ns{0};
};

class SchedulingPolicy {
public:
    virtual ~SchedulingPolicy() = default;

    // 策略名称
    virtual std::string_view name() const noexcept = 0;

    // 核心决策：根据队列快照决定从哪个队列取多少任务
    // 如果返回 idle=true，调度器进入自适应休眠
    virtual ScheduleDecision decide(
        const std::vector<QueueSnapshot>& queues,
        const SchedulerStats& stats) noexcept = 0;

    // 任务执行完成回调（用于记账、更新权重等）
    virtual void on_task_completed(QueueType queue, uint64_t exec_ns) noexcept {}
};

}  // namespace storage::runtime
