#pragma once
#include "types.h"
#include <cstddef>
#include <cstdint>

namespace storage::runtime {

// 调度器可见的队列快照
struct QueueSnapshot {
    QueueType type{};
    Priority priority{Priority::kLow};
    size_t approx_count{0};          // 队列中待处理任务数
    uint64_t last_poll_ns{0};        // 上次 poll 的时间戳 (ns)
    uint64_t total_dequeued{0};      // 累计出队数
};

// 调度策略的输出决策
struct ScheduleDecision {
    size_t queue_index{0};           // 选中的队列索引
    size_t batch_size{1};            // 本次取出的任务数 (≥1)

    bool idle{false};                // true = 所有队列为空，进入休眠
    bool idle_fast_recovery{true};   // true = 使用快恢复路径
};

}  // namespace storage::runtime
