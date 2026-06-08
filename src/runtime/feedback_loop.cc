#include "feedback_loop.h"
#include "online_group.h"
#include "online_worker.h"
#include "local_work_queue.h"
#include "affine_work_queue.h"
#include <chrono>

namespace storage::runtime {

FeedbackLoop::FeedbackLoop(MetricsCollector& collector,
                           StrategyDecider& decider,
                           StrategyExecutor& executor,
                           const Config& cfg)
    : collector_(collector), decider_(decider), executor_(executor), cfg_(cfg) {
    hw_ = HardwareTopology::probe();
    // 默认 RTC (DirectAll) 模式 — 非网络IO分离
    current_plan_.strategy = DispatchStrategy::kDirectAll;
    current_plan_.direct_workers = 4;
}

void FeedbackLoop::bind_workers(OnlineWorkerGroup* workers) {
    workers_ = workers;
    if (workers_) {
        current_plan_.direct_workers = 0;
        current_plan_.consumers_per_dispatch = workers_->worker_count();
    }
}

bool FeedbackLoop::tick() {
    tick_count_++;

    // 1. 采集指标
    auto metrics = collector_.collect();
    if (metrics.elapsed_seconds < 1.0) return false;  // 样本太短

    // 2. 估计容量
    size_t active_workers = workers_ ? workers_->worker_count() : 1;
    auto cap = RealTimeCapacity::estimate(metrics, active_workers);

    // 3. 决策
    auto new_plan = decider_.decide(cap, current_plan_, hw_);
    if (!new_plan) {
        // 无调整建议 → 重置 pending
        pending_plan_ = std::nullopt;
        consecutive_same_ = 0;
        return false;
    }

    // 4. 防抖动
    if (pending_plan_ && *pending_plan_ == *new_plan) {
        consecutive_same_++;
    } else {
        pending_plan_ = *new_plan;
        consecutive_same_ = 1;
    }

    // 5. 达到阈值 → 执行
    if (consecutive_same_ >= cfg_.consecutive_samples) {
        bool ok = apply_plan(*pending_plan_);
        if (ok) {
            executor_.record_decision(current_plan_, *pending_plan_, cap,
                cap.utilization > 0.8 ? "overload" :
                cap.utilization < 0.3 ? "underload" : "latency_spike");
            current_plan_ = *pending_plan_;
        }
        pending_plan_ = std::nullopt;
        consecutive_same_ = 0;
        return ok;
    }

    return false;
}

bool FeedbackLoop::apply_plan(const DispatchPlan& plan) {
    if (!workers_) return false;

    for (size_t i = 0; i < workers_->worker_count(); ++i) {
        bool is_direct = (i < plan.direct_workers);
        auto& w = workers_->worker(i);

        if (is_direct) {
            w.swap_engine_queue(std::make_unique<LocalWorkQueue>(
                QueueType::kEngine, Priority::kMedium, "engine-direct", 200000));
        } else {
            w.swap_engine_queue(std::make_unique<AffineWorkQueue>(
                QueueType::kEngine, Priority::kMedium, "engine-dispatch", 200000));
        }
    }
    return true;
}

}  // namespace storage::runtime
