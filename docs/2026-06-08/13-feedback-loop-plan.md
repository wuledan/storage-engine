# Dispatch 决策系统：闭环实现计划

> 日期：2026-06-08
> 原则：观测驱动，闭环决策，自适应收敛

## 1. 闭环架构

```
         ┌──────────────────────────────────────┐
         │           Runtime (Running)           │
         │                                      │
         │  WorkerPerf ──► MetricsCollector     │
         │  Scheduler    ──► (per-queue stats)  │
         │  IO Backend   ──► io completion rate │
         │                                      │
         └──────────────┬───────────────────────┘
                        │ snapshot every T seconds
                        ▼
         ┌──────────────────────────────────────┐
         │       CapacityEstimator              │
         │                                      │
         │  λ  = enqueue_rate / Δt              │
         │  μ  = 1 / avg_exec_ns                │
         │  ρ  = busy_time / total_time         │
         │  L  = avg_queue_depth                │
         │                                      │
         │  输出: RealTimeCapacity              │
         └──────────────┬───────────────────────┘
                        │
                        ▼
         ┌──────────────────────────────────────┐
         │       StrategyDecider                │
         │                                      │
         │  BottleneckAnalysis                  │
         │  QueueTheoryLatencyPredict           │
         │  StrategyComparison                  │
         │                                      │
         │  输出: DispatchPlan (recommended)    │
         └──────────────┬───────────────────────┘
                        │ if plan != current
                        ▼
         ┌──────────────────────────────────────┐
         │       StrategyExecutor               │
         │                                      │
         │  validate(hardware_feasibility)      │
         │  apply(queue_type_swap)              │
         │  apply(dispatch_start/stop)          │
         │  record(decision_log)                │
         └──────────────┬───────────────────────┘
                        │
                        ▼
              back to Runtime (next cycle)
```

## 2. 数据流

### 2.1 MetricsCollector — 观测数据采集

从已有的 WorkerPerf / IO backend 中提取建模所需指标：

```cpp
struct RuntimeMetrics {
    uint64_t snapshot_ns;          // 快照时间戳
    
    // 全局
    double elapsed_seconds;         // 本次采样间隔
    size_t total_enqueued;          // 所有队列入队总数
    size_t total_executed;          // 所有队列执行总数
    
    // 按队列类型分
    struct QueueMetrics {
        QueueType type;
        size_t enqueued_delta;      // 本周期入队增量
        size_t executed_delta;      // 本周期执行增量
        uint64_t total_exec_ns;     // 总执行时间
        uint64_t avg_wait_ns;       // 平均排队延迟
        uint64_t max_queue_depth;   // 峰值队列深度
        double p50_wait_ns;
        double p99_wait_ns;
    };
    std::vector<QueueMetrics> per_queue;
    
    // Worker 利用率
    struct WorkerUtilization {
        size_t worker_id;
        uint64_t busy_ns;           // 调度循环 busy 时间
        uint64_t idle_ns;           // 进入 idle 的时间（含 SPIN/YIELD/PARK）
        uint64_t park_count;        // PARK 次数
    };
    std::vector<WorkerUtilization> per_worker;

    // IO backend
    double io_ops_per_sec;
    double io_bytes_per_sec;
};
```

**采集方式**：周期调用现有 API

```cpp
class MetricsCollector {
public:
    // 绑定数据源
    void attach_worker(size_t idx, WorkerPerf* perf, SchedulerStats* sched);
    void attach_io_backend(IIOBackend* backend);

    // 采集快照（计算 delta）
    RuntimeMetrics collect();

private:
    // 差分计算：本周期增量 = 当前累加值 - 上次快照
    struct WorkerSource {
        WorkerPerf* perf;
        uint64_t last_enqueued[8];   // 按 QueueType
        uint64_t last_executed[8];
    };
    std::vector<WorkerSource> worker_sources_;
    IIOBackend* io_backend_{nullptr};
    uint64_t last_collect_ns_{0};
};
```

### 2.2 CapacityEstimator — 实时容量估计

```cpp
struct RealTimeCapacity {
    // 测量值（从 RuntimeMetrics 推导）
    double arrival_rate;         // λ (req/s)
    double service_rate_per_core; // μ_eng (req/s/core)
    double io_rate_per_core;     // μ_disk (IOPS/core)
    size_t active_workers;       // 当前 worker 数

    // 推导值
    double utilization;          // ρ = λ / (N * μ)
    double avg_queue_depth;      // L
    double avg_latency_us;       // 实测 P50

    // 预估容量
    double max_throughput;       // 当前配置的极限吞吐
    
    BottleneckAnalysis::Stage bottleneck();
    double headroom_pct();       // 容量余量百分比
};

class CapacityEstimator {
public:
    RealTimeCapacity estimate(const RuntimeMetrics& m);
};
```

**推导公式**：

```
λ = total_enqueued_delta / elapsed_seconds
μ = total_executed_delta / (elapsed_seconds * N_workers)
ρ = (busy_ns_total / (elapsed_seconds * N_workers * 1e9))
L = avg(per_queue.avg_queue_depth)

余量 = (1 - ρ) * 100%

瓶颈判定:
  if ρ > 0.9: 当前限制因子 = bottleneck_from_queue_depth_max()
  else: bottleneck = NONE (有余量)
```

### 2.3 StrategyDecider — 策略决策

```cpp
class StrategyDecider {
public:
    struct Config {
        // 触发阈值
        double utilization_high{0.85};    // 利用率 > 此值触发扩容
        double utilization_low{0.30};     // 利用率 < 此值触发缩容
        double latency_p99_degrade{2.0};  // P99 超过基线 N 倍触发切换
        
        // 决策间隔
        uint64_t decision_interval_ms{5000};  // 5s 重新决策
        
        // 稳定性
        size_t consecutive_samples{3};    // 连续 N 次同结论才执行
        double hysteresis{0.1};           // 滞回区间防抖动
    };

    // 核心决策
    std::optional<DispatchPlan> decide(
        const RealTimeCapacity& cap,
        const DispatchPlan& current_plan,
        const HardwareTopology& hw);
};
```

**决策逻辑**：

```cpp
std::optional<DispatchPlan> StrategyDecider::decide(...) {
    // Step 1: 是否需要调整？
    if (cap.utilization < cfg_.utilization_high - cfg_.hysteresis &&
        cap.utilization > cfg_.utilization_low + cfg_.hysteresis) {
        return std::nullopt;  // 健康状态，不调整
    }

    // Step 2: 过载 → 扩容
    if (cap.utilization > cfg_.utilization_high) {
        auto plan = compute_optimal_plan_for_overload(cap, hw);
        if (plan != current_plan) {
            return plan;
        }
    }

    // Step 3: 低载 → 缩容（回收 CPU 给 Offline）
    if (cap.utilization < cfg_.utilization_low) {
        auto plan = compute_optimal_plan_for_underload(cap, hw);
        if (plan != current_plan) {
            return plan;
        }
    }

    // Step 4: 延迟异常 → 切换策略
    if (cap.avg_latency_us > baseline_latency_ * cfg_.latency_p99_degrade) {
        auto plan = compute_optimal_plan_for_low_latency(cap, hw);
        if (plan != current_plan) {
            return plan;
        }
    }

    return std::nullopt;
}
```

### 2.4 StrategyExecutor — 策略执行

```cpp
class StrategyExecutor {
public:
    // 执行策略变更，返回是否成功
    bool apply(const DispatchPlan& plan, OnlineWorkerGroup& online_group);

    // 记录决策日志（用于事后分析）
    void record_decision(const DispatchPlan& from, const DispatchPlan& to,
                         const RealTimeCapacity& cap_at_decision);

    struct DecisionRecord {
        uint64_t timestamp_ns;
        DispatchPlan from;
        DispatchPlan to;
        double utilization_at_decision;
        double p99_latency_at_decision;
        std::string reason;
    };
    std::vector<DecisionRecord> history() const;
};
```

**执行步骤**：

```cpp
bool StrategyExecutor::apply(const DispatchPlan& plan, 
                              OnlineWorkerGroup& online_group) {
    // 1. 验证：检查硬件资源是否支持
    if (!validate_hardware(plan)) {
        return false;
    }

    // 2. 应用队列类型变更
    for (size_t i = 0; i < online_group.worker_count(); ++i) {
        bool is_direct = i < plan.direct_workers;
        auto& worker = online_group.worker(i);
        
        if (is_direct) {
            worker.swap_engine_queue(
                std::make_unique<LocalWorkQueue>(/*...*/));
        } else {
            worker.swap_engine_queue(
                std::make_unique<AffineWorkQueue>(/*...*/));
        }
    }

    // 3. 启动/停止 Dispatch 线程
    if (plan.strategy == DispatchPlan::kDispatchSingle ||
        plan.strategy == DispatchPlan::kDispatchMultiple) {
        ensure_dispatch_running(plan.dispatch_threads);
    } else {
        stop_all_dispatchers();
    }

    // 4. 记录决策
    record_decision(previous_plan_, plan, current_capacity_);
    previous_plan_ = plan;

    return true;
}
```

### 2.5 FeedbackLoop — 闭环控制器

```cpp
class FeedbackLoop {
public:
    FeedbackLoop(OnlineWorkerGroup& online_group,
                 MetricsCollector& collector,
                 StrategyDecider& decider,
                 StrategyExecutor& executor);

    // 运行闭环（在 Runtime 的 timer 线程中调用）
    void tick();

    // 统计
    size_t decision_count() const;
    const std::vector<StrategyExecutor::DecisionRecord>& history() const;

private:
    OnlineWorkerGroup& online_group_;
    MetricsCollector& collector_;
    StrategyDecider& decider_;
    StrategyExecutor& executor_;
    
    DispatchPlan current_plan_;
    HardwareTopology hw_;

    // 防抖动：连续 N 次同结论才切换
    std::optional<DispatchPlan> pending_plan_;
    size_t consecutive_same_{0};
    size_t required_consecutive_{3};

    uint64_t last_tick_ns_{0};
};
```

**闭环主循环**：

```cpp
void FeedbackLoop::tick() {
    // 1. 采集指标
    auto metrics = collector_.collect();
    if (metrics.elapsed_seconds < 1.0) return;  // 样本太短

    // 2. 估计容量
    auto cap = CapacityEstimator{}.estimate(metrics);

    // 3. 决策
    auto new_plan = decider_.decide(cap, current_plan_, hw_);

    // 4. 防抖动
    if (new_plan) {
        if (pending_plan_ && *pending_plan_ == *new_plan) {
            consecutive_same_++;
            if (consecutive_same_ >= required_consecutive_) {
                // 执行切换
                executor_.apply(*new_plan, online_group_);
                current_plan_ = *new_plan;
                pending_plan_ = std::nullopt;
                consecutive_same_ = 0;
            }
        } else {
            pending_plan_ = new_plan;
            consecutive_same_ = 1;
        }
    } else {
        pending_plan_ = std::nullopt;
        consecutive_same_ = 0;
    }
}
```

## 3. OnlineWorker 队列热切换

### 3.1 安全交换

```cpp
class OnlineWorker : public Worker {
public:
    // 原子交换 engine 队列（不影响正在运行的调度循环）
    void swap_engine_queue(std::unique_ptr<WorkQueue> new_queue) {
        // Step 1: 获取调度器锁（或利用单线程特性）
        // Step 2: 排空旧队列中剩余任务
        drain_queue(idx_engine_);
        // Step 3: 替换队列指针
        scheduler().swap_queue(idx_engine_, std::move(new_queue));
        // Step 4: 记录切换
        perf().record_queue_swap(old_type, new_type);
    }
private:
    void drain_queue(size_t idx);  // 将旧队列任务移动到新队列
};
```

### 3.2 排空协议

```
swap_engine_queue:
  while (old_queue->try_dequeue(item)):
    new_queue->enqueue(item)          // 转移到新队列
  // 此时可能还有并发入队（Dispatch 模式）
  // 再次排空
  while (old_queue->try_dequeue(item)):
    new_queue->enqueue(item)
  // 替换
  scheduler.replace_queue(idx, new_queue)
```

## 4. 运行时集成

```cpp
class Runtime {
public:
    void start() {
        // 1. 硬件探测
        auto hw = HardwareProbe::probe();

        // 2. 初始策略 (偏保守: DispatchSingle, 收集数据)
        DispatchPlan initial_plan = DispatchPlan::conservative_default(hw);

        // 3. 创建组件
        online_group_ = OnlineWorkerGroup(online_cfg_with(initial_plan));
        collector_ = MetricsCollector();
        for (auto& w : online_group_.workers()) {
            collector_.attach_worker(w.id(), &w.perf(), &w.scheduler_stats());
        }

        // 4. 启动 Runtime
        online_group_.start();
        offline_group_.start();

        // 5. 启动闭环（在 timer 线程中）
        feedback_loop_ = FeedbackLoop(online_group_, collector_, decider_, executor_);
        timer_.schedule_periodic(5s, [this] { feedback_loop_.tick(); });
    }

private:
    OnlineWorkerGroup online_group_;
    OfflineWorkerGroup offline_group_;
    MetricsCollector collector_;
    StrategyDecider decider_;
    StrategyExecutor executor_;
    FeedbackLoop feedback_loop_;
};
```

## 5. 实现任务

| Task | 内容 | 工时 | 新文件 |
|------|------|------|--------|
| H1 | `RuntimeMetrics` + `MetricsCollector` (从已有 perf/IO 采集) | 3h | `src/runtime/metrics_collector.h/cc` |
| H2 | `RealTimeCapacity` + `CapacityEstimator` (μ/λ/ρ 估计) | 2h | `src/runtime/capacity_estimator.h/cc` |
| H3 | `StrategyDecider` (容量→策略) + 防抖动 | 2h | `src/runtime/strategy_decider.h/cc` |
| H4 | `StrategyExecutor` (队列热切换 + dispatch 启停) | 3h | `src/runtime/strategy_executor.h/cc` |
| H5 | `FeedbackLoop` (闭环控制器) + `HardwareProbe` 桩 | 2h | `src/runtime/feedback_loop.h/cc` |
| H6 | `Runtime` 集成 + 测试（mock 指标 → 决策验证 → 切换验证） | 3h | 修改 `src/runtime/runtime.cc` |
| **总计** | **6 任务** | **15h** | |

### 测试验证点

```
H6-1: 过载检测 → ρ > 0.85 → 扩容建议 → 连续3次 → 执行
H6-2: 低载检测 → ρ < 0.30 → 缩容建议 → 连续3次 → 执行
H6-3: 防抖动   → ρ 在 0.85 上下振荡 → 不触发切换
H6-4: 延迟异常 → P99 飙升 → 切换到 Direct 模式
H6-5: 队列热切换 → swap_engine_queue 不丢任务
H6-6: 决策历史  → 完整记录可查询
```

## 6. 闭环效果

```
初始状态:                收敛后:
  保守 DispatchSingle        DirectAll
  ρ = 0.3                    ρ = 0.85
  P99 = 3.5μs                P99 = 1.5μs
  
  tick 1: ρ=0.3, 低载 → 保持观察
  tick 2: ρ=0.45, 正常 → 无调整
  tick 3: λ 上升, ρ=0.65 → 分析: Direct 收益 > Dispatch
  tick 4: ρ=0.65 (2nd) → pending
  tick 5: ρ=0.67 (3rd) → 执行切换 DirectAll
  tick 6: ρ=0.72, P99=1.5μs → 收敛, 延迟降低 57%
```
