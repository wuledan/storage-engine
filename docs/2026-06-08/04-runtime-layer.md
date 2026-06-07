# Runtime 层详细设计

> 日期：2026-06-08
> 前置：03-execution-layer.md

## 1. 设计目标

构建通用的 Runtime 执行层，为 Online (RTC) 和 Offline (Worker-Stealing) 两种 Worker Group 提供统一的基础设施。核心要求：

- 每个 Worker 支持多种输入队列（网络 IO / 盘 IO / 引擎操作 / 亲和恢复），按优先级区分
- 每个执行线程内置调度器协程，负责跨队列 Polling 和任务选取
- 调度策略可插拔，通过统一 API 对接不同策略
- 任务批量获取（最小批次 1）
- 空闲时自适应休眠：快恢复 → 慢降级

## 2. 整体架构

```
┌─────────────────── Worker Thread ───────────────────────────┐
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                  Scheduler Coroutine                  │    │
│  │                                                      │    │
│  │   while (running) {                                 │    │
│  │     snapshots = collect_queue_snapshots();           │    │
│  │     decision = policy_->decide(snapshots);           │    │
│  │     if (decision.idle) → adaptive_sleep();           │    │
│  │     batch = queues[decision.idx].dequeue_batch(n);   │    │
│  │     execute(batch);                                  │    │
│  │   }                                                  │    │
│  └───────────────────┬─────────────────────────────────┘    │
│                      │ poll / batch dequeue                  │
│         ┌────────────┼────────────┬─────────────┐           │
│         ▼            ▼            ▼             ▼           │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐      │
│  │ NetIO    │ │ DiskIO   │ │ Engine   │ │ Affine   │ ...  │
│  │ Queue    │ │ Queue    │ │ Queue    │ │ Queue    │      │
│  │(SPSC)    │ │(SPSC)    │ │(MPSC)    │ │(MPMC)    │      │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘      │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                Adaptive Idle Controller               │    │
│  │   L0(SPIN) → L1(YIELD) → L2(PARK) → L3(DEEP)        │    │
│  │   快恢复 ◄────────────────────────── 慢降级           │    │
│  └─────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────┘
```

## 3. 任务类型与优先级

### 3.1 任务分类

| 队列类型 | 生产者 | 消费者 | 队列特性 | 基本优先级 |
|---------|--------|--------|---------|-----------|
| **Affine** | 其他 Worker（协程恢复） | 本 Worker | MPMC，低延迟优先 | P0 — 最高 |
| **NetIO** | IO Poller（网络完成） | 本 Worker | SPSC，批量 | P1 — 高 |
| **Timer** | Timer Engine | 本 Worker | MPSC | P1 — 高 |
| **Engine** | 引擎内部（查询/插入/扫描） | 本 Worker | MPSC | P2 — 中 |
| **DiskIO** | IO Poller（磁盘完成） | 本 Worker | SPSC，批量 | P2 — 中 |
| **Background** | 其它 Worker / 全局 | 本 Worker | MPSC | P3 — 低 |

### 3.2 优先级语义

```
P0 (Critical)  — 协程恢复：必须立即处理，否则连锁阻塞其他任务
P1 (High)      — 网络/定时器：直接影响请求延迟
P2 (Medium)    — 引擎/磁盘：吞吐核心，可批处理
P3 (Low)       — 后台维护：compaction kick-off、统计等
```

## 4. 队列抽象 (WorkQueue)

### 4.1 接口定义

```cpp
enum class QueueType : uint8_t {
    kAffine,
    kNetIO,
    kDiskIO,
    kEngine,
    kTimer,
    kBackground,
    kCustom,
};

enum class Priority : uint8_t {
    kCritical = 0,  // P0
    kHigh     = 1,  // P1
    kMedium   = 2,  // P2
    kLow      = 3,  // P3
};

class WorkQueue {
public:
    virtual ~WorkQueue() = default;

    // 非阻塞 dequeue 单个
    virtual bool try_dequeue(WorkItem& item) noexcept = 0;

    // 批量 dequeue，返回实际获取数 (≥0, ≤max_count)
    virtual size_t try_dequeue_batch(WorkItem* items, size_t max_count) noexcept = 0;

    // 快速读取当前队列中的任务数（近似即可，无需精确）
    virtual size_t approx_count() const noexcept = 0;

    // 队列元信息
    virtual QueueType type() const noexcept = 0;
    virtual Priority base_priority() const noexcept = 0;
    virtual const char* name() const noexcept = 0;

    // 通知队列有新任务到达（用于唤醒空闲 worker）
    virtual bool has_pending_notify() noexcept { return false; }
};

// 任务项
struct WorkItem {
    using Func = void(*)();

    union {
        Func func;                          // 普通回调
        std::coroutine_handle<> coro;       // 协程恢复
    };
    uint8_t tag;                            // 0=func, 1=coroutine
    uint32_t trace_id_lo;                   // 低 32 位 trace_id（调试用）

    void execute() noexcept {
        if (tag == 0) func();
        else coro.resume();
    }
};
```

### 4.2 具体队列实现选择

| 队列 | 实现 | 理由 |
|------|------|------|
| Affine | `MPMCQueue<T>` (folly::UMPMCQueue) | 多生产者（其他 worker）+ 单消费者 |
| NetIO | `SPSCQueue<T>` (Rigtorp) | 单生产者（IO Poller）+ 单消费者，高性能 |
| DiskIO | `SPSCQueue<T>` | 同上 |
| Engine | `MPSCQueue<T>` + ChaseLevDeque | 多生产者（引擎内部并发提交）+ 单消费者 |
| Timer | `MPSCQueue<T>` | 定时器线程 → worker 线程 |
| Background | `MPSCQueue<T>` | 多来源 + 单消费者 |

## 5. 调度策略 API (SchedulingPolicy)

### 5.1 接口定义

```cpp
// 调度器可见的队列快照
struct QueueSnapshot {
    QueueType type;
    Priority priority;
    size_t approx_count;                   // 队列中待处理任务数
    uint64_t last_poll_ns;                 // 上次 poll 的时间戳
    uint64_t total_dequeued;               // 累计出队数
};

// 调度策略的输出决策
struct ScheduleDecision {
    size_t queue_index;                    // 选中的队列索引
    size_t batch_size;                     // 本次取出的任务数 (≥1)

    // 空闲决策
    bool idle{false};                      // true = 所有队列为空，进入休眠
    bool idle_fast_recovery{true};         // true = 使用快恢复路径
};

// 基础调度策略
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
```

### 5.2 内置策略

#### 5.2.1 StrictPriorityPolicy（严格优先级）

```
算法：
  for each queue in priority order (P0→P3):
    if queue.approx_count > 0:
      return {queue, batch_size = min(approx_count, kMaxBatch)}
  return {idle = true}
```

#### 5.2.2 WeightedFairPolicy（加权公平）

```
算法：
  每个队列有预设权重 w[i]
  每个队列维护虚拟时间 virtual_time[i]
  
  选择 virtual_time 最小的非空队列
  取出 batch_size 个任务
  更新 virtual_time += batch_size / w[i]

  所有队列空 → idle
```

#### 5.2.3 DeadlineAwarePolicy（截止时间感知）

```
算法：
  每个 WorkItem 可选携带 deadline
  优先选择 deadline 最近的队列
  同 deadline 按 priority 降序
  无 deadline 任务退化为 StrictPriority
```

#### 5.2.4 策略配置

```cpp
// 策略工厂
std::unique_ptr<SchedulingPolicy> make_policy(std::string_view name, const PolicyConfig& cfg);

// 内置策略名
// "strict_priority" — 严格优先级
// "weighted_fair"   — 加权公平
// "deadline_aware"  — 截止时间感知
// "round_robin"     — 简单轮询
```

## 6. 调度器协程 (Scheduler)

### 6.1 核心结构

```cpp
class Scheduler {
public:
    Scheduler(SchedulingPolicy* policy, AdaptiveIdle* idle);

    // 注册一个输入队列，返回队列索引
    size_t register_queue(std::unique_ptr<WorkQueue> queue);

    // 启动调度循环（作为协程运行）
    folly::coro::Task<void> run();

    // 请求停止（优雅退出）
    void request_stop();

private:
    std::vector<std::unique_ptr<WorkQueue>> queues_;
    SchedulingPolicy* policy_;
    AdaptiveIdle* idle_;
    std::atomic<bool> running_{false};
};
```

### 6.2 调度循环

```cpp
folly::coro::Task<void> Scheduler::run() {
    running_.store(true, std::memory_order_release);

    std::vector<QueueSnapshot> snapshots(queues_.size());
    std::vector<WorkItem> batch(kMaxBatchSize);

    while (running_.load(std::memory_order_acquire)) {
        // Step 1: 收集队列快照
        for (size_t i = 0; i < queues_.size(); ++i) {
            snapshots[i] = {
                .type = queues_[i]->type(),
                .priority = queues_[i]->base_priority(),
                .approx_count = queues_[i]->approx_count(),
                .last_poll_ns = last_poll_time_[i],
                .total_dequeued = total_dequeued_[i],
            };
        }

        // Step 2: 获取调度决策
        auto decision = policy_->decide(snapshots, stats_);

        // Step 3: 空闲处理
        if (decision.idle) {
            idle_->enter_idle(decision.idle_fast_recovery);
            continue;  // 唤醒后重新评估
        }

        // Step 4: 批量取出任务
        auto& queue = queues_[decision.queue_index];
        size_t n = queue->try_dequeue_batch(
            batch.data(),
            std::min(decision.batch_size, kMaxBatchSize));

        if (n == 0) continue;  // 竞态：队列在快照后变空

        // Step 5: 执行任务
        last_poll_time_[decision.queue_index] = now_ns();
        for (size_t i = 0; i < n; ++i) {
            auto t0 = now_ns();
            batch[i].execute();
            auto t1 = now_ns();
            policy_->on_task_completed(
                queue->type(), t1 - t0);
        }
        total_dequeued_[decision.queue_index] += n;
    }

    co_return;
}
```

### 6.3 关键设计点

1. **单调度器协程 + 单 Worker 线程**：调度器本身是一个协程，运行在 Worker 线程上。任务执行可能挂起（co_await），此时调度器协程也挂起，Worker 线程通过 poll 恢复调度器或其他协程。

2. **批量获取**：一次策略决策可返回 `batch_size` 个任务，减少调度开销，支持 IO 完成批量处理。

3. **快照-决策-执行分离**：快照阶段不持锁，决策纯计算，执行阶段独立。

## 7. 自适应空闲控制 (AdaptiveIdle)

### 7.1 状态机

```
          ┌──────────────────────────────────────┐
          │                                      │
          ▼                                      │
   ┌──────────┐    idle_time > T0    ┌──────────┐
   │  ACTIVE  │ ──────────────────► │  SPIN    │
   │          │ ◄────────────────── │  (L0)    │
   └──────────┘    new task arrives  └────┬─────┘
                                         │ idle_time > T1
                                         ▼
                                    ┌──────────┐    idle_time > T2   ┌──────────┐
                                    │  YIELD   │ ─────────────────► │  PARK    │
                                    │  (L1)    │                    │  (L2)    │
                                    └────┬─────┘                    └────┬─────┘
                                         │                              │
                                         │ new task                      │ idle_time > T3
                                         ▼                              ▼
                                    ┌──────────┐                  ┌──────────┐
                                    │  ACTIVE  │                  │  DEEP    │
                                    │          │                  │  (L3)    │
                                    └──────────┘                  └──────────┘
```

### 7.2 各阶段参数

| 级别 | 名称 | 机制 | 延迟 | CPU 开销 | 触发条件 |
|------|------|------|------|---------|---------|
| L0 | SPIN | `__builtin_ia32_pause` 循环 | ~100ns | 100% | idle_time < 10μs |
| L1 | YIELD | `std::this_thread::yield()` | ~1-10μs | 可抢占 | idle_time < 50μs |
| L2 | PARK | `futex(FUTEX_WAIT)` 可中断 | ~5-20μs | ~0% | idle_time < 10ms |
| L3 | DEEP | `epoll_wait` + timerfd，长休眠 | ~50μs+ | ~0% | idle_time ≥ 10ms |

### 7.3 快恢复机制

```
任务到达 → 检查当前 Worker 是否在 IDLE
         → 如果是 SPIN:    无需操作，worker 正在自旋检查
         → 如果是 YIELD:   无需操作，worker 很快返回
         → 如果是 PARK:    futex_wake，worker 在 ~5-20μs 内恢复
         → 如果是 DEEP:    eventfd_write，worker 在 ~50μs 内恢复

唤醒后立即重置 idle_level = 0，重置 idle_time = 0
```

### 7.4 接口定义

```cpp
class AdaptiveIdle {
public:
    struct Config {
        std::chrono::nanoseconds spin_threshold{10'000};     // 10μs
        std::chrono::nanoseconds yield_threshold{50'000};    // 50μs
        std::chrono::nanoseconds park_threshold{10'000'000}; // 10ms
        uint32_t spin_iterations{100};                       // ~10μs @100ns/iter
        uint32_t yield_rounds{5};
    };

    explicit AdaptiveIdle(const Config& cfg);

    // 进入空闲循环，返回时表示有新任务或超时
    void enter_idle(bool fast_recovery);

    // 通知有新任务到达（非 owner 线程调用）
    void notify();

    // 获取一个可 poll 的 fd（用于 epoll 集成，deep 级别使用）
    int notify_fd() const noexcept;

private:
    // 四级退避
    void idle_spin();
    void idle_yield();
    void idle_park();
    void idle_deep();

    Config cfg_;
    int event_fd_;                      // eventfd，用于 notify 和 epoll
    std::mutex park_mutex_;
    std::condition_variable park_cv_;
    std::atomic<IdleLevel> level_{IdleLevel::kActive};
    std::atomic<uint64_t> idle_start_ns_{0};
};
```

## 8. Worker 基类

### 8.1 接口

```cpp
class Worker {
public:
    struct Config {
        uint32_t cpu_id{0};                     // CPU 亲和绑定
        uint32_t numa_node{0};
        SchedulingPolicy::Config policy_cfg;
        AdaptiveIdle::Config idle_cfg;
        size_t max_batch_size{64};
    };

    explicit Worker(const Config& cfg);

    // 注册队列（必须在 start 前完成）
    size_t add_queue(std::unique_ptr<WorkQueue> queue);

    // 设置调度策略（必须在 start 前完成）
    void set_policy(std::unique_ptr<SchedulingPolicy> policy);

    // 启动 worker 线程
    void start();

    // 停止
    void stop();
    void join();

    // 通知唤醒
    void notify();

    // 统计
    WorkerStats stats() const;

    size_t worker_id() const noexcept { return id_; }

private:
    void worker_loop();

    size_t id_;
    Config cfg_;
    std::unique_ptr<Scheduler> scheduler_;
    std::unique_ptr<AdaptiveIdle> idle_;
    std::thread thread_;
};
```

## 9. 与 Online/Offline Worker 的关系

### 9.1 Online Worker

```cpp
class OnlineWorker : public Worker {
    // 继承基础 Worker 的多队列 + 调度器 + 自适应空闲

    // 额外的 Online 特化：
    // - 不参与工作窃取
    // - 独占 IO 队列（per-worker io_uring）
    // - 调度策略偏向 Affine/NetIO 优先级
    // - strict_priority 或 deadline_aware 策略
};
```

### 9.2 Offline Worker

```cpp
class OfflineWorker : public Worker {
    // 继承基础 Worker 的多队列 + 调度器 + 自适应空闲

    // 额外的 Offline 特化：
    // - 注册一个额外的 StealableQueue（可被其他 worker 窃取）
    // - 空闲时尝试从同 NUMA 节点的其他 Offline Worker 窃取
    // - 调度策略偏向 Engine/DiskIO
    // - weighted_fair 策略
};
```

### 9.3 差异对比

| 维度 | Online Worker | Offline Worker |
|------|--------------|----------------|
| 队列组合 | Affine + NetIO + Engine + DiskIO + Timer | Affine + Engine + DiskIO + Timer + Stealable |
| 调度策略 | strict_priority / deadline_aware | weighted_fair |
| 空闲行为 | 快速 SPIN 优先（低延迟） | 允许较长时间 PARK |
| 窃取 | 无 | 从同 NUMA 的 Offline Worker 窃取 |
| CPU | 独占物理核 | 共享核组 |
| MAX_BATCH | 较小 (16-32) | 较大 (64-128) |

## 10. 数据流示例

### 读请求路径

```
1. RPC 层接收请求
2. 一致性哈希 → 确定 target Online Worker
3. 投递请求到 Worker 的 Engine Queue
4. 调度器 poll Engine Queue → 取出请求 → 执行引擎查询
5. 引擎需要读磁盘 → 提交 IO 到 io_uring → co_await 挂起
6. IO Poller 检测完成 → 投递完成事件到 Worker 的 DiskIO Queue + Affine Queue
7. 调度器 poll Affine Queue → 恢复协程 → 读取完成
8. 引擎组装响应
9. 投递响应到 NetIO Queue → 网络线程发送
```

### Compaction 路径

```
1. Engine 触发 compaction → 投递到 Offline Worker 的 Engine Queue
2. Offline 调度器 poll Engine Queue → 执行 compaction 任务
3. 大量 IO → 批量提交 → co_await 挂起
4. IO 完成 → Affine Queue 恢复
5. 任务执行完毕 → 通知 Online Worker：compaction 完成
```
