# Runtime 层实现计划 (v2)

> 日期：2026-06-08
> 设计：04-runtime-layer.md  +  修正：06-runtime-refinement.md

---

## 测试标准

### 三级测试体系

| 级别 | 目录 | 目标 | 覆盖 |
|------|------|------|------|
| **单元测试** | `tests/unit/` | 单组件接口契约、边界条件 | 每行代码 |
| **模块测试** | `tests/module/` | 组件间集成、并发正确性 | 跨组件交互 |
| **压力/稳定性测试** | `tests/stress/` | 高负载正确性、长稳不漏 | 全系统 |

### 强制要求

每个组件交付时必须通过：

- **ASAN** (AddressSanitizer) — 内存安全
- **TSAN** (ThreadSanitizer) — 数据竞争
- **UBSAN** (UndefinedBehaviorSanitizer) — 未定义行为

### 压力/稳定性测试标准

| 维度 | 指标 | 阈值 |
|------|------|------|
| **并发正确性** | 多生产者多消费者，元素不丢不重 | 100% 通过 |
| **高压力吞吐** | 持续满负荷投递并消费 | 运行 60s，吞吐抖动 < 10% |
| **长稳运行** | 启动→投递→空闲→重复，持续运行 | ≥ 30 分钟无异常 |
| **内存泄漏** | RSS 增长趋势 | 稳态后 RSS 增长 < 1MB/min |
| **竞态条件** | TSAN 告警数 | 0 |
| **资源回收** | 线程/协程/fd 泄漏 | stop 后 5s 内全部回收 |
| **异常路径** | 空队列/满队列/stop 中投递 | 不崩溃/不阻塞/不丢数据 |

---

## 依赖关系图 (DAG)

```
Phase 1 ─────────────────────────────────────────
  T1:  项目骨架 + CMake + GTest + 三San集成
  T2:  WorkItem / QueueType / Priority
  T3a: LocalQueue (非原子) + BatchedSPSCQueue
  T3b: 导入 quant/infra 组件
       ├── ChaseLevDeque
       ├── AffinityBaton/Mutex/SharedMutex
       ├── WorkStealingExecutor
       ├── TimerScheduler (适配)
       ├── StatRegistry
       ├── ObjectPool / MemoryPool
      │
Phase 2 ─────────────────────────────────────────
  T4:  WorkQueue 抽象接口 (含 Local/Batched/Remote 语义)  (T2)
  T5:  LocalWorkQueue (非原子, worker-only)              (T3a, T4)
  T6:  BatchedSPSCWorkQueue (批量SPSC, IO完成)             (T3a, T4)
  T7:  AffineWorkQueue (MPMC, 协程恢复)                   (T4)
      │
Phase 3 ─────────────────────────────────────────
  T8:  QueueSnapshot / ScheduleDecision                    (T4)
  T9:  SchedulingPolicy 接口                               (T8)
  T10: StrictPriorityPolicy                                (T9)
  T11: WeightedFairPolicy                                  (T9)
  T12: RoundRobinPolicy                                    (T9)
  T13: PolicyFactory                                       (T10,T11,T12)
      │
Phase 4 ─────────────────────────────────────────
  T14: SimplifiedAdaptiveIdle (Online精简版)               (无依赖)
  T15: OnlineScheduler (调度器协程, Online专用)             (T5,T6,T7,T9,T14)
      │
Phase 5 ─────────────────────────────────────────
  T16: Worker 基类                                         (T15)
  T17: OnlineWorker                                        (T16)
  T18: OfflineWorkerGroup (适配 WorkStealingExecutor)       (T3b)
      │
Phase 6 ─────────────────────────────────────────
  T19: OnlineWorkerGroup                                   (T17)
  T20: Runtime 总控                                        (T18,T19)
```

---

## Phase 1: 基础设施与类型

### T1 — 项目骨架搭建

**工时**：4h

**文件**：
```
CMakeLists.txt
cmake/Sanitizers.cmake          # ASAN/TSAN/UBSAN 集成
src/runtime/
tests/unit/CMakeLists.txt
tests/module/CMakeLists.txt
tests/stress/CMakeLists.txt
```

**内容**：
- 目录结构创建
- CMake 构建 (C++20, folly, hwloc, jemalloc)
- CMake 预设: `Debug` / `Release` / `Sanitized` (ASAN+TSAN+UBSAN)
- GTest 测试框架集成
- 编译验证

**单测**：
- `tests/unit/test_build.cpp` — 编译链接验证

**验收**：
- `cmake -B build && cmake --build build` 成功
- `cmake --preset sanitized -B build-san && cmake --build build-san` 成功
- `ctest` 通过

---

### T2 — WorkItem / QueueType / Priority

**工时**：2h

**文件**：
```
src/runtime/types.h
src/runtime/work_item.h
```

**接口**：
```cpp
// types.h
enum class QueueType : uint8_t {
    kLocal,        // 新增: 非原子本地队列
    kAffine,
    kNetIO,
    kDiskIO,
    kEngine,
    kTimer,
    kBackground,
};
enum class Priority : uint8_t { kCritical = 0, kHigh = 1, kMedium = 2, kLow = 3 };

// work_item.h
struct WorkItem {
    using Func = void(*)();
    union { Func func; std::coroutine_handle<> coro; };
    uint8_t tag;          // 0=func, 1=coroutine
    uint32_t trace_id;

    void execute() noexcept;
    static WorkItem make_func(Func f) noexcept;
    static WorkItem make_coro(std::coroutine_handle<> h) noexcept;
};
```

**单测**：
- `tests/unit/test_work_item.cpp` — func/coro tag 正确，execute 双分支覆盖

**验收**：单测通过

---

### T3a — 本地队列组件 (非原子)

**工时**：4h

**文件**：
```
src/runtime/local_queue.h       # 非原子环形缓冲 (worker-only)
src/runtime/batched_spsc_queue.h # 批量 SPSC (IO完成)
```

**接口**：
```cpp
// LocalQueue — 完全非原子，单线程访问
template<typename T, size_t Capacity = 1024>
class LocalQueue {
public:
    bool try_enqueue(T item) noexcept;        // 无任何原子操作
    bool try_dequeue(T& item) noexcept;
    size_t try_dequeue_batch(T* items, size_t max) noexcept;
    size_t size() const noexcept;
    bool empty() const noexcept;
};

// BatchedSPSCQueue — 批量 SPSC
// 生产者侧 collect → flush (1次 release fence per batch)
// 消费者侧 1次 acquire fence per batch
template<typename T, size_t Capacity = 4096>
class BatchedSPSCQueue {
public:
    // 生产者 (IO Poller 线程)
    void push_batch(const T* items, size_t count) noexcept;

    // 消费者 (Worker 线程)
    size_t try_dequeue_batch(T* items, size_t max) noexcept;
    size_t approx_count() const noexcept;
};
```

**单测**：
- `tests/unit/test_local_queue.cpp`
  - 单线程 enqueue/dequeue 顺序
  - 满队列 enqueue 返回 false
  - 批量 dequeue 数量正确
  - 空队列 dequeue 返回 false

- `tests/unit/test_batched_spsc_queue.cpp`
  - 单生产者单消费者，batch 完整性
  - 生产者 flush 空 batch 正确
  - 消费者 dequeue 跨 batch 边界
  - approx_count 近似正确

**并发正确性测试 (TSAN)**：
- `tests/module/test_local_queues_concurrent.cpp`
  - BatchedSPSCQueue: 1 生产者持续 push_batch + 1 消费者持续 dequeue → 无 TSAN 告警，元素不丢失不重复
  - 压力：生产者 4 线程各以不同间隔 flush，消费者 1 线程 → 元素计数正确

**稳定性测试**：
- `tests/stress/test_local_queues_stability.cpp`
  - BatchedSPSCQueue: 持续 push_batch / dequeue 30min，RSS 不增长，元素总数校验正确
  - 1000 次 LocalQueue 创建/销毁 → ASAN 无泄漏

**验收**：单测通过 + TSAN=0 + ASAN=0 + 稳定性通过

---

### T3b — 导入 quant/infra 组件

**工时**：3h

**策略**：将以下源文件复制到 `src/runtime/adapt/`，修改命名空间：
```
src/runtime/adapt/
├── chase_lev_deque.h           # ← quant::infra::ChaseLevDeque (原样复制)
├── affinity_baton.h/cc         # ← quant::infra::AffinityBaton
├── affinity_mutex.h/cc         # ← quant::infra::AffinityMutex
├── affinity_shared_mutex.h/cc  # ← quant::infra::AffinitySharedMutex
├── work_stealing_executor.h/cc # ← quant::infra::WorkStealingExecutor (稍后适配)
├── timer_scheduler.h/cc        # ← quant::infra::TimerScheduler
├── thread_local_stats.h        # ← quant::infra::StatRegistry
├── object_pool.h               # ← quant::infra::ObjectPool
└── memory_pool.h/cc            # ← quant::infra::QuantMemoryResource
```

**单测**（验证导入正确）：
- `tests/unit/test_adapt_import.cpp`
  - 所有头文件可编译
  - ChaseLevDeque push/pop 基本操作
  - AffinityBaton co_await + post 基本操作
  - StatRegistry increment/aggregate

**验收**：单测通过，编译无警告

---

## Phase 2: 队列实现

### T4 — WorkQueue 抽象接口

**工时**：1h

**文件**：
```
src/runtime/work_queue.h
```

**接口**（修正后，增加队列特性标记）：
```cpp
enum class QueueSemantic : uint8_t {
    kLocal,       // 非原子，worker-only 访问
    kSPSC,        // 单生产者单消费者 (IO完成)
    kMPSC,        // 多生产者单消费者 (Engine/Timer)
    kMPMC,        // 多生产者多消费者 (Affine)
};

class WorkQueue {
public:
    virtual ~WorkQueue() = default;
    virtual bool try_dequeue(WorkItem& item) noexcept = 0;
    virtual size_t try_dequeue_batch(WorkItem* items, size_t max) noexcept = 0;
    virtual size_t approx_count() const noexcept = 0;
    virtual QueueType type() const noexcept = 0;
    virtual Priority base_priority() const noexcept = 0;
    virtual QueueSemantic semantic() const noexcept = 0;
    virtual const char* name() const noexcept = 0;
};
```

**验收**：编译通过，无测试代码（抽象接口由具体实现验证）

---

### T5 — LocalWorkQueue

**工时**：2h

**依赖**：T3a (LocalQueue), T4

**文件**：`src/runtime/local_work_queue.h/cc`

**接口**：WorkQueue 实现，底层 LocalQueue（QueueSemantic::kLocal）

**单测**：
- `tests/unit/test_local_work_queue.cpp`
  - type/priority/semantic 返回正确
  - 单线程 enqueue/dequeue
  - 批量 dequeue
  - approx_count 准确

**验收**：单测通过

---

### T6 — BatchedSPSCWorkQueue

**工时**：3h

**依赖**：T3a (BatchedSPSCQueue), T4

**文件**：`src/runtime/batched_spsc_work_queue.h/cc`

**接口**：WorkQueue 实现，底层 BatchedSPSCQueue（QueueSemantic::kSPSC）

**单测**：
- `tests/unit/test_batched_spsc_work_queue.cpp`
  - type/priority/semantic 正确 (kNetIO/kDiskIO)
  - 生产者线程 push_batch → 消费者线程 dequeue_batch
  - 批量边界正确
  - has_pending_notify 通知机制

**并发正确性测试 (TSAN)**：
- `tests/module/test_batched_spsc_concurrent.cpp`
  - 4 生产者线程持续 push_batch（不同 batch size），1 消费者线程 dequeue
  - 累计 100万 元素 → 总数校验，无丢失无重复
  - TSAN = 0

**验收**：单测通过 + TSAN=0

---

### T7 — AffineWorkQueue

**工时**：2h

**依赖**：T4

**文件**：`src/runtime/affine_work_queue.h/cc`

**接口**：WorkQueue 实现，底层 folly::UMPMCQueue（QueueSemantic::kMPMC）
- 支持 `try_dequeue_batch`，减少 worker 侧原子开销

**单测**：
- `tests/unit/test_affine_work_queue.cpp`
  - type/priority/semantic 正确 (kAffine)
  - 多线程 enqueue，单线程 batch dequeue

**并发正确性测试 (TSAN)**：
- `tests/module/test_affine_concurrent.cpp`
  - 8 线程并发 enqueue，1 线程 batch dequeue → 100k 元素不丢不重
  - TSAN = 0

**验收**：单测通过 + TSAN=0

---

## Phase 3: 调度策略

### T8 — QueueSnapshot / ScheduleDecision

**工时**：1h

**文件**：`src/runtime/schedule_types.h`

**单测**：
- `tests/unit/test_schedule_types.cpp` — 默认值、字段赋值

**验收**：编译通过

---

### T9 — SchedulingPolicy 接口

**工时**：1h

**文件**：`src/runtime/scheduling_policy.h`

**单测**：由 T10-T12 覆盖

**验收**：接口编译通过

---

### T10 — StrictPriorityPolicy

**工时**：2h

**文件**：`src/runtime/strict_priority_policy.h/cc`

**算法**：P0→P3 扫描，首个非空队列 → batch_size = min(approx_count, max_batch)；全空 → idle

**单测**：
- `tests/unit/test_strict_priority_policy.cpp`
  - p0胜出/p1降级 → 优先级链正确
  - 全空idle
  - batch_size上限裁剪
  - 注册顺序不影响优先级

**验收**：单测通过

---

### T11 — WeightedFairPolicy

**工时**：3h

**文件**：`src/runtime/weighted_fair_policy.h/cc`

**算法**：virtual_time 最小非空队列 → batch_size → vtime += batch/w

**单测**：
- `tests/unit/test_weighted_fair_policy.cpp`
  - 同等权重均匀分配
  - 权重 3:1 比例约 3:1
  - vtime 单调递增
  - 全空idle

**验收**：单测通过，权重分配偏差 < 10%

---

### T12 — RoundRobinPolicy

**工时**：1h

**文件**：`src/runtime/round_robin_policy.h/cc`

**单测**：
- `tests/unit/test_round_robin_policy.cpp`
  - 轮转顺序 / 跳过空队列 / 全空idle

**验收**：单测通过

---

### T13 — PolicyFactory

**工时**：1h

**文件**：`src/runtime/policy_factory.h/cc`

**单测**：
- `tests/unit/test_policy_factory.cpp`
  - strict_priority / weighted_fair / round_robin 创建
  - 未知名称异常

**验收**：单测通过

---

## Phase 4: 调度器与空闲控制

### T14 — SimplifiedAdaptiveIdle

**工时**：3h

**文件**：`src/runtime/adaptive_idle.h/cc`

**核心功能**：
- 三级退避: SPIN(10μs) → YIELD(50μs) → PARK(∞)
- `enter_idle()` — 入口，自动升级
- `notify()` — 远程唤醒 (futex_wake)
- `notify_fd()` — eventfd (用于 epoll 集成)
- 丢失唤醒防护

**单测**：
- `tests/unit/test_adaptive_idle.cpp`
  - fast_recovery 快速返回
  - notify 唤醒 park 状态
  - 退避升级路径
  - 丢失唤醒防护
  - 唤醒后 level 重置

**并发正确性测试 (TSAN)**：
- `tests/module/test_adaptive_idle_concurrent.cpp`
  - 1 线程在 idle 循环，4 线程随机 notify → TSAN=0，每次 notify 都在阈值内唤醒

**稳定性测试**：
- `tests/stress/test_adaptive_idle_stability.cpp`
  - enter_idle/notify 循环 30min，唤醒延迟 P99 < 100μs
  - RSS 不增长

**验收**：单测通过 + TSAN=0 + 稳定性通过

---

### T15 — OnlineScheduler

**工时**：5h

**依赖**：T5,T6,T7 (WorkQueue), T9 (SchedulingPolicy), T14 (AdaptiveIdle)

**文件**：`src/runtime/scheduler.h/cc`

**核心功能**：
- `register_queue()` → 注册输入队列 (Local/BatchedSPSC/Affine)
- `run()` → folly::coro::Task<void> 调度循环
- 快照-决策-批量出队-执行 分离
- 队列快照缓存优化 (Remote 队列 approx_count 缓读)
- `request_stop()` → 优雅退出

**单测**：
- `tests/unit/test_scheduler.cpp`
  - 单队列注册+poll
  - 多队列优先级顺序
  - 批量出队
  - 全空idle
  - stop退出循环
  - 新任务唤醒idle

**模块测试**：
- `tests/module/test_scheduler_integration.cpp`
  - Local(Engine) + BatchedSPSC(NetIO) + Affine(协程恢复) + StrictPriorityPolicy + AdaptiveIdle
  - 完整调度循环验证

**并发正确性测试 (TSAN)**：
- `tests/module/test_scheduler_concurrent.cpp`
  - 4 线程并发向不同队列投递，调度器在 1 线程消费
  - 10万任务 → 总数校验，TSAN=0

**稳定性测试**：
- `tests/stress/test_scheduler_stability.cpp`
  - 持续 60s 满负荷投递+消费，吞吐抖动 < 10%
  - 长稳 30min: 启动→满载→空闲→满载→停止，RSS 不增长
  - idle 时 CPU ≈ 0%

**验收**：单测通过 + TSAN=0 + 稳定性通过

---

## Phase 5: Worker 实现

### T16 — Worker 基类

**工时**：3h

**依赖**：T15 (OnlineScheduler)

**文件**：`src/runtime/worker.h/cc`

**接口**：
```cpp
class Worker {
public:
    struct Config {
        uint32_t cpu_id{0};
        uint32_t numa_node{0};
        SchedulingPolicy::Config policy_cfg;
        AdaptiveIdle::Config idle_cfg;
        size_t max_batch_size{64};
    };

    explicit Worker(Config cfg);
    size_t add_queue(std::unique_ptr<WorkQueue> q);
    void set_policy(std::unique_ptr<SchedulingPolicy> p);
    void start();       // 创建 OS 线程 + 绑定 CPU + 启动调度器
    void stop();
    void join();
    void notify();
    WorkerStats stats() const;
    size_t worker_id() const noexcept;
};
```

**单测**：
- `tests/unit/test_worker.cpp`
  - start/stop 生命周期
  - start 前 add_queue/set_policy
  - notify 唤醒 idle worker
  - stats 累加正确

**稳定性测试**：
- `tests/stress/test_worker_stability.cpp`
  - start/stop 循环 200 次 → ASAN 无泄漏，线程及时回收
  - idle 30min → RSS 不增长

**验收**：单测通过 + ASAN=0 + 稳定性通过

---

### T17 — OnlineWorker

**工时**：3h

**依赖**：T16 (Worker)

**文件**：`src/runtime/online_worker.h/cc`

**核心功能**：
- 继承 Worker
- 构造时自动注册默认队列：Local(Engine) + BatchedSPSC(NetIO) + BatchedSPSC(DiskIO) + Affine(恢复) + MPSC(Timer)
- 默认策略: StrictPriorityPolicy
- 默认空闲: 快速 SPIN 优先
- API: `submit_engine()` / `submit_net_io()` / `submit_disk_io()`

**单测**：
- `tests/unit/test_online_worker.cpp`
  - 默认队列已注册
  - 默认策略 strict_priority
  - submit_engine 正确路由到 Local queue
  - submit_net_io 正确路由到 BatchedSPSC

**模块测试**：
- `tests/module/test_online_worker_integration.cpp`
  - OnlineWorker 完整流程：submit_engine → 调度器消费 → 无丢失

**验收**：单测通过 + TSAN=0

---

### T18 — OfflineWorkerGroup (适配 WorkStealingExecutor)

**工时**：4h

**依赖**：T3b (quant/infra 导入)

**文件**：
```
src/runtime/offline_group.h/cc       # OfflineWorkerGroup
src/runtime/adapt/wse_adapter.h/cc   # WorkStealingExecutor 适配层
```

**核心功能**：
- `OfflineWorkerGroup(Config)` — 构造 N 个 worker
  - 内部持有 `quant::infra::WorkStealingExecutor`
- `start()` / `stop()`
- `submit(WorkItem item)` — 投递到负载最低的 worker
- `submit_to_worker(size_t id, WorkItem item)` — 指定 worker
- `stats()` — 聚合统计
- NUMA 感知：构造时由 WorkStealingExecutor 自动完成
- Worker-stealing: WorkStealingExecutor.worker_loop 内置

**适配层 (wse_adapter.h)**：
```cpp
// 将 storage::WorkItem 转换为 WorkStealingExecutor::WorkItem
// 在 worker 入口设置 current_worker_id (AffinityBaton 需要)
// 桥接 TimerScheduler → V2 环境
```

**单测**：
- `tests/unit/test_offline_group.cpp`
  - start/stop 生命周期
  - submit 任务被消费
  - NUMA peer 分组正确 (验证 WorkStealingExecutor 拓扑)
  - steal 功能可用

**并发正确性测试 (TSAN)**：
- `tests/module/test_offline_group_concurrent.cpp`
  - 4 线程并发 submit，N worker 消费 → 10万任务不丢不重
  - 单 worker 堆积 5000 任务 → 其他 worker 窃取 → 负载均衡
  - TSAN = 0

**稳定性测试**：
- `tests/stress/test_offline_group_stability.cpp`
  - 持续 submit 60s + 间歇空闲 30s，循环 30min
  - 负载分布标准差 < 20% 均值
  - RSS 不增长，worker 利用率报告准确

**验收**：单测通过 + TSAN=0 + 稳定性通过

---

## Phase 6: Worker Group 与 Runtime

### T19 — OnlineWorkerGroup

**工时**：4h

**依赖**：T17 (OnlineWorker)

**文件**：`src/runtime/online_group.h/cc`

**核心功能**：
- `OnlineWorkerGroup(Config)` — 创建 N 个 OnlineWorker
- `start()` / `stop()` — 批量启停
- `route_by_hash(key)` → worker_index — 一致性哈希
- `submit_to(key, item)` — 按 key 路由投递
- `worker(index)` — 获取特定 worker
- `stats()` — 全局聚合

**单测**：
- `tests/unit/test_online_group.cpp`
  - start/stop N worker
  - route_by_hash 确定性
  - 大量 key 分布均匀 (CV < 5%)
  - submit_to 正确投递

**模块测试**：
- `tests/module/test_online_group_integration.cpp`
  - N worker + 多队列 + 跨 worker 协程恢复 (AffinityBaton)
  - 路由投递 + 完整消费

**并发正确性测试 (TSAN)**：
- `tests/module/test_online_group_concurrent.cpp`
  - 8 线程并发 submit_to(不同 key)，N worker 消费
  - 100万任务 → 总数校验，TSAN=0
  - Online/Offline 隔离：Online 任务不流入 Offline

**稳定性测试**：
- `tests/stress/test_online_group_stability.cpp`
  - 持续高负载 60s + idle 30s，循环 30min
  - 吞吐抖动 < 10%
  - RSS 不增长
  - 路由偏差 < 5%

**验收**：单测通过 + TSAN=0 + 稳定性通过

---

### T20 — Runtime 总控

**工时**：3h

**依赖**：T18 (OfflineWorkerGroup), T19 (OnlineWorkerGroup)

**文件**：`src/runtime/runtime.h/cc`

**核心功能**：
- `Runtime(Config)` — 创建 OnlineGroup + OfflineGroup
- `start()` / `stop()` — 启动顺序 Online → Offline；停止逆序
- `online_group()` / `offline_group()`
- 全局统计快照

**单测**：
- `tests/unit/test_runtime.cpp`
  - start/stop 生命周期
  - config 正确下发到子组
  - 默认配置可用

**模块测试**：
- `tests/module/test_runtime_integration.cpp`
  - 完整 Runtime 启动 → 投递 Online + Offline → 停止
  - 跨组隔离验证

**系统级压力/稳定性测试**：
- `tests/stress/test_runtime_full.cpp`

  **场景一：混合负载压力测试 (60s)**
  - Online: 8 worker, 16 线程持续 submit_to(随机key)
  - Offline: 4 worker, 4 线程持续 submit
  - 总计 100万+ 任务
  - 验证：不丢失、不重复、吞吐稳定

  **场景二：长稳运行测试 (30min)**
  - 启动 → 满载 5min → 空闲 5min → 满载 5min → 空闲 5min → 满载 5min → 停止
  - 验证：RSS ≤ 初始 + 50MB, CPU idle时 ≈ 0%
  - 验证：stop 后所有线程在 5s 内退出

  **场景三：异常路径测试**
  - stop 进行中时 submit → 不崩溃、不阻塞
  - stop 后 join 不超时 (5s)
  - 空配置创建 Runtime → 优雅处理
  - 重复 start → 幂等或报错

  **场景四：资源回收测试**
  - start → stop 循环 100 次
  - 每次验证：线程/协程/fd 全部回收
  - ASAN = 0

**验收**：
- 单测通过
- 模块测试通过
- 四个场景全部通过
- ASAN + TSAN + UBSAN = 0
- 压力测试稳态指标达标

---

## 总工作量

| Phase | 任务 | 工时 |
|-------|------|------|
| 1 | T1(4) T2(2) T3a(4) T3b(3) | 13h |
| 2 | T4(1) T5(2) T6(3) T7(2) | 8h |
| 3 | T8(1) T9(1) T10(2) T11(3) T12(1) T13(1) | 9h |
| 4 | T14(3) T15(5) | 8h |
| 5 | T16(3) T17(3) T18(4) | 10h |
| 6 | T19(4) T20(3) | 7h |
| **总计** | **20 任务** | **55h** |

---

## 关键路径

```
T1 → T2 → T4 ─┐
T3a ───────────┤
               ├→ T5/T6/T7 ─┐
               │             ├→ T15 → T16 → T17 → T19 ─┐
T8 → T9 ───────┤             │                          ├→ T20
               ├→ T10/T11/T12→ T13                      │
                                    ↑                   │
T14 ─────────────────────────────────┤                  │
                                                         │
T3b → T18 ──────────────────────────────────────────────┘
```

**关键路径**: T1 → T3a → T6 → T15 → T16 → T17 → T19 → T20

---

## 执行说明

1. Phase 内多任务可并行（不同文件无冲突）
2. 每个 Phase 提交前跑完整测试套件（单测 + 模块 + 压力）
3. **每完成 3 个开发任务 → 触发架构师 Review**
4. Review 通过 → 代码 commit + push
5. Review 发现 P0/P1 问题 → 修复后重新 Review
6. Phase 6 完成后 → 全系统 San 通过 → 最终 Review + push

## 测试文件索引

```
tests/
├── unit/
│   ├── test_build.cpp
│   ├── test_work_item.cpp
│   ├── test_local_queue.cpp
│   ├── test_batched_spsc_queue.cpp
│   ├── test_adapt_import.cpp
│   ├── test_local_work_queue.cpp
│   ├── test_batched_spsc_work_queue.cpp
│   ├── test_affine_work_queue.cpp
│   ├── test_schedule_types.cpp
│   ├── test_strict_priority_policy.cpp
│   ├── test_weighted_fair_policy.cpp
│   ├── test_round_robin_policy.cpp
│   ├── test_policy_factory.cpp
│   ├── test_adaptive_idle.cpp
│   ├── test_scheduler.cpp
│   ├── test_worker.cpp
│   ├── test_online_worker.cpp
│   ├── test_offline_group.cpp
│   ├── test_online_group.cpp
│   └── test_runtime.cpp
├── module/
│   ├── test_local_queues_concurrent.cpp
│   ├── test_batched_spsc_concurrent.cpp
│   ├── test_affine_concurrent.cpp
│   ├── test_adaptive_idle_concurrent.cpp
│   ├── test_scheduler_integration.cpp
│   ├── test_scheduler_concurrent.cpp
│   ├── test_online_worker_integration.cpp
│   ├── test_offline_group_concurrent.cpp
│   ├── test_online_group_integration.cpp
│   ├── test_online_group_concurrent.cpp
│   └── test_runtime_integration.cpp
└── stress/
    ├── test_local_queues_stability.cpp
    ├── test_adaptive_idle_stability.cpp
    ├── test_scheduler_stability.cpp
    ├── test_worker_stability.cpp
    ├── test_offline_group_stability.cpp
    ├── test_online_group_stability.cpp
    └── test_runtime_full.cpp
```
