# Runtime 层设计修正

> 日期：2026-06-08
> 关联：[04-runtime-layer.md](04-runtime-layer.md) [05-runtime-plan.md](05-runtime-plan.md)

## 修正一：Online 队列原子变量开销优化

### 1.1 问题分析

原设计中 Online Worker 所有队列均使用无锁实现（SPSC/MPSC/MPMC），每条队列的 enqueue/dequeue 操作都涉及原子指令。即便在 shared-nothing 模型下：

- **每条原子指令** ≈ `lock cmpxchg` 锁定内存总线 + 跨核心 cache line 失效
- **qps 损耗** — 单次原子操作约 20-50ns，如果每 1μs 处理一个请求中触发 5 次队列操作，约 10-25% CPU 浪费在同步开销上
- **伪共享** — 即使不同队列在不同 cache line，相邻原子变量仍可能竞争总线

关键洞察：在 shared-nothing 模型中，**同一 Worker 内的自我投递不需要同步**。

### 1.2 队列分类重构

```
┌─────────────────── Online Worker ───────────────────────────────┐
│                                                                  │
│   ┌──────────────────────┐    ┌─────────────────────────────┐   │
│   │  Local Queue          │    │  Remote Queues              │   │
│   │  (非原子, worker-only) │    │  (含原子, 但批量平摊)       │   │
│   │                       │    │                             │   │
│   │  引擎自身操作：        │    │  ┌──────────┐ ┌──────────┐ │   │
│   │  - 查询入口           │    │  │ Batched  │ │ Affine   │ │   │
│   │  - 插入/更新          │    │  │ SPSC     │ │ MPMC     │ │   │
│   │  - 内部触发回调       │    │  │ Queue    │ │ Queue    │ │   │
│   │                       │    │  │(IO完成)  │ │(协程恢复)│ │   │
│   │  实现: std::deque     │    │  └──────────┘ └──────────┘ │   │
│   │  or 环形缓冲          │    │                             │   │
│   └──────────────────────┘    └─────────────────────────────┘   │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

| 队列 | 生产者 | 同步方式 | 原子开销 |
|------|--------|---------|---------|
| **LocalQueue** | Worker 自身 | 无（同一线程） | **0** |
| **BatchedSPSCQueue** | IO Poller 线程 | 批量 fence：N 个完成事件 = 1 次 store-release | **≈1/N per item** |
| **AffineMPMCQueue** | 其他 Worker 线程 | 每次 enqueue 1 次 CAS，dequeue 批量无原子 | **1 CAS per enqueue** |

### 1.3 BatchedSPSCQueue 设计

关键思路：生产者（IO Poller）先收集完成事件到本地 batch buffer，达到阈值或超时后，整体写入共享环形缓冲。

```cpp
// 生产者侧 (IO Poller 线程)
class BatchedSPSCProducer {
public:
    void push(const WorkItem& item) {
        local_batch_[batch_count_++] = item;
        if (batch_count_ >= kBatchSize) flush();
    }

    void flush() {
        if (batch_count_ == 0) return;
        // 批量写入共享环形缓冲
        size_t head = ring_head_.load(std::memory_order_relaxed);
        for (size_t i = 0; i < batch_count_; ++i) {
            ring_[head + i] = local_batch_[i];
        }
        // 一次 release 发布整批数据
        ring_head_.store(head + batch_count_, std::memory_order_release);
        batch_count_ = 0;
    }

private:
    WorkItem local_batch_[64];
    size_t batch_count_{0};
    std::atomic<size_t>& ring_head_;  // 共享环形缓冲的 write head
    WorkItem* ring_;
};

// 消费者侧 (Worker 线程)
class BatchedSPSCConsumer {
public:
    size_t dequeue_batch(WorkItem* out, size_t max_count) {
        size_t head = ring_head_.load(std::memory_order_acquire);
        size_t n = std::min(head - tail_, max_count);
        for (size_t i = 0; i < n; ++i) {
            out[i] = ring_[tail_ + i];
        }
        tail_ += n;
        return n;
    }

private:
    std::atomic<size_t>& ring_head_;
    size_t tail_{0};
    const WorkItem* ring_;
};
```

**效果**：IO Poller 每轮 poll 可能返回 32 个完成事件，合为 1 次 store-release，平摊到每个事件的原子开销 ≈ 0.6ns。

### 1.4 AffineMPMCQueue 优化

跨线程协程恢复必须用 MPMC 队列（多生产者），无法消除原子操作。但可以做到：

```
enqueue: 1 次 CAS (不可避免，生产者多)
dequeue: 批量取出，lock-free drain → 1 次 CAS 取一批
```

使用 folly::UMPMCQueue 的 `try_dequeue_batch` 替代逐条 dequeue，减少 worker 侧原子开销。

### 1.5 调度器队列快照优化

原设计每次调度决策前遍历所有队列调用 `approx_count()`。对于 Remote 队列（涉及原子读），改为缓存上次轮询结果，仅在以下情况下更新：

- 上次 dequeue 后 `approx_count` 变为 0 时
- 间隔 > 一定周期（如 100μs）时

减少跨 cache line 读取频率。

---

## 修正二：Offline 组直接复用 quant/infra

### 2.1 可复用组件评估

quant/infra 项目的组件与我们的需求高度吻合：

| quant/infra 组件 | 对应我们的需求 | 复用度 | 说明 |
|-----------------|--------------|--------|------|
| **WorkStealingExecutor** | OfflineWorkerGroup 核心 | **100%** | worker-stealing 循环 + NUMA 感知 + 自适应退避 + 统计 |
| **ChaseLevDeque** | 可窃取双端队列 | **100%** | 直接使用，无需重写 |
| **AffinityBaton** | 协程信号量 (全局) | **100%** | Online + Offline 都需要 |
| **AffinityMutex** | 协程互斥锁 (全局) | **100%** | 同上 |
| **AffinitySharedMutex** | 协程读写锁 (全局) | **100%** | 同上 |
| **TimerScheduler** | 定时器调度 (全局) | **90%** | 需适配，去掉 quant 业务逻辑 |
| **ObjectPool** | 对象池 (全局) | **100%** | 直接使用 |
| **QuantMemoryResource** | 内存池 (全局) | **100%** | 直接使用 |
| **StatRegistry** | 线程本地统计 (全局) | **80%** | 直接复用，改指标名 |
| **etcd_client** | — | **0%** | 不需要，我们有一致性层 |
| **strategy_watcher** | — | **0%** | 不需要 |

### 2.2 复用策略

```
┌─────────────────────────────────────────────────┐
│              storage-engine                       │
│                                                   │
│  src/runtime/                                     │
│  ├── local_queue.h         # 非原子本地队列 (新)   │
│  ├── batched_spsc_queue.h  # 批量 SPSC (新)        │
│  ├── affine_queue.h        # Affine MPMC (新封装)  │
│  ├── work_queue.h          # WorkQueue 抽象 (新)   │
│  ├── scheduling_policy.h   # 调度策略 (新)         │
│  ├── scheduler.h/cc        # Online 调度器 (新)     │
│  ├── online_worker.h/cc    # Online Worker (新)    │
│  ├── online_group.h/cc     # Online Group (新)     │
│  ├── runtime.h/cc          # Runtime 总控 (新)     │
│  │                                                 │
│  └── adapt/                # quant/infra 复用适配  │
│      ├── chase_lev_deque.h     # ← 直接复制        │
│      ├── affinity_baton.h/cc   # ← 直接复制        │
│      ├── affinity_mutex.h/cc   # ← 直接复制        │
│      ├── affinity_shared_mutex.h/cc  # ← 直接复制  │
│      ├── work_stealing_executor.h/cc # ← 适配封装  │
│      ├── timer_scheduler.h/cc  # ← 适配            │
│      ├── object_pool.h         # ← 直接复制        │
│      └── memory_pool.h/cc      # ← 直接复制        │
│                                                   │
│  OfflineWorkerGroup 内部持有一个                   │
│  WorkStealingExecutor，将其 add_to_worker /        │
│  worker_loop / steal 机制直接用于后台任务调度        │
└─────────────────────────────────────────────────┘
```

### 2.3 OfflineWorkerGroup 实现策略

```cpp
class OfflineWorkerGroup {
public:
    // 构造：创建 WorkStealingExecutor + 包装
    OfflineWorkerGroup(const Config& cfg);

    void start();
    void stop();

    // 投递后台任务
    void submit(WorkItem item);
    void submit_to_worker(size_t worker_id, WorkItem item);

    // 查询
    size_t worker_count() const;
    Stats stats() const;

private:
    // 核心复用：quant::infra::WorkStealingExecutor
    std::unique_ptr<WorkStealingExecutor> executor_;

    // 适配层：将我们的 WorkItem 映射到 WorkStealingExecutor 的任务
    // 在线程局部存储中设置 get_current_worker_id
};
```

### 2.4 复用带来的计划修正

原计划中的这些任务可以简化：

| 原任务 | 修正 |
|--------|------|
| T3: 无锁队列基础组件 (SPSC/MPSC/MPMC) | **保留** — 仅实现 LocalQueue (非原子) + BatchedSPSCQueue + 精简 MPMC 封装。ChaseLevDeque 从 quant/infra 直接复制 |
| T14: AdaptiveIdle | **大幅简化** — Online 使用精简版 (SPIN/YIELD/PARK)，Offline 复用 WorkStealingExecutor 内置退避 |
| T15: Scheduler | **仅 Online** — Offline 的调度复用 WorkStealingExecutor.worker_loop |
| T17: ThreadLocalStats | **直接复用** quant::infra::StatRegistry |
| T19: OfflineWorker + ChaseLevDeque | **替换为**适配封装 WorkStealingExecutor |

### 2.5 代码依赖

```
storage-engine
├── 依赖 folly (现有)
├── 依赖 hwloc (CPU 绑定)
└── 依赖 quant/infra (源文件导入)
    ├── chase_lev_deque.h
    ├── affinity_baton.h/cc
    ├── affinity_mutex.h/cc
    ├── affinity_shared_mutex.h/cc
    ├── work_stealing_executor.h/cc
    ├── timer_scheduler.h/cc
    ├── thread_local_stats.h
    ├── object_pool.h
    └── memory_pool.h/cc
```

导入方式：通过 CMake 的 `add_subdirectory` 或直接将源文件复制到 `src/runtime/adapt/`，修改命名空间前缀为项目命名空间。

---

## 修订后的任务规划

| Phase | 任务 | 工时变化 | 说明 |
|-------|------|---------|------|
| 1 | T1 项目骨架 | 4h→4h | 不变 |
| 1 | T2 WorkItem/类型 | 2h→2h | 不变 |
| 1 | T3a 本地队列(LocalQueue + BatchedSPSC) | 新增 4h | 非原子实现 |
| 1 | T3b 导入 quant/infra 组件 | 新增 3h | 复制+适配命名空间 |
| 2 | T4 WorkQueue 抽象 | 2h→2h | 不变，增加 Local/Batched/Remote 子类 |
| 2 | T5 LocalWorkQueue | 2h→2h | 新，包装非原子 LocalQueue |
| 2 | T6 BatchedSPSCWorkQueue | 3h→3h | 新，包装 BatchedSPSCQueue |
| 2 | T7 AffineWorkQueue | 2h→2h | 新，包装 MPMC for Affine |
| 3 | T8-T13 调度策略 | 9h→9h | 不变 |
| 4 | T14 SimplifiedAdaptiveIdle | 4h→3h | Online 精简版，Offline 复用内置退避 |
| 4 | T15 OnlineScheduler | 6h→5h | 仅 Online 调度器 |
| 5 | T16 Worker 基类 | 4h→3h | 简化，Offline 侧由 WorkStealingExecutor 承担 |
| 5 | T17 ThreadLocalStats | 2h→0h | **删除**，直接复用 StatRegistry |
| 5 | T18 OnlineWorker | 3h→3h | 不变 |
| 5 | T19 Offline adapt WorkStealingExecutor | 5h→4h | 适配封装，非重新实现 |
| 6 | T20 OnlineWorkerGroup | 4h→4h | 不变 |
| 6 | T21 OfflineWorkerGroup | 4h→3h | 大幅简化，内部持 WorkStealingExecutor |
| 6 | T22 Runtime 总控 | 3h→3h | 不变 |
| **总计** | **22→21 任务** | **67h→54h** | 减少 13h (19%) |
