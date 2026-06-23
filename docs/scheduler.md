# Scheduler — 单 Worker 调度引擎设计

## 1. Scheduler（scheduler.h / scheduler.cc）

### 1.1 概述

`Scheduler` 是单 Worker 架构中驱动所有协程执行的引擎。它管理一组 `WorkQueue`（每个队列有独立优先级），通过调度策略决定从哪个队列取任务执行。

```cpp
// src/runtime/scheduler.h 第 26-81 行
class Scheduler {
public:
    Scheduler(SchedulingPolicy* policy, AdaptiveIdle* idle);
    size_t register_queue(std::unique_ptr<WorkQueue> queue);
    void run();
    void run_busy();
    void request_stop();
    // ...
};
```

### 1.2 两种运行模式

#### `run()` — 策略驱动（默认）

`run()` 是默认调度循环，适用于没有 IO 后端或不需要持续轮询的场景。每轮迭代流程：

```
┌─────────────────────────────────────────────────┐
│            Scheduler::run() 迭代                  │
├─────────────────────────────────────────────────┤
│ ① drain_p0()        ─── P0: affine 队列排空      │
│ ② 快照队列状态       ─── 收集各队列 approx_count   │
│ ③ policy_->decide() ─── 策略决策                  │
│ ④ idle?             ─── 进入自适应休眠或跳过       │
│ ⑤ dequeue + execute ─── 执行选中的队列任务         │
│ ⑥ drain_p0()        ─── 再次排空 P0 (任务可能产生产) │
└─────────────────────────────────────────────────┘
```

```cpp
// src/runtime/scheduler.cc 第 73-158 行
void Scheduler::run() {
    if (busy_poll_) { run_busy(); return; }  // IO 后端存在时走 busy poll
    // ...
    while (running_.load(std::memory_order_acquire)) {
        drain_p0(batch, kMaxBatchSize);                         // P0
        // 快照 + 策略决策
        auto decision = policy_->decide(snapshots, stats_);
        if (decision.idle) { /* adaptive idle */ continue; }
        // dequeue 并执行
        // 再次 drain_p0
    }
}
```

特点：
- **无 IO 后端时**：空闲时通过 `AdaptiveIdle` 进入三级回退（spin → yield → cv_wait）
- **有 IO 后端时**：自动切换到 `run_busy()`（`busy_poll_` 标志在 `set_io_backend()` 时设置）
- **策略驱动**：`SchedulingPolicy::decide()` 根据队列快照做出选择，支持 `StrictPriority`、`RoundRobin` 等

#### `run_busy()` — 无条件轮转

当 `IIOBackend` 被附加时启用（通过 `set_io_backend()` 自动设置 `busy_poll_ = true`）：

```
┌────────────────────────────────────────────────────┐
│               Scheduler::run_busy() 迭代             │
├────────────────────────────────────────────────────┤
│ ① drain_p0()              ─── P0: affine 队列排空   │
│ ② drain disk_io (P1)      ─── IO 收割协程            │
│ ③ drain engine (P2)       ─── 引擎业务               │
│ ④ drain net_io (P1)       ─── 网络 IO               │
│ ⑤ drain timer (P1)        ─── 定时器                 │
│ ⑥ drain_p0()              ─── 再次排空 P0            │
└────────────────────────────────────────────────────┘
```

```cpp
// src/runtime/scheduler.cc 第 166-253 行
void Scheduler::run_busy() {
    while (running_.load(std::memory_order_acquire)) {
        drain_p0(p0_batch, kMaxBatchSize);                 // P0
        // P1: disk IO
        if (disk_io_idx_ < queues_.size()) {
            queues_[disk_io_idx_]->try_dequeue_batch(...);
            for (auto& item : io_batch) item.execute();
        }
        // P2 及以下: engine, net_io, timer (跳过 affine 和 disk_io)
        for (size_t qi = 0; qi < queues_.size(); ++qi) {
            if (qi == affine_idx_ || qi == disk_io_idx_) continue;
            // dequeue + execute
        }
        drain_p0(p0_batch, kMaxBatchSize);                 // 二次 P0
    }
}
```

特点：
- **不进入空闲**：持续轮询所有队列，确保 IO 完成事件被及时处理
- **P0 无条件排空两次**：开始前和结束后，保证 affine 协程（如 baton.post 唤醒的）得到最高优先级
- **目标**：防止 P1（IO）/ P2（engine）在 IO 后端活跃时被饿死

### 1.3 优先级策略 — P0 → P1 → P2

`OnlineWorker` 创建 5 个队列，优先级从高到低：

```
P0 ── affine_queue    (Priority::kCritical, QueueType::kAffine)
  │    用途：AffinityBaton.post() 唤醒的协程
  │    特点：每轮迭代最先排空
  │
P1 ── disk_io_queue   (Priority::kHigh, QueueType::kDiskIO)
  │    用途：IO 收割协程（轮询 CQE、触发回调）
  │
P1 ── timer_queue     (Priority::kHigh, QueueType::kTimer)
  │    用途：定时器到期处理协程
  │
P2 ── engine_queue    (Priority::kMedium, QueueType::kEngine)
  │    用途：引擎业务协程
  │
P1 ── net_io_queue    (Priority::kHigh, QueueType::kNetIO)
  │    用途：网络 IO 完成处理
```

```cpp
// src/runtime/online_worker.cc 第 88-100 行
idx_affine_ = add_queue(std::make_unique<RingWorkQueue>(
    QueueType::kAffine, Priority::kCritical, "affine", 65536));
idx_net_io_ = add_queue(std::make_unique<BatchedSPSCWorkQueue>(
    QueueType::kNetIO, Priority::kHigh, "net_io"));
idx_disk_io_ = add_queue(std::make_unique<BatchedSPSCWorkQueue>(
    QueueType::kDiskIO, Priority::kHigh, "disk_io"));
idx_engine_ = add_queue(std::make_unique<RingWorkQueue>(
    QueueType::kEngine, Priority::kMedium, "engine", 65536));
idx_timer_ = add_queue(std::make_unique<AffineWorkQueue>(
    QueueType::kTimer, Priority::kHigh, "timer"));
```

### 1.4 `drain_p0()` — 无条件排空 affine 队列

`drain_p0` 是 `run()` 和 `run_busy()` 中共用的核心方法。它以最大批处理量（64）持续从 affine 队列 dequeue，直到队列为空。

```cpp
// src/runtime/scheduler.cc 第 42-71 行
void Scheduler::drain_p0(std::vector<WorkItem>& batch, size_t max_batch) {
    if (affine_idx_ >= queues_.size()) return;
    auto& q = queues_[affine_idx_];
    auto lv = perf_ ? perf_->level() : PerfLevel::kNone;
    while (true) {
        size_t n = q->try_dequeue_batch(batch.data(), max_batch);
        if (n == 0) break;
        for (size_t i = 0; i < n; ++i) {
            tls_source_queue_idx = affine_idx_;     // yield() 回到本队列
            tls_source_queue = q.get();
            batch[i].execute();
            stats_.total_tasks_executed++;
        }
    }
}
```

关键要点：
- **每次 execute 前**设置 `tls_source_queue_idx` 和 `tls_source_queue`，使协程内部的 `co_await yield()` 能正确回到 affine 队列
- **PerfLevel 门控**：`kNone` 时跳过计时，达到 `kTrace` 时记录每任务的执行耗时到 `WorkerPerf`
- **非阻塞**：队列为空立即返回，不会自旋等待

---

## 2. IO 系统

### 2.1 IIOBackend 接口（io_backend.h / io_backend.cc）

`IIOBackend` 是 IO 后端的抽象基类，定义了 submit / poll 模型：

```cpp
// src/io/io_backend.h 第 17-44 行
class IIOBackend {
public:
    virtual ~IIOBackend() = default;
    virtual std::string_view name() const noexcept = 0;
    virtual void submit(IORequest req) = 0;
    virtual void submit_batch(std::vector<IORequest> requests);
    virtual void flush_submissions() {}
    virtual size_t poll(IOCompletion* out, size_t max) = 0;

    // 协程友好的 IO API
    Task<IOCompletion> co_read(int fd, uint64_t offset, void* buf, size_t len);
    Task<IOCompletion> co_write(int fd, uint64_t offset, const void* buf, size_t len);
};
```

### 2.2 co_read / co_write — 零分配协程 IO

`co_read` 和 `co_write` 是协程友好的 IO 接口。它们使用嵌入在协程帧上的 `IOState`（零堆分配），通过 `AffinityBaton` 实现挂起/恢复：

```cpp
// src/io/io_backend.cc 第 9-22 行
struct IOState {
    runtime::adapt::AffinityBaton baton;
    IOCompletion result;
};

void io_rw_complete(void* ctx, IOCompletion comp) {
    auto* state = static_cast<IOState*>(ctx);
    state->result = comp;
    state->baton.post();
}
```

`co_read` 实现：

```cpp
// src/io/io_backend.cc 第 24-46 行
Task<IOCompletion> IIOBackend::co_read(
    int fd, uint64_t offset, void* buf, size_t len) {
    IOState state;              // 在协程帧上分配
    IORequest req;
    req.op = IORequest::kRead;
    req.fd = fd;
    req.offset = offset;
    req.buf = buf;
    req.len = len;
    req.callback_fn = io_rw_complete;
    req.callback_ctx = &state;

    this->submit(std::move(req));  // 提交 IO 请求
    co_await state.baton;          // 挂起, 等待 IO 完成
    co_return state.result;
}
```

**零分配的关键**：`IOState` 是局部变量，存储在协程帧上。协程帧在 `initial_suspend` 时由运行时分配（通常在栈上或内存池中）。`co_await state.baton` 将协程挂起，IO 完成后 `io_rw_complete` 回调调用 `baton.post()` 将协程重新入队到 affine 队列恢复执行。

### 2.3 IOUringBackend（io_uring_backend.h / io_uring_backend.cc）

`IOUringBackend` 是 `IIOBackend` 的 io_uring 实现，支持 SQPOLL、SINGLE_ISSUER、共享 WorkQueue 组等高级特性。

#### SQE 填充

`submit_impl` 负责填充 SQE 并管理 `pending_` 数组（通过 `submit_count_` 索引映射）：

```cpp
// src/io/io_uring_backend.cc 第 99-170 行
void IOUringBackend::submit_impl(IORequest req) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
        // SQ 满 → drain CQE 循环
        // ...
    }
    // 填充 SQE
    uint64_t idx = submit_count_++;
    pending_[idx] = std::move(req);
    io_uring_prep_read(sqe, ...);  // 或 io_uring_prep_write
    io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(idx));
}
```

#### CQE 轮询

`poll()` 非阻塞收割完成事件，内联调用回调函数：

```cpp
// src/io/io_uring_backend.cc 第 207-239 行
size_t IOUringBackend::poll(IOCompletion* out, size_t max) {
    size_t count = 0;
    struct io_uring_cqe* cqe = nullptr;
    while (count < max) {
        int ret = io_uring_peek_cqe(&ring_, &cqe);
        if (ret == -EAGAIN) break;
        uint64_t idx = reinterpret_cast<uint64_t>(io_uring_cqe_get_data(cqe));
        out[count].result = cqe->res;
        if (pending_[idx].callback_fn) {
            pending_[idx].callback_fn(pending_[idx].callback_ctx, out[count]);
        }
        io_uring_cqe_seen(&ring_, cqe);
        count++;
    }
    return count;
}
```

#### `flush_submissions`

将待处理的 SQE 批量提交到内核：

```cpp
// src/io/io_uring_backend.cc 第 188-193 行
void IOUringBackend::flush_submissions() {
    if (pending_sqe_count_ > 0) {
        io_uring_submit(&ring_);
        pending_sqe_count_ = 0;
    }
}
```

#### `submit_impl` ENOBUFS 修复

**问题**：当 SQ（Submission Queue）环形缓冲区满时，`io_uring_get_sqe()` 返回 `nullptr`。在 SQPOLL 模式下，io_uring 内核线程异步消费 SQE，因此不能简单地等待——必须主动 drain CQE 释放 SQ 槽位。

**修复方案**（`src/io/io_uring_backend.cc` 第 101-143 行）：

```cpp
void IOUringBackend::submit_impl(IORequest req) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
        // 先 submit 已有 SQE
        io_uring_submit(&ring_);
        pending_sqe_count_ = 0;

        // 循环 drain CQE: 最多尝试 queue_depth*2 次
        for (int drain_attempts = 0;
             drain_attempts < (int)(queue_depth_ * 2); ++drain_attempts) {
            struct io_uring_cqe* cqe = nullptr;
            int ret = io_uring_wait_cqe(&ring_, &cqe);
            if (ret == 0 && cqe) {
                // 处理 CQE：调用回调函数
                uint64_t idx = reinterpret_cast<uint64_t>(io_uring_cqe_get_data(cqe));
                if (idx < pending_.size() && pending_[idx].callback_fn) {
                    IOCompletion comp;
                    comp.result = cqe->res;
                    pending_[idx].callback_fn(pending_[idx].callback_ctx, comp);
                }
                io_uring_cqe_seen(&ring_, cqe);
            }
            sqe = io_uring_get_sqe(&ring_);
            if (sqe) goto got_sqe;  // SQ 槽位已释放
        }

        // 耗尽所有重试仍无可用 SQE → 回调 -ENOBUFS
        if (req.callback_fn) {
            IOCompletion comp;
            comp.result = -ENOBUFS;
            req.callback_fn(req.callback_ctx, comp);
        }
        return;
    }
got_sqe:
    // 填充 SQE ...
}
```

**修复要点**：
1. `io_uring_submit()` 将已填充的 SQE 刷入内核，为后续 `io_uring_get_sqe()` 腾出空间
2. `io_uring_wait_cqe()` 等待并处理 CQE，每处理一个 CQE 就释放一个 SQ 槽位
3. `queue_depth_ * 2` 的上界保证：即使 SQPOLL 线程处理滞后，所有提交的 SQE 最终都会被消费
4. 耗尽重试仍然失败时，通过回调传递 `-ENOBUFS` 错误，不会丢失请求

---

## 3. Timer 系统

### 3.1 `timer_coro_fn` — 持久定时器协程

定时器系统是一个永不退出的持久协程，在 `on_worker_start()` 时启动：

```cpp
// src/runtime/online_worker.cc 第 48-80 行
static adapt::Task<void> timer_coro_fn(OnlineWorker* w) {
    while (true) {
        auto& ts = w->timer_state();
        auto now = Clock::now();
        auto expired = ts.expire(now);

        if (!expired.empty()) {
            for (auto& node : expired) {
                if (node.on_expire) {
                    node.on_expire();
                } else {
                    size_t qidx = (node.source_queue_idx != SIZE_MAX)
                        ? node.source_queue_idx : w->idx_engine_;
                    auto* q = w->get_queue(qidx);
                    if (q) q->enqueue(WorkItem::make_coro(node.handle));
                }
            }
            co_await yield_to(w->idx_timer_);  // 回到 timer 队列, 立即检查下一轮
        } else {
            co_await yield();  // 无过期定时器 → 回到 engine(P2) 队列
        }
    }
}
```

### 3.2 无过期定时器时的 yield 策略 — 防止 P2 饿死

当没有定时器到期时，`timer_coro_fn` 使用 `co_await yield()`（而非 `co_await yield_to(timer_queue)`）。

**为什么重要**：在 `StrictPriority` 策略下，如果一个队列持续非空，调度器永远不会轮询较低优先级的队列。`timer_coro_fn` 是一个持久协程——它执行完一轮后会重新入队。如果它始终入队到 `timer_queue`（P1），则 P1 永远不会变空，P2（engine）和 P3 将永久饿死。

**解决方案**：
- 无过期定时器 → `co_await yield()`，利用 `yield()` 的路由规则：当 `tls_source_queue_idx` 未设置时回退到 `current_worker()->default_queue_idx()`（engine 队列 / P2）
- 有到期定时器 → `co_await yield_to(w->idx_timer_)`，保持在 P1 快速检查后续定时器

### 3.3 生命周期

```cpp
// src/runtime/online_worker.cc 第 103-108 行
auto timer_coro = timer_coro_fn(this);
timer_handle_ = timer_coro.release();
// 注意: 在 on_worker_start() 中才 resume，确保 TLS 已就绪
```

```cpp
// src/runtime/online_worker.cc 第 111 行起
void OnlineWorker::on_worker_start() {
    // Worker TLS 已就绪 (tls_worker_id, tls_current_worker)
    // 设置 tls_source_queue_idx 后 resume 持久协程
    tls_source_queue_idx = idx_timer_;
    timer_handle_.resume();
}
```

---

## 4. 统计框架

### 4.1 MetricCounter — 原子计数器

`MetricCounter` 是一个简单的单调递增原子计数器，提供 `<<` 运算符进行累加：

```cpp
// src/runtime/metric_counter.h 第 15-31 行
class MetricCounter {
public:
    void operator<<(int64_t delta) {
        value_.fetch_add(delta, std::memory_order_relaxed);
    }
    int64_t value() const noexcept {
        return value_.load(std::memory_order_relaxed);
    }
    void reset() noexcept { value_.store(0); }
private:
    std::atomic<int64_t> value_{0};
};
```

配套的类型：
- **MetricGauge**：可读写的瞬时值（set / value）
- **MetricPeak**：峰值追踪（仅单调递增方向 CAS）
- **MetricRegistry**：全局变量注册表，支持 JSON 和 Prometheus 导出

### 4.2 Scheduler 指标

在 `scheduler.cc` 中注册了 5 个全局计数器：

```cpp
// src/runtime/scheduler.cc 第 8-14 行
namespace {
    metric::MetricCounter g_sched_iters;
    metric::MetricCounter g_drain_p0_ns;
    metric::MetricCounter g_drain_all_ns;
    metric::MetricCounter g_io_poll_ns;
    metric::MetricCounter g_drain_p0b_ns;
}
```

这些计数器在 `run_busy()` 循环中填充（单位为 rdtsc 周期，通过 `/3` 粗略转换为 ns）：

```cpp
// src/runtime/scheduler.cc 第 243-248 行
g_sched_iters << 1;
g_drain_p0_ns  << ((t1 - t_iter) / 3);   // P0 排空耗时
g_drain_all_ns << ((t2 - t1) / 3);        // P1+P2 排空耗时
g_io_poll_ns   << ((t2 - t1) / 3);        // 同上, 向后兼容
g_drain_p0b_ns << ((t3 - t2) / 3);        // 二次 P0 排空耗时
```

在 `register_scheduler_metrics()` 中注册到全局 `MetricRegistry`：

```cpp
// src/runtime/scheduler.cc 第 255-262 行
void register_scheduler_metrics() {
    MetricRegistry::instance().register_counter("scheduler/iters",        &g_sched_iters);
    MetricRegistry::instance().register_counter("scheduler/drain_p0_ns",  &g_drain_p0_ns);
    MetricRegistry::instance().register_counter("scheduler/drain_all_ns", &g_drain_all_ns);
    MetricRegistry::instance().register_counter("scheduler/io_poll_ns",   &g_io_poll_ns);
    MetricRegistry::instance().register_counter("scheduler/drain_p0b_ns", &g_drain_p0b_ns);
}
```

### 4.3 PerfLevel 门控

`PerfLevel` 定义了三级性能监控，用于在统计开销和详细程度之间做 trade-off：

```cpp
// src/runtime/worker_perf.h 第 11-15 行
enum class PerfLevel : uint8_t {
    kNone  = 0,   // 完全禁用：零开销，不记录任何统计
    kCount = 1,   // 仅计数：记录执行次数和总耗时 (total_exec_ns)
    kTrace = 2,   // 完整追踪：额外记录直方图、入队时间戳、P50/P99
};
```

**使用场景**：

| PerfLevel | vDSO 开销 | WorkerPerf 记录 | 适用场景 |
|-----------|-----------|----------------|---------|
| `kNone` | 零 | 无 | 生产环境默认，最高性能 |
| `kCount` | 1 次 `now_ns()` / 任务 | total_exec_ns | 性能基线，轻量监控 |
| `kTrace` | 2+ 次 `now_ns()` / 任务 | wait_hist, exec_hist | Debug，性能分析 |

控制路径：

```cpp
// scheduler.cc drain_p0 中的门控示例
auto lv = perf_ ? perf_->level() : PerfLevel::kNone;
if (lv == PerfLevel::kNone) {
    // Fast path: 无计时，仅计数
    batch[i].execute();
    stats_.total_tasks_executed++;
} else {
    auto t0 = now_ns();
    batch[i].execute();
    stats_.total_exec_ns += now_ns() - t0;
    if (lv >= PerfLevel::kTrace) {
        perf_->record_exec(q->type(), exec_ns);
    }
}
```

### 4.4 Scheduler Probe — rdtsc 级探针

除了全局计数器，Scheduler 还维护了基于 `__builtin_ia32_rdtsc` 的循环级探针：

```cpp
// src/runtime/scheduler.h 第 59-62 行
struct Probe {
    uint64_t io_poll{0};      // IO 轮询耗时 (cycle)
    uint64_t drain_p0{0};     // P0 排空耗时
    uint64_t snapshot{0};     // 快照 + 决策耗时
    uint64_t dequeue_exec{0}; // 出队执行耗时
    uint64_t drain_p0b{0};    // 二次 P0 排空耗时
    uint64_t iter{0};         // 总迭代耗时
};
Probe probe;
size_t probe_count{0};
```

这些探针在 `run()` 和 `run_busy()` 循环中被填充，通过 `probe_count` 可计算均值。在 `run_busy()` 中略有不同（将 IO 和其他队列处理合并为 `io_poll`）：

```cpp
// src/runtime/scheduler.cc 第 238-242 行
probe.drain_p0    += t1 - t_iter;
probe.io_poll     += t2 - t1;       // IO + other queue processing
probe.drain_p0b   += t3 - t2;       // Second P0 drain
probe.iter        += t3 - t_iter;
probe_count++;
```

### 4.5 IO 系统指标

IO 后端也有独立的计数器，在 `io_uring_backend.cc` 中注册：

```cpp
// src/io/io_uring_backend.cc 第 12-16 行
storage::runtime::metric::MetricCounter g_io_submitted;
storage::runtime::metric::MetricCounter g_io_completed;
storage::runtime::metric::MetricLatency g_io_latency;

// 注册 (第 241-246 行)
void register_io_metrics() {
    MetricRegistry::instance().register_counter("io/submitted",  &g_io_submitted);
    MetricRegistry::instance().register_counter("io/completed",  &g_io_completed);
    g_io_latency.register_with("io/latency");
}
```

---

## 5. 关键数据流汇总

```
          ┌──────────────┐
          │  Application  │
          │  协程/回调    │
          └──────┬───────┘
                 │
          co_await / release()
                 │
                 ▼
          ┌──────────────┐
          │  WorkQueue[]  │ ← 5 个优先级队列
          │  (P0..P2)     │
          └──────┬───────┘
                 │
          Scheduler  │
          drain_p0()  │  run() / run_busy()
                 │
                 ▼
          ┌──────────────┐
          │  WorkItem     │ → execute() → coro.resume() / func()
          │  execute()    │
          └──────┬───────┘
                 │
        ┌────────┴────────┐
        ▼                  ▼
  ┌──────────┐      ┌──────────┐
  │ IIOBackend│      │  Timer   │
  │ submit() │      │ expire() │
  │ poll()   │      │          │
  └──────────┘      └──────────┘
        │                  │
        ▼                  ▼
  ┌──────────┐      ┌──────────┐
  │ io_uring  │      │ on_expire│
  │ SQ/CQE   │      │ enqueue  │
  └──────────┘      └──────────┘
```
