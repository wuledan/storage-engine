# io_uring 优化方案 & 定时器设计

## 一、io_uring 优化评估

当前状态：SQPOLL + idle=0，flush=66ns，已消除 io_uring_enter 提交开销。

### 1. IOPOLL — 内核轮询设备完成

**原理**：`IORING_SETUP_IOPOLL`。内核不依赖中断，直接轮询 NVMe 完成队列（PCIe BAR 读取），CQE 写入消除中断→softirq 延迟。

**路径对比**：
```
当前:   IO完成 → 中断 → softirq → io_cqring_ev_posted → CQE 可见
IOPOLL:  IO完成 → 内核紧循环轮询 CQ → CQE 立即可见
```

**预期收益**：
- QD=1: P50 降低 2-5μs（消除中断延迟）
- 高 QD: 消除中断风暴，CPU 效率提升
- 配合 SQPOLL idle=0: 提交和完成均为 polling，全路径零中断

**限制**：
- 需要 `O_DIRECT` 文件（我们已有）
- 设备必须支持 NVMe polling（P41 Plus 应支持）
- **IOPOLL 不能与 SQPOLL 同时使用**（内核文档：IOPOLL requires the application to reap completions via io_uring_enter）
  - 实际：IOPOLL + SQPOLL 的组合在某些内核上可行但未官方支持
  - 安全方案：IOPOLL 单独使用，P1 协程调用 `io_uring_enter(0, 0, IORING_ENTER_GETEVENTS)` 收割
- 盘独占：bind 到 vfio-pci 后内核不可见

**实施复杂度**：中。需改 ring 初始化、P1 poll 路径，且需要单独的环境（nvme0 当前内核在用，nvme1 预留给 SPDK）。

**优先级**：高。理论收益最大，与我们的 polling 模型最吻合。

### 2. Registered Buffers — 预注册 IO 缓冲区

**原理**：`io_uring_register_buffers(ring, iovecs, nr)`。内核预 pin 物理页，每 IO 省去 `get_user_pages` → `pin` → `unpin` 路径。SQE 使用 `IOSQE_BUFFER_SELECT` 或固定 buffer index。

**预期收益**：
- QD=1: 微小（单次 pin/unpin ~1μs，在 30μs 总量中占比小）
- QD=128+: 显著，pin/unpin 成为瓶颈（100K+ IOPS 时页表操作 > 10% CPU）
- 搭配 IOPOLL 效果叠加

**限制**：
- 需固定 buffer pool 大小（不可动态增长）
- 每个 IO 的 buf 必须在注册范围内
- 4K 随机写场景 buffer 可复用，适合；大 buffer 场景不适合

**实施复杂度**：低。在 `init_io_backend` 时注册，submit 时传 `buf_index`。

**优先级**：中。QD=1 收益小，高 QD 时配合其他优化使用。

### 3. Registered Files — 预注册文件描述符

**原理**：`io_uring_register_files(ring, fds, nr)`。将 fd 数组注册到内核，SQE 使用 `IOSQE_FIXED_FILE` + `fd_index` 替代 `fd`。消除每 IO 的 `fdget` → `file` 查找。

**预期收益**：
- QD=1: 极小（fd 查找 ~100ns）
- 多文件场景：显著（每个文件多一次查找）

**限制**：
- 需要预知所有使用到的 fd
- 单文件 benchmark 无收益

**实施复杂度**：低。预注册 fd，submit 时加 SQE flag。

**优先级**：低。当前单文件场景无收益。

### 4. 优化路线

```
Phase 1: IOPOLL — 延迟优化，QD=1 收益最大
Phase 2: Registered Buffers — 高 QD 吞吐优化
Phase 3: Registered Files — 多文件场景
```

---

## 二、定时器设计

### 需求

1. `co_await co_sleep(duration)` — 协程挂起指定时间
2. `co_await co_sleep_until(deadline)` — 挂起到绝对时间
3. 定时器到期后协程恢复到**原优先级队列**
4. Online: 每 worker 一个 timer 协程 (P4 队列)
5. Offline: 全局共享 timer

### Online Timer — 每 Worker 独立

**模型**：与 IOCoro 完全相同的挂起/恢复模式。

```
TimerCoro (P4, timer 队列):
  while(true) {
      now = steady_clock::now()
      for each expired timer:
          获取对应协程 handle
          协程入队到其原优先级队列(从 timer_node.source_queue_idx 读取)
      
      next_timeout = 最近到期时间 - now
      if (next_timeout <= 0) continue  // 立即重试
      co_await yield()  // 挂起, 下轮继续
  }
```

**数据结构**：

```cpp
// Timer node — 存入小顶堆
struct TimerNode {
    std::chrono::steady_clock::time_point deadline;
    size_t source_queue_idx;          // 原优先级队列索引
    size_t source_worker_id;          // 原 worker ID (offline)
    std::coroutine_handle<> handle;   // 待恢复协程
    bool operator<(const TimerNode& o) const { return deadline > o.deadline; } // min-heap
};

// Per-worker timer state
struct WorkerTimerState {
    std::priority_queue<TimerNode> pending;  // 小顶堆, 按 deadline 排序
    // 单 worker, 无锁
};
```

**API**：

```cpp
// Online: per-worker sleep
folly::coro::Task<void> co_sleep(std::chrono::nanoseconds duration);
// → 创建 TimerNode{now+duration, tls_source_queue_idx, handle}
// → 入当前 worker 的 pending 堆
// → 挂起, timer 协程到期后恢复

// 实现: Awaiter 在 await_suspend 中:
//   current_online_worker()->timer_state().push(node);
//   node.handle = h;  // 不挂起自己, 让 timer 协程后面恢复我
//   h.resume(); ← 不对, 应该挂起, timer 协程来 resume
```

**关键：谁挂起？谁恢复？**

方案 A（推荐）: await_suspend 中把 handle 存入 timer 堆，**返回 void 挂起**。timer 协程到期后从堆取出 handle，入队到原队列。Scheduler 恢复。

```cpp
struct SleepAwaiter {
    TimerNode node;
    bool await_ready() noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) noexcept {
        node.handle = h;
        auto* w = current_online_worker();
        node.source_queue_idx = tls_source_queue_idx;
        w->timer_state().push(std::move(node));
        // 协程挂起, timer 协程后面负责恢复
    }
    void await_resume() noexcept {}
};

SleepAwaiter co_sleep(nanoseconds dur) {
    return { TimerNode{ now() + dur } };
}
```

**Timer 协程实现**：

```cpp
struct TimerCoroTask { /* same promise_type as IOCoroTask */ };

static TimerCoroTask timer_coro_fn(OnlineWorker* w) {
    while (true) {
        auto& heap = w->timer_state().pending;
        auto now = steady_clock::now();
        
        while (!heap.empty() && heap.top().deadline <= now) {
            auto node = heap.top(); heap.pop();
            // 入队到原优先级队列
            auto* q = w->get_queue(node.source_queue_idx);
            if (q) q->enqueue(WorkItem::make_coro(node.handle));
        }
        
        // 可选优化: 如果下一个到期时间 > 某个阈值, yield 后 sleep 几微秒
        // 避免空转 CPU
        co_await yield();
    }
}
```

**注册**：在 `init_io_backend` 或 Worker 构造函数中启动 timer 协程。

### Offline Timer — 全局共享

**模型**：一个全局 timer_wheel 或 priority_queue，所有 worker 的 sleep 请求都入同一个堆。

```
全局 TimerWheel (单实例, mutex 保护, 或 lock-free):
  所有 worker 的 co_sleep → 入全局堆

Timer 线程 (或任意 worker 在 idle 时检查):
  定期检查堆顶, 到期项 → add_to_worker(source_worker_id, handle)
```

**数据结构**：

```cpp
class GlobalTimerWheel {
    std::priority_queue<TimerNode> pending_;
    std::mutex mutex_;  // 多 worker 并发访问
    std::thread timer_thread_;  // 或者用条件变量
public:
    void insert(TimerNode node);
    // timer_thread: loop { check top; if expired, route to worker; wait for next }
};
```

**与 Online 的关键区别**：
- 多 worker 并发访问需要同步 (mutex 或 lock-free)
- 恢复需要跨 worker 路由: `add_to_worker(node.source_worker_id, node.handle)`
- 可选独立 timer 线程, 或由 idle worker 承担 timer 检查

**API 统一**：

```cpp
// 自动感知 Online/Offline 上下文
auto co_sleep(nanoseconds dur) {
    if (auto* ow = current_online_worker()) {
        // Online: per-worker timer
        return OnlineSleepAwaiter{...};
    } else if (auto* exec = WorkStealingExecutor::current_executor()) {
        // Offline: global timer
        return OfflineSleepAwaiter{...};
    }
}
```

### 优先级队列恢复

**核心保证**：定时器到期后, 协程恢复回**原优先级队列** (`source_queue_idx`)。

```
协程在 P2 (engine) 调用 co_sleep(5ms):
  → handle 存入 timer 堆, 记录 source_queue_idx=P2
  → 5ms 后 timer 协程取出
  → w->get_queue(P2)->enqueue(WorkItem::make_coro(handle))
  → Scheduler drain P2 时恢复协程
```

### 实施任务

| 任务 | 描述 | 预计工时 |
|------|------|---------|
| T1: TimerNode + 堆结构 | 定义 TimerNode, 小顶堆, WorkerTimerState | 小 |
| T2: co_sleep Awaiter | 实现 SleepAwaiter, await_suspend 入堆 | 小 |
| T3: timer_coro_fn | 实现 Timer 协程 (挂起, 检查到期, 入队恢复) | 中 |
| T4: Online 注册 | Worker 启动时创建 timer 协程 (P4 队列) | 小 |
| T5: Offline GlobalTimer | 全局 timer_wheel, 跨 worker 路由 | 中 |
| T6: 测试 | co_sleep 精度, 到期恢复, Online/Offline 切换 | 中 |
