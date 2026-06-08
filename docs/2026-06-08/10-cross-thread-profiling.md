# 跨线程任务投递 + 火焰图 Profiling 分析

> 日期：2026-06-08

## 1. 跨线程投递场景分析

### 1.1 当前 Engine 队列状态

**Engine 队列 = `BatchedSPSCWorkQueue`（SPSC，单生产者）**

```cpp
// online_worker.cc
idx_engine_ = add_queue(std::make_unique<BatchedSPSCWorkQueue>(
    QueueType::kEngine, Priority::kMedium, "engine", 200000));

void OnlineWorker::submit_engine(WorkItem item) {
    auto* q = static_cast<BatchedSPSCWorkQueue*>(get_queue(idx_engine_));
    q->push_batch(&item, 1);  // SPSC: 多生产者会数据损坏
}
```

### 1.2 多生产者场景

| 生产者 | 线程 | 到达方式 |
|--------|------|---------|
| RPC TCP 线程 | rpc_thread_X | `submit_engine(key_hash → worker_Y)` |
| RPC RDMA 线程 | rdma_thread | 同上 |
| Timer | timer_thread | `submit_to_worker(wid, item)` |
| 自身 Engine | worker 线程内 | `submit_engine` |

**结论：Engine 队列的实际生产者 ≥ 3 个不同线程，SPSC 语义已被违反。**

### 1.3 RDMA 场景细化

```
┌──────────────────────────────────────────────────┐
│                  RDMA Polling Core                │
│  ┌─────────────────────────────────────────┐     │
│  │  rdma_get_cq_event → CQ poll → request  │     │
│  │    → hash(key) → target OnlineWorker    │     │
│  └────────────┬────────────────────────────┘     │
│               │ submit_engine(item)               │
└───────────────┼──────────────────────────────────┘
                ▼
┌─────────── OnlineWorker 0 ───────────┐
│  engine_queue (MPMC) ← 修复后         │
│  Scheduler: poll P2 → dequeue → 处理  │
└───────────────────────────────────────┘
```

RDMA 连接管理的 CQ polling 可能独占一个 core（避免中断开销），请求处理交由 Online Worker 池。这是典型的 **分离式 IO + 计算** 模型。

### 1.4 修正方案

| 队列 | 当前类型 | 修正 | 容量 |
|------|---------|------|------|
| engine | `BatchedSPSCWorkQueue` | **`AffineWorkQueue`**（MPMC） | 200k |
| timer | `LocalWorkQueue` | **`AffineWorkQueue`**（MPMC） | 8k |

AffineWorkQueue 底层是 `folly::UMPMCQueue`，天然多生产者安全。engine 队列和 timer 队列统一使用 MPMC。

修正后的 submit_engine：
```cpp
void OnlineWorker::submit_engine(WorkItem item) {
    item.enqueue_ns = perf().record_enqueue(QueueType::kEngine, item.trace_id);
    auto* q = static_cast<AffineWorkQueue*>(get_queue(idx_engine_));
    q->enqueue(std::move(item));
    notify();
}
```

## 2. 火焰图 / 连续 Profiling 支持

### 2.1 当前状态

| 维度 | 状态 |
|------|------|
| `-fno-omit-frame-pointer` | ❌ 仅 Sanitized mode，Release 未启用 |
| jemalloc profiling | ❌ 未链接，未配置 `--enable-prof` |
| libunwind | ❌ 仅 folly 依赖，项目未直接使用 |
| perf 工具链外挂 | ✅ 但 Release 下缺帧指针 |

### 2.2 轻量级集成方案

#### Level 1: 外部 perf 火焰图（最小改动）

```cmake
# CMakeLists.txt: 所有 build type 添加
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g -fno-omit-frame-pointer")
```

一行搞定。`perf record -g -p PID` + FlameGraph 脚本可直接生成 CPU 火焰图。

#### Level 2: jemalloc 内存火焰图

```cmake
# 链接 jemalloc
target_link_libraries(runtime PUBLIC ${JEMALLOC_LIBRARY})
```

运行时：
```bash
MALLOC_CONF=prof:true,prof_prefix:jeprof.out LD_PRELOAD=libjemalloc.so ./test
jeprof --show_bytes --pdf ./test jeprof.out.*.heap > mem_flame.pdf
```

#### Level 3: 内置采样（连续 Profiling，进阶）

```cpp
// 周期性 SIGPROF → 采样调用栈 → ring buffer
// 合并 WorkerPerf 的统计 + 调用栈信息
class Profiler {
    void start(uint64_t interval_us);  // 采样间隔
    void stop();
    void dump_flamegraph(std::ostream&);  // 折叠栈格式输出
};
private:
    // libunwind 展开调用栈
    void sample_signal_handler(int);
    // ring buffer of sampled stacks
};
```

Level 1 + 2 改动最小（两行 CMake），优先实施。Level 3 可作为后续优化。

### 2.3 与 WorkerPerf 的整合

```
WorkerPerf (热路径):
  ├─ record_enqueue/dequeue/exec    【计数 + 直方图】
  └─ snapshot()                     【Pull 聚合】

外部 Profiling:
  ├─ perf record -g                 【CPU 火焰图】
  └─ jemalloc profiling             【内存火焰图】
```

两者互补：WorkerPerf 给出分阶段的精确延迟分布；perf/jemalloc 给出调用栈和内存分配的宏观视图。

## 3. 修正任务

| Task | 内容 | 工时 |
|------|------|------|
| H1 | Engine/Timer 队列 SPSC→MPMC 修正 | 1h |
| H2 | CMake: `-fno-omit-frame-pointer` + jemalloc 链接 | 0.5h |
| H3 | 测试：多线程同时 submit_engine 并发正确性 | 2h |
