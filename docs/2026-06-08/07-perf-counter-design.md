# 性能计数与 Trace 系统设计

> 日期：2026-06-08
> 参考：RocksDB PerfContext, Seastar reactor metrics, DPDK per-lcore, LevelDB Histogram
> 原则：shared-nothing 单写者，关键路径 < 10 指令

## 1. 设计目标

| 目标 | 指标 |
|------|------|
| 关键路径开销（禁用时） | **0 指令**（编译期内联消除） |
| 关键路径开销（仅计数启用） | **~3 指令**（load flag + add） |
| 关键路径开销（trace 启用） | **~15 指令**（flag + rdtsc + add to ring） |
| 线程安全 | **零锁**（shared-nothing 单写者） |
| 伪共享 | **零**（cache line 对齐，per-worker 隔离） |

## 2. 业界参考与本设计的映射

| 参考模式 | 来源 | 我们的采用 |
|---------|------|-----------|
| 单写者 per-core 原子 | Seastar / DPDK | ✅ Worker 独占 PerfCounters，无需 atomic |
| 比特操作直方图 | Seastar `lzcnt` 桶查找 | ✅ 64 桶 power-of-2，`__builtin_clzll` 无分支 |
| PerfLevel 多级开关 | RocksDB 7 级 | ✅ 简化为 kNone / kCount / kTrace 三级 |
| Pull 模式聚合 | Seastar lambda scrape | ✅ `snapshot()` 时跨 worker 归并 |
| Cache line 对齐 | DPDK `__rte_cache_aligned` | ✅ `alignas(64)` |
| TSC 时间戳 | DPDK `rte_rdtsc` | ✅ `__builtin_ia32_rdtsc` / `std::chrono` |

## 3. 数据结构

### 3.1 PerfLevel — 运行时三级开关

```cpp
enum class PerfLevel : uint8_t {
    kNone  = 0,  // 全部禁用，0 开销
    kCount = 1,  // 仅计数（ops, sum, max），不记录延迟分布
    kTrace  = 2,  // 完整 trace（直方图 + trace span）
};
```

### 3.2 Histogram — 无分支比特操作直方图

```cpp
class alignas(64) Histogram {
public:
    static constexpr size_t kNumBuckets = 64;  // 覆盖 1ns ~ ~9.2e18ns

    // O(1) 无分支：log2floor via lzcnt
    void record(uint64_t value) noexcept {
        int b = 63 - __builtin_clzll(value | 1);  // value|1 → >=1, clz → 前导零数
        buckets_[b].fetch_add(1, std::memory_order_relaxed);
        count_.fetch_add(1, std::memory_order_relaxed);
        sum_.fetch_add(value, std::memory_order_relaxed);
    }

    // 读取时估算 P50/P99
    double percentile(double p) const noexcept;

private:
    struct alignas(64) {
        uint64_t buckets_[kNumBuckets]{};
        uint64_t count_{0};
        uint64_t sum_{0};
    };
};
```

### 3.3 QueuePerf — 单队列性能计数

```cpp
struct alignas(64) QueuePerf {
    uint64_t enqueued_count{0};
    uint64_t total_wait_ns{0};       // sum of (dequeue_ns - enqueue_ns)
    uint64_t max_wait_ns{0};
    uint64_t total_exec_ns{0};

    // 延迟分布直方图（仅 PerfLevel::kTrace 时记录）
    Histogram wait_hist;
    Histogram exec_hist;
};
```

### 3.4 TraceSpan — 单次追踪记录

```cpp
struct TraceSpan {
    uint64_t trace_id;       // 请求 trace ID
    uint64_t enqueue_ns;     // 入队时间戳
    uint64_t dequeue_ns;     // 出队时间戳
    uint8_t  queue_type;     // QueueType 枚举值
    uint8_t  stage;          // 0=enqueue, 1=dequeue, 2=execute
};
```

### 3.5 WorkerPerf — Per-Worker 性能计数器

```cpp
class alignas(64) WorkerPerf {
public:
    explicit WorkerPerf(size_t worker_id);

    // ── 全局开关 ──
    void set_level(PerfLevel level) noexcept {
        level_.store(level, std::memory_order_release);
    }
    PerfLevel level() const noexcept {
        return level_.load(std::memory_order_acquire);
    }

    // ── 入队记录 ──
    // @param q: 队列类型
    // @param trace_id: 请求 trace ID (仅在 kTrace 时生效)
    // @return: 入队时间戳 (仅在 kTrace 时有效)
    uint64_t record_enqueue(QueueType q, uint32_t trace_id) noexcept {
        auto lv = level_.load(std::memory_order_acquire);
        if (lv == PerfLevel::kNone) [[likely]]
            return 0;

        queues_[static_cast<uint8_t>(q)].enqueued_count++;

        if (lv >= PerfLevel::kTrace) {
            uint64_t now = rdtsc_ns();
            trace_ring_.push_back({trace_id, now, 0, static_cast<uint8_t>(q), 0});
            return now;
        }
        return 0;
    }

    // ── 出队记录 ──
    void record_dequeue(QueueType q, uint64_t enqueue_ns) noexcept {
        auto lv = level_.load(std::memory_order_acquire);
        if (lv == PerfLevel::kNone) [[likely]]
            return;

        uint64_t now = rdtsc_ns();
        uint64_t wait = now - enqueue_ns;
        auto& qp = queues_[static_cast<uint8_t>(q)];
        qp.total_wait_ns += wait;
        qp.max_wait_ns = std::max(qp.max_wait_ns, wait);

        if (lv >= PerfLevel::kTrace) {
            qp.wait_hist.record(wait);
            // 补充 trace ring 中最近一条匹配的入队记录
            // (简化：按 trace_id 关联，或用 reserved trace_id)
        }
    }

    // ── 执行记录 ──
    void record_exec(QueueType q, uint64_t exec_ns) noexcept {
        if (level_.load(std::memory_order_acquire) == PerfLevel::kNone) [[likely]]
            return;
        auto& qp = queues_[static_cast<uint8_t>(q)];
        qp.total_exec_ns += exec_ns;
        if (level_.load(std::memory_order_acquire) >= PerfLevel::kTrace)
            qp.exec_hist.record(exec_ns);
    }

    // ── 快照归并 ──
    PerfSnapshot snapshot() const noexcept;

private:
    uint64_t rdtsc_ns() noexcept;

    std::atomic<PerfLevel> level_{PerfLevel::kNone};
    QueuePerf queues_[8];  // indexed by QueueType

    // Trace ring buffer（仅 kTrace 时使用）
    static constexpr size_t kTraceRingSize = 4096;
    std::array<TraceSpan, kTraceRingSize> trace_ring_;
    size_t trace_ring_pos_{0};
};
```

### 3.6 PerfSnapshot — 可查询的快照

```cpp
struct QueueSnapshotPerf {
    QueueType type;
    uint64_t enqueued;
    uint64_t avg_wait_ns;
    uint64_t max_wait_ns;
    uint64_t avg_exec_ns;
    uint64_t p50_wait_ns;
    uint64_t p99_wait_ns;
};

struct PerfSnapshot {
    size_t worker_id;
    std::vector<QueueSnapshotPerf> queues;
    std::vector<TraceSpan> traces;  // 仅当 level >= kTrace
    uint64_t snapshot_ns;
};
```

## 4. 关键路径集成

### 4.1 Scheduler 改造

```cpp
folly::coro::Task<void> Scheduler::run() {
    // ...
    while (running_.load(std::memory_order_acquire)) {
        // 批量 dequeue
        size_t n = queue->try_dequeue_batch(batch.data(), batch_size);

        for (size_t i = 0; i < n; ++i) {
            auto& item = batch[i];

            // ── 出队记录 ──
            if (item.enqueue_ns) [[unlikely]]    // 仅在 trace 启用时非零
                perf_->record_dequeue(queue->type(), item.enqueue_ns);

            // 执行
            auto t0 = now_ns();
            item.execute();
            auto exec_ns = now_ns() - t0;

            // ── 执行记录 ──
            perf_->record_exec(queue->type(), exec_ns);
        }
    }
    co_return;
}
```

### 4.2 WorkItem 扩展

```cpp
struct WorkItem {
    using Func = void(*)();
    union {
        Func func;
        std::coroutine_handle<> coro;
    };
    uint8_t tag;
    uint32_t trace_id;

    // 新增：入队时间戳（仅 PerfLevel::kTrace 时非零）
    uint64_t enqueue_ns{0};

    void execute() noexcept;
    // ...
};
```

### 4.3 enqueue 调用点改造

```cpp
void OnlineWorker::submit_engine(WorkItem item) {
    auto* q = static_cast<BatchedSPSCWorkQueue*>(get_queue(engine_idx_));
    // ── 入队记录 ──
    item.enqueue_ns = perf_.record_enqueue(QueueType::kEngine, item.trace_id);
    q->enqueue(item);
    notify();
}
```

## 5. 请求级别 Trace

### 5.1 TraceConfig

```cpp
class TraceConfig {
public:
    // 全局开关
    static void set_enabled(bool v) {
        enabled_.store(v, std::memory_order_release);
    }
    static bool enabled() {
        return enabled_.load(std::memory_order_acquire);
    }

    // 请求级别采样（1/N 采样率）
    static void set_sample_rate(uint32_t n) {
        sample_rate_.store(n, std::memory_order_release);
    }

    // 判断某个请求是否需要 trace
    static bool should_trace(uint32_t trace_id) {
        if (!enabled_.load(std::memory_order_acquire)) return false;
        uint32_t rate = sample_rate_.load(std::memory_order_acquire);
        if (rate == 0) return false;
        if (rate == 1) return true;
        return (trace_id % rate) == 0;
    }

private:
    static std::atomic<bool> enabled_;
    static std::atomic<uint32_t> sample_rate_;
};
```

### 5.2 使用方式

```cpp
// RPC 入口处分配 trace_id
uint32_t trace_id = next_trace_id_++;

// 根据配置决定是否 trace
bool do_trace = TraceConfig::should_trace(trace_id);

// 构造 WorkItem 时携带 trace_id
WorkItem item = WorkItem::make_func(callback);
item.trace_id = do_trace ? trace_id : 0;  // trace_id=0 表示不 trace
```

## 6. 性能评估

| 场景 | PerfLevel | 每次操作额外指令 | 估算开销 |
|------|-----------|----------------|---------|
| 无计数 | kNone | 0（编译期内联，分支预测 always taken） | 0ns |
| 仅计数 | kCount | ~3 (load flag + cmp + add) | ~0.5-1ns |
| Trace | kTrace | ~12 (flag + rdtsc + ring write + hist) | ~15-20ns |

rdtsc 开销：`__builtin_ia32_rdtsc` ≈ 20-30 CPU cycles ≈ 5-10ns @3GHz

## 7. 实现计划

| Task | 内容 | 工时 |
|------|------|------|
| G1 | Histogram + QueuePerf 基础类型 | 2h |
| G2 | WorkerPerf + WorkItem 扩展 + 集成 | 3h |
| G3 | TraceConfig + 全局/请求级开关 | 1h |
| G4 | 测试：直方图精度 + 计数器正确性 + 禁用时零开销 | 2h |
