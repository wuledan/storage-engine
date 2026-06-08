# 在线任务分发模式：Direct / Dispatch 自适应

> 日期：2026-06-08
> 原则：根据硬件资源自动选择最优模式

## 1. 两种模式

### 1.1 Direct 模式（RTC，资源充足）

```
NIC RSS Queue 0 ──► Worker 0  (dedicated)
NIC RSS Queue 1 ──► Worker 1  (dedicated)
NIC RSS Queue 2 ──► Worker 2  (dedicated)
NIC RSS Queue 3 ──► Worker 3  (dedicated)
```

- 每个 Worker 独占一条 NIC RX 队列
- Scheduler 在 P1 优先级直接 poll 网络队列
- 请求到达 → 解析 → 处理，**全程无跨线程交互**
- Engine 队列为 `LocalWorkQueue`（零原子）
- **要求**：NIC 队列数 ≥ Worker 数

### 1.2 Dispatch 模式（资源受限）

```
NIC (有限队列)
  └─ Dispatch Poller Thread (独占 core)
       ├─ poll → parse → hash(key)
       └─ worker_N.submit_engine(task) ──► Worker N engine_queue (MPMC)
```

- NIC 队列数 < Worker 数，无法一一绑定
- 引入独立的 Dispatch Poller 线程负责收包和路由
- 投递走 MPMC Engine 队列
- **开销**：跨线程 enqueue (1 CAS) + notify

### 1.3 对比

| 维度 | Direct | Dispatch |
|------|--------|----------|
| NIC 队列要求 | ≥ Worker 数 | 任意 |
| 首包路径 | Worker 直接 poll | Dispatch → MPMC → Worker |
| Engine 队列类型 | `LocalWorkQueue`（零原子） | `AffineWorkQueue`（MPMC） |
| 跨线程投递 | **无** | 每次请求 1 次 CAS |
| 额外延迟 | 0 | ~300ns（跨线程 notify） |
| 适用 | DPDK RSS、io_uring 多队列 | 单队列 NIC、RDMA 独立 CQ 核 |

## 2. 自动检测

### 2.1 硬件拓扑采集

```cpp
struct HardwareTopology {
    size_t nic_rx_queues{0};    // ethtool -l 或 DPDK rte_eth_dev_info
    size_t rdma_cq_count{0};    // ibv_query_device
    size_t cpu_cores{0};        // hwloc
    size_t numa_nodes{0};

    // 有效网络队列数（取各接口最小值）
    size_t effective_nic_queues() const {
        return nic_rx_queues;
    }
};
```

### 2.2 模式决策

```cpp
enum class TaskDispatchMode : uint8_t {
    kDirect,      // per-worker NIC queue → LocalQueue
    kIndirect,    // shared NIC → DispatchPoller → MPMC
};

class DispatchConfig {
public:
    struct Config {
        // 强制模式（0 = 自动检测）
        TaskDispatchMode force_mode{static_cast<TaskDispatchMode>(0xFF)};
        
        // 自动检测时，NIC 队列 / Worker 达到此比例才启用 Direct
        double direct_mode_ratio{1.0};  // default: NIC queues >= workers
    };

    // 自动检测最优模式
    static TaskDispatchMode detect(
        const HardwareTopology& hw,
        size_t online_workers,
        const Config& cfg = {});

    // 为 Engine 队列创建合适的类型
    static std::unique_ptr<WorkQueue> create_engine_queue(
        TaskDispatchMode mode, size_t capacity);

    // 为 Timer 队列创建合适类型
    static std::unique_ptr<WorkQueue> create_timer_queue(
        TaskDispatchMode mode, size_t capacity);
};
```

### 2.3 决策逻辑

```cpp
TaskDispatchMode DispatchConfig::detect(
    const HardwareTopology& hw,
    size_t online_workers,
    const Config& cfg) {
    
    // 强制模式
    if (static_cast<uint8_t>(cfg.force_mode) != 0xFF) {
        return cfg.force_mode;
    }

    // 自动检测
    size_t effective_queues = hw.effective_nic_queues();
    if (effective_queues >= online_workers * cfg.direct_mode_ratio) {
        return TaskDispatchMode::kDirect;
    }
    return TaskDispatchMode::kIndirect;
}
```

## 3. OnlineWorker 自适应

### 3.1 构造时根据模式配置队列

```cpp
class OnlineWorker : public Worker {
public:
    struct Config : Worker::Config {
        TaskDispatchMode dispatch_mode{TaskDispatchMode::kDirect};
        size_t engine_queue_capacity{200000};
    };

    explicit OnlineWorker(const Config& cfg) : Worker(cfg) {
        switch (cfg.dispatch_mode) {
        case TaskDispatchMode::kDirect:
            // RTC: 零原子 LocalQueue
            idx_engine_ = add_queue(std::make_unique<LocalWorkQueue>(
                QueueType::kEngine, Priority::kMedium, "engine",
                cfg.engine_queue_capacity));
            break;
        case TaskDispatchMode::kIndirect:
            // Dispatch: MPMC AffineWorkQueue
            idx_engine_ = add_queue(std::make_unique<AffineWorkQueue>(
                QueueType::kEngine, Priority::kMedium, "engine",
                cfg.engine_queue_capacity));
            break;
        }
        // timer 队列同样处理
    }

    void submit_engine(WorkItem item) {
        item.enqueue_ns = perf().record_enqueue(QueueType::kEngine, item.trace_id);
        auto* q = get_queue(idx_engine_);
        switch (dispatch_mode_) {
        case TaskDispatchMode::kDirect: {
            auto* lq = static_cast<LocalWorkQueue*>(q);
            lq->try_enqueue(std::move(item));
            break;
        }
        case TaskDispatchMode::kIndirect: {
            auto* mq = static_cast<AffineWorkQueue*>(q);
            mq->enqueue(std::move(item));
            notify();  // 跨线程投递需唤醒 worker
            break;
        }
        }
    }

private:
    TaskDispatchMode dispatch_mode_;
};
```

### 3.2 Dispatch Poller（Indirect 模式专用）

```cpp
class DispatchPoller {
public:
    struct Config {
        uint32_t cpu_id{0};
        TaskDispatchMode mode{TaskDispatchMode::kIndirect};
    };

    explicit DispatchPoller(const Config& cfg, OnlineWorkerGroup& workers);

    void start();
    void stop();

private:
    void poll_loop();
    // poll NIC → parse → hash(key) → workers_.submit_to(key, task)

    OnlineWorkerGroup& workers_;
    // ...

    // 统计
    std::atomic<uint64_t> packets_received_{0};
    std::atomic<uint64_t> tasks_dispatched_{0};
};
```

## 4. Runtime 集成

```cpp
class Runtime {
public:
    struct Config {
        OnlineWorkerGroup::Config online_cfg;
        OfflineWorkerGroup::Config offline_cfg;
        DispatchPoller::Config dispatch_cfg;
        bool auto_detect_mode{true};
    };

    Runtime(const Config& cfg) {
        if (cfg.auto_detect_mode) {
            auto hw = HardwareTopology::probe();
            auto mode = DispatchConfig::detect(hw, cfg.online_cfg.num_workers);

            online_cfg_ = cfg.online_cfg;
            online_cfg_.dispatch_mode = mode;

            dispatch_cfg_ = cfg.dispatch_cfg;
            dispatch_cfg_.mode = mode;
        }
        // ...
    }

    void start() {
        online_group_.start();
        offline_group_.start();
        if (dispatch_cfg_.mode == TaskDispatchMode::kIndirect) {
            dispatch_poller_.start();
        }
    }

private:
    OnlineWorkerGroup online_group_;
    OfflineWorkerGroup offline_group_;
    DispatchPoller dispatch_poller_;
};
```

## 5. 配置示例

```cpp
// 自动检测
Runtime::Config cfg;
cfg.auto_detect_mode = true;
cfg.online_cfg.num_workers = 8;
Runtime rt(cfg);  // NIC >= 8 queues → Direct, else → Dispatch

// 强制 Direct
cfg.auto_detect_mode = false;
cfg.online_cfg.dispatch_mode = TaskDispatchMode::kDirect;

// 强制 Dispatch（RDMA 独立 CQ 场景）
cfg.online_cfg.dispatch_mode = TaskDispatchMode::kIndirect;
cfg.dispatch_cfg.cpu_id = 0;  // dispatch 独占 core 0
```

## 6. 数据流对比

### Direct 模式

```
NIC RSS → Worker.scheduler.poll(net_io P1)
  → parse → process → response
  【0 跨线程，0 CAS，全链路单线程】
```

### Dispatch 模式

```
NIC → DispatchPoller.poll
  → parse → hash(key) → queue.enqueue(MPMC, 1 CAS) → notify
  → Worker.scheduler.poll(engine P2) → dequeue → process
  【1 次 CAS per request，额外 ~300ns】
```

### 延迟预算

| 模式 | 首包额外开销 | 说明 |
|------|-------------|------|
| Direct | 0 | per-worker NIC 队列，直接 poll |
| Dispatch | ~982ns (active) / ~16.4μs (idle) | 跨线程 + 跨队列 hop |

> 数据基于现有 benchmark：Active 跨线程 P50=982ns

## 7. 实现任务

| Task | 内容 | 工时 |
|------|------|------|
| H1 | `TaskDispatchMode` 枚举 + `HardwareTopology` 探测 + `DispatchConfig::detect` | 2h |
| H2 | OnlineWorker 自适应队列（Direct→Local, Indirect→MPMC） | 2h |
| H3 | `DispatchPoller` 骨架（Indirect 模式） | 2h |
| H4 | Runtime 集成（auto_detect + 启动顺序） | 2h |
| H5 | 测试：两种模式独立 + 切换 + Benchmark 对比 | 3h |
| **总计** | **5 任务** | **11h** |
