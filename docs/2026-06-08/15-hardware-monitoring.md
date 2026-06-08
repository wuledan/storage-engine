# 硬件监控与容量分析框架

> 日期：2026-06-08

## 1. 容量模型的输入矩阵

排队模型 `DispatchPlan = f(λ, μ, ρ, C_disk, C_nic, N_workers)` 所需输入：

### 1.1 已有指标（WorkerPerf + SchedulerStats）

| 指标 | 来源 | 采集点 | 用途 |
|------|------|--------|------|
| λ (arrival rate) | `WorkerPerf::record_enqueue` delta | enqueue 时计数 | 负载强度 |
| μ (service rate) | `WorkerPerf::record_exec` ns | execute 后记录 | 单核处理能力 |
| ρ (utilization) | Scheduler busy_ns / (busy+idle) | 调度循环中 | 饱和度 |
| queue depth | `WorkQueue::approx_count` | 策略决策前 | 排队深度 |
| IO wait P50/P99 | `IOBackend::poll` 回调延迟 | IO 完成路径 | 存储延迟 |
| IOPS | `IOBackend::poll` 回调计数 | IO 完成路径 | 存储吞吐 |

### 1.2 缺失指标

| 指标 | 当前状态 | 采集方式 |
|------|---------|---------|
| **CPU wall-clock utilization** | ❌ 无 | `/proc/self/stat` 或 `clock_gettime(CLOCK_THREAD_CPUTIME_ID)` |
| **NVMe device utilization** | ❌ 无 | `/sys/block/nvme0n1/stat` (字段 11: IO 进行中) |
| **NVMe bandwidth** | ❌ 无 | `/sys/block/nvme0n1/stat` (字段 4,8: 读写扇区数) |
| **有效内存带宽** | ❌ 无 | 未来：perf_event PMU |
| **跨 NUMA 访问比例** | ❌ 无 | 未来：perf_event PMU |
| **hwloc 拓扑快照** | ⚠️ 桩 | `HardwareTopology::probe()` 静态 |

## 2. CPU 利用率采集

### 2.1 方案

```cpp
class CPUUtilization {
public:
    struct Snapshot {
        uint64_t user_ns;    // 用户态 CPU 时间
        uint64_t system_ns;  // 内核态 CPU 时间
        uint64_t wall_ns;    // 墙上时钟
        double utilization;  // (user+sys) / wall
    };

    // 线程级别（单 Worker）
    static Snapshot thread_snapshot() {
        struct timespec ts;
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
        // ...
    }

    // 进程级别
    static Snapshot process_snapshot() {
        // 读取 /proc/self/stat 字段 14-17
    }
};
```

### 2.2 集成点

每个 Worker 的 Scheduler 循环中，在 idle 分支记录：
```
if idle:
    idle_start = now()
    enter_idle()
    idle_ns += now() - idle_start
else:
    busy_ns += iteration_time
```

已有部分实现（`SchedulerStats::total_idles`），需扩展为纳秒精度累加。

## 3. NVMe 利用率采集

### 3.1 方案

```cpp
class DiskStats {
public:
    struct Snapshot {
        uint64_t read_ios;
        uint64_t read_sectors;
        uint64_t write_ios;
        uint64_t write_sectors;
        uint64_t ios_in_flight;    // 当前队列深度
        uint64_t io_ticks;         // IO 繁忙时间
    };

    // 读取 /sys/block/nvme0n1/stat
    static Snapshot read(const std::string& device);

    // 计算差值利用率
    static double utilization(const Snapshot& prev, const Snapshot& curr,
                               uint64_t elapsed_ms);
};
```

### 3.2 /sys/block/{dev}/stat 字段

| 字段 | 含义 | 用于 |
|------|------|------|
| 1 | 读 IO 次数 | IOPS |
| 4 | 读扇区数 | 带宽 (×512) |
| 5 | 写 IO 次数 | IOPS |
| 8 | 写扇区数 | 带宽 |
| 10 | IO 进行中 | 队列深度 |
| 11 | IO 毫秒数 | 利用率 = Δfield11 / Δtime |

### 3.3 集成点

`MetricsCollector::collect()` 中增加：
```cpp
auto disk = DiskStats::read("/dev/nvme0n1");
double disk_util = DiskStats::utilization(prev_disk_, disk, elapsed_ms);
```

## 4. 统一快照 API

```cpp
struct SystemMetrics {
    // 时间
    uint64_t snapshot_ns;
    double elapsed_seconds;

    // 应用层
    RuntimeMetrics runtime;  // 已有的 per-queue stats

    // 硬件层
    struct {
        double utilization;       // CPU 利用率
        uint64_t busy_ns;
        uint64_t idle_ns;
    } cpu;

    struct {
        double utilization;       // NVMe 繁忙比例
        double read_iops;
        double write_iops;
        double read_mbps;
        double write_mbps;
        uint64_t avg_queue_depth;
    } disk;

    struct {
        double utilization;
        double rx_pps;
        double tx_pps;
        double rx_mbps;
        double tx_mbps;
    } nic;  // 未来

    // 容量推导值
    RealTimeCapacity capacity;  // λ/μ/ρ/bottleneck/headroom
};
```

## 5. 容量分析框架

### 5.1 运行流程

```
FeedbackLoop::tick() 每 5s:
  1. SystemMetrics = MetricsCollector::collect()
     ├─ WorkerPerf snapshots  (已有)
     ├─ DiskStats::read()     (新增)
     └─ CPUUtil::snapshot()   (新增)

  2. capacity = CapacityEstimator::estimate(metrics)
     ├─ λ = enqueue_delta / time
     ├─ μ = exec_delta / time / workers
     ├─ ρ_cpu = cpu.utilization
     ├─ ρ_disk = disk.utilization
     └─ bottleneck = argmin(1-ρ_cpu, 1-ρ_disk, headroom_from_λ)

  3. plan = StrategyDecider::decide(capacity)
     ├─ ρ_disk > 0.9 → 存储瓶颈, 减少 IO 压力
     ├─ ρ_cpu > 0.85 → CPU 瓶颈, 扩容 worker
     └─ λ/μ > 0.7 → 接近饱和, 触发告警

  4. 决策执行 / 告警 / 日志记录
```

### 5.2 瓶颈判定升级

```cpp
BottleneckAnalysis CapacityEstimator::analyze(const SystemMetrics& m) {
    BottleneckAnalysis ba;

    ba.cpu_headroom = 1.0 - m.cpu.utilization;
    ba.disk_headroom = 1.0 - m.disk.utilization;
    ba.app_headroom = 1.0 - m.capacity.utilization;

    // 最小余量即为瓶颈
    double min_h = std::min({ba.cpu_headroom, ba.disk_headroom, ba.app_headroom});
    if (min_h == ba.cpu_headroom) ba.bottleneck = kCPU;
    else if (min_h == ba.disk_headroom) ba.bottleneck = kDisk;
    else ba.bottleneck = kApp;

    ba.headroom = min_h * 100;  // 百分比余量
    return ba;
}
```

## 6. 实现任务

| Task | 内容 | 工时 |
|------|------|------|
| M1 | `CPUUtilization` — 线程/进程 CPU 时间采集 | 1h |
| M2 | `DiskStats` — NVMe /sys/block 统计读取 | 1h |
| M3 | `SystemMetrics` — 统一快照结构体 | 0.5h |
| M4 | `MetricsCollector` 扩展 — 集成 CPU+Disk 采集 | 1h |
| M5 | `CapacityEstimator` 升级 — 硬件级瓶颈判定 | 1h |
| M6 | 测试：mock 指标 → 瓶颈判定验证 | 1.5h |
| **总计** | **6 任务** | **6h** |

## 7. 可视化输出参考

测试或运行时可输出：

```
=== System Metrics (5s interval) ===
CPU:     util=42.3%  busy=2.1s  idle=2.9s
NVMe:    util=67.8%  write=18.2K IOPS  bw=71.3MB/s  qd=3.2
Runtime: λ=10.5K/s  μ=12.3K/s/core  ρ=0.85  P99=1.2ms
Capacity: headroom=15%  bottleneck=CPU
Decision: maintain current (4 workers, Direct)
```
