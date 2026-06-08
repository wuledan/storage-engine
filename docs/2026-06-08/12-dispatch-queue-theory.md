# Dispatch 模式智能决策系统 & 队列分析框架

> 日期：2026-06-08

## 1. 队列理论分析框架

### 1.1 系统建模

将存储引擎建模为 Jackson 排队网络：

```
         λ
    ┌─────────┐
    │  NIC    │  ──►  μ_net  (收包/解析)
    └────┬────┘
         │ (可选: Dispatch 路由)
    ┌────▼────┐
    │ Engine  │  ──►  μ_eng  (业务处理)
    └────┬────┘
         │
    ┌────▼────┐
    │  Disk   │  ──►  μ_disk (IO)
    └─────────┘
```

### 1.2 核心参数

| 参数 | 含义 | 获取方式 |
|------|------|---------|
| λ | 外部请求到达率 (req/s) | 压测 / 线上采样 |
| μ_net_core | 单核收包+解析吞吐 | benchmark: `net_poll + parse` 循环 |
| μ_eng_core | 单核业务处理吞吐 | benchmark: OnlineWorker 批量 submit |
| μ_disk_core | 单核磁盘 IO 吞吐 | fio + io_uring benchmark |
| Q | NIC 硬件队列数 | `ethtool -l` / DPDK dev_info |
| N_cpu | 可用 Worker 核数 | hwloc |
| N_disk | 盘数 / io_uring 队列数 | NVMe 数量 |
| T_cross | 跨线程提交延迟 | benchmark: Active cross-thread P50=982ns |
| T_poll | 单次空 polling 开销 | ~683ns (当前 Scheduler poll) |

### 1.3 瓶颈判定

```cpp
struct BottleneckAnalysis {
    // 各阶段有效容量 (req/s)
    double c_net  = Q * μ_net_core;              // NIC 总收包能力
    double c_eng  = N_cpu * μ_eng_core;           // CPU 总处理能力
    double c_disk = N_disk * μ_disk_core;         // 磁盘总吞吐能力
    double c_total = std::min({c_net, c_eng, c_disk});

    // 瓶颈在哪里
    enum Stage { kNet, kEngine, kDisk, kBalanced };
    Stage bottleneck;
    double headroom;  // c_total / λ - 1, 正值=有余量

    Stage identify_bottleneck() {
        if (c_net <= c_eng && c_net <= c_disk) return kNet;
        if (c_eng <= c_net && c_eng <= c_disk) return kEngine;
        if (c_disk <= c_net && c_disk <= c_eng) return kDisk;
        return kBalanced;  // 三者近似相等
    }
};
```

### 1.4 延迟预估模型 (M/M/1 近似)

```
系统利用率: ρ = λ / c_total

平均排队 + 服务延迟:
  L_queue = ρ² / (1 - ρ)          (M/M/1)
  L_total = L_queue + 1/μ_mean    (排队 + 服务)

P99 近似:
  L_p99 ≈ -ln(1-0.99) * 1/(μ_mean * (1-ρ))

跨线程 hop 额外延迟 (Dispatch 模式):
  L_extra = T_cross * P(跨线程)    // 982ns per hop
```

### 1.5 Direct vs Dispatch 的排队模型差异

```
Direct:
  λ_local = λ / N_worker           (负载均分)
  ρ_direct = λ_local / μ_eng_core

  L_direct = 1 / (μ_eng_core - λ_local)   ← 单级 M/M/1

Dispatch (M 个 dispatch, N 个 worker):
  λ_dispatch = λ / M               (每个 dispatch 线程负载)
  ρ_dispatch = λ_dispatch / μ_net_core

  λ_worker = λ / N                 (每个 worker 负载)
  ρ_worker = λ_worker / μ_eng_core

  L_dispatch = 1/(μ_net_core - λ_dispatch)     ← dispatch 级
             + T_cross                          ← 跨线程
             + 1/(μ_eng_core - λ_worker)       ← worker 级
             + T_cross                          ← 响应返回？
```

## 2. 四种 Dispatch 策略

### 2.1 策略定义

```
┌──────────────────────────────────────────────────────────┐
│ 1. DirectAll                                             │
│    NIC_RSS_0 → W0    NIC_RSS_1 → W1    ...              │
│    条件: Q >= N_workers, NIC 非瓶颈                       │
│    最优: μ 最大化，T_cross = 0                           │
├──────────────────────────────────────────────────────────┤
│ 2. DispatchSingle                                        │
│    NIC → [Dispatch] → {W0, W1, ..., Wn}                 │
│    条件: NIC 瓶颈 或 Q < N_workers, 但 λ 低              │
│    最优: 减少无效 polling 开销                            │
├──────────────────────────────────────────────────────────┤
│ 3. DispatchMultiple                                      │
│    NIC_RSS_0 → [D0] → {W0,W1}                           │
│    NIC_RSS_1 → [D1] → {W2,W3}                           │
│    条件: Q < N_workers, λ 高, Q > 1                      │
│    最优: 水平扩展 dispatch 吞吐                           │
├──────────────────────────────────────────────────────────┤
│ 4. Mixed                                                 │
│    NIC_RSS_0 → W0 (Direct)                              │
│    NIC_RSS_1 → W1 (Direct)                              │
│    NIC_RSS_2 → [D0] → {W2,W3,...}  (Dispatch)          │
│    条件: Q < N_workers, λ 中等                           │
│    最优: 部分绑核 + 部分消费者                            │
└──────────────────────────────────────────────────────────┘
```

### 2.2 决策树

```
                    ┌─────────────┐
                    │ NIC 是瓶颈?  │
                    └──┬──────┬───┘
                  是   │      │ 否
                       ▼      ▼
              ┌──────────┐  ┌──────────────┐
              │ 减少 poll │  │ Q >= N_workers?│
              │ 开销优先  │  └──┬──────┬────┘
              └─────┬────┘   是 │      │ 否
                    │          ▼      ▼
        λ*N_cpu/Q   │    ┌────────┐ ┌──────────────┐
        > μ_net?    │    │DirectAll│ │NIC容量>CPU+Disk?│
        ├──是──►DispatchMultiple   │ └──┬──────┬─────┘
        └──否──►DispatchSingle     │  是 │      │ 否
                                   │    ▼      ▼
                                   │ ┌──────┐ ┌────────┐
                                   │ │Mixed │ │DirectAll│
                                   │ └──────┘ │(Q个直连)│
                                   │          │+Dispatch│
                                   │          │consumer │
                                   │          └────────┘
                                   │
```

### 2.3 策略选择算法

```cpp
struct DispatchPlan {
    enum Strategy { kDirectAll, kDispatchSingle, kDispatchMultiple, kMixed };
    Strategy strategy;
    size_t direct_workers{0};
    size_t dispatch_threads{0};
    size_t consumers_per_dispatch{0};

    std::string describe() const;
};

DispatchPlan compute_optimal_plan(
    const Capacity& cap,
    const BottleneckAnalysis& bottleneck,
    size_t online_workers) 
{
    DispatchPlan plan;

    // Phase 1: NIC 瓶颈 → 减少 polling 开销
    if (bottleneck.bottleneck == BottleneckAnalysis::kNet) {
        // NIC 本身满负荷 → 更多 dispatch 线程无意义
        // 计算：单个 dispatch thread 能否覆盖 NIC 吞吐
        size_t min_dispatch = std::max(1UL,
            (size_t)std::ceil(cap.effective_nic() / μ_net_core));
        if (min_dispatch == 1) {
            plan.strategy = DispatchPlan::kDispatchSingle;
            plan.dispatch_threads = 1;
        } else {
            plan.strategy = DispatchPlan::kDispatchMultiple;
            plan.dispatch_threads = min_dispatch;
        }
        plan.consumers_per_dispatch = (online_workers - plan.dispatch_threads)
                                      / plan.dispatch_threads;
        return plan;
    }

    // Phase 2: NIC 非瓶颈 → 优先 Direct
    if (cap.nic_rx_queues >= online_workers) {
        plan.strategy = DispatchPlan::kDirectAll;
        plan.direct_workers = online_workers;
        return plan;
    }

    // Phase 3: Q < N, NIC 非瓶颈
    if (cap.effective_nic() >= cap.effective_cpu()) {
        // NIC 吞吐 > CPU 总吞吐 → 部分 Direct + 部分 Dispatch
        plan.strategy = DispatchPlan::kMixed;
        plan.direct_workers = cap.nic_rx_queues;
        plan.dispatch_threads = 1;
        plan.consumers_per_dispatch = online_workers - cap.nic_rx_queues;
    } else {
        // NIC 吞吐 < CPU → Dispatch 足够
        plan.strategy = DispatchPlan::kDispatchSingle;
        plan.dispatch_threads = 1;
        plan.consumers_per_dispatch = online_workers;
    }

    return plan;
}
```

## 3. 延迟预测模型

### 3.1 各策略延迟估算

```
DirectAll:
  L_e2e = 1/(μ_eng_core - λ/N)              ← 单级 M/M/1
        = ~1.5μs (P50, λ << μ)

DispatchSingle:
  L_e2e = 1/(μ_net_core - λ)                 ← dispatch 排队
        + T_cross * 2                         ← 跨线程 x2 (请求+响应)
        + 1/(μ_eng_core - λ/N)               ← worker 处理
        = ~1.5μs + 2*982ns = ~3.5μs (active)

DispatchMultiple:
  L_e2e = 1/(μ_net_core - λ/M)              ← 每 dispatch λ/M
        + T_cross * 2
        + 1/(μ_eng_core - λ/N)
        = ~1.5μs + 2*982ns = ~3.5μs (M≥1时dispatch队列浅)

Mixed:
  Direct worker: ~1.5μs
  Dispatch consumer: ~3.5μs
  全局 P50: (D*1.5 + C*3.5) / N   (D=direct数, C=consumer数)
```

### 3.2 饱和度曲线

```
延迟 (μs)
  ^
  │                        Dispatch
  │                    ╱
  │               ╱        Direct
  │          ╱         ╱
  │     ╱         ╱
  │╱         ╱
  └────────────────────────────► λ (req/s)
  0                           λ_max

  Direct 在低负载时延迟更低 (无跨线程)
  Dispatch 在 λ 接近 λ_max 时 Direct 队列先满 (因为每 worker 的 λ/N)
  Direct 理论最大吞吐: N * μ_eng_core
  Dispatch 理论最大吞吐: min(μ_net_core, N * μ_eng_core)
```

## 4. 硬件容量探测接口

### 4.1 统一抽象

```cpp
// 不同网络栈的探测接口
class INetworkProbe {
public:
    virtual ~INetworkProbe() = default;
    virtual size_t rx_queues() const = 0;          // RSS 队列数
    virtual double bandwidth_gbps() const = 0;      // 链路带宽
    virtual double max_pps_per_core() const = 0;    // 单核收包能力
};

// io_uring 网络栈
class IoUringNetworkProbe : public INetworkProbe {
    size_t rx_queues() const override {
        // io_uring 使用 IORING_SETUP_ATTACH_WQ 共享 worker queue
        // 每个 io_uring 实例可视为一个"队列"
        return num_io_uring_instances_;
    }
    double max_pps_per_core() const override {
        // 基于 io_uring multishot recv benchmark
        return pps_per_core_;
    }
};

// DPDK 网络栈
class DpdkNetworkProbe : public INetworkProbe {
    size_t rx_queues() const override {
        // rte_eth_dev_info.nb_rx_queues
        return rte_eth_dev_info_get(port_id_)->nb_rx_queues;
    }
    double max_pps_per_core() const override {
        // DPDK testpmd 基准
        return pps_per_core_;
    }
};

// RDMA 网络栈
class RdmaNetworkProbe : public INetworkProbe {
    size_t rx_queues() const override {
        // ibv_query_device → max_cq
        return device_attr_.max_cq;
    }
    double max_pps_per_core() const override {
        // perftest 基准
        return pps_per_core_;
    }
};
```

### 4.2 磁盘容量探测

```cpp
class IDiskProbe {
public:
    virtual ~IDiskProbe() = default;
    virtual size_t device_count() const = 0;
    virtual double bandwidth_gbps() const = 0;
    virtual double iops_per_core() const = 0;
    virtual size_t io_queues() const = 0;  // NVMe 硬件队列数
};

class NvmeDiskProbe : public IDiskProbe { /* ... */ };
class SpdkDiskProbe : public IDiskProbe { /* ... */ };
```

### 4.3 统一探测器

```cpp
class HardwareProbe {
public:
    struct Result {
        size_t cpu_cores;
        size_t numa_nodes;
        
        // 网络
        size_t nic_queues;
        double nic_bandwidth_gbps;
        double nic_pps_per_core;
        
        // 磁盘
        size_t disk_count;
        size_t disk_io_queues;
        double disk_bandwidth_gbps;
        double disk_iops_per_core;

        // 应用层 benchmark 数据（预热或历史数据）
        double eng_ops_per_core;   // 单核 Engine 处理能力
        double net_ops_per_core;   // 单核收包能力（含解析）

        Capacity to_capacity() const;
    };

    // 统一接口：根据当前使用的网络/磁盘栈自动选择探测方法
    static Result probe();
};
```

## 5. 实现任务（整合 H1-H5 + 分析框架）

| Task | 内容 | 工时 |
|------|------|------|
| H1 | `Capacity` + `BottleneckAnalysis` + `DispatchPlan` 数据模型 | 2h |
| H2 | `NetworkProbe`/`DiskProbe` 抽象 + io_uring 探测实现 | 2h |
| H3 | `OnlineWorker` 自适应队列（Direct→Local, 其他→MPMC） | 2h |
| H4 | `DispatchPoller` 骨架 + 多 Dispatch 支持 | 2h |
| H5 | `Runtime` 集成：auto_detect → DispatchPlan → 启动 | 2h |
| H6 | 测试：策略计算正确性 + 探测接口 mock + 两种模式 Benchmark 对比 | 3h |
| **总计** | **6 任务** | **13h** |

## 6. 框架演进方向

- [ ] 引入实时负载反馈，动态切换策略（λ 变化时重新评估）
- [ ] 引入 G/G/c 模型替代 M/M/1（更准确的延迟预测）
- [ ] 引入 Little's Law：L = λ * W，实时校准队列深度
- [ ] 引入网络排队建模：NIC 侧 < 1μs, dispatch 侧 < 1μs, engine 侧 ~1.5μs
- [ ] 引入方差分析：P99 不仅取决于平均值，还取决于服务时间分布
