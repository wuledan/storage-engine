# Storage Engine

高性能存储引擎，构建可插拔的多引擎存储资源池。

## 设计哲学

在低延迟时代，**尽量抛弃一切多余动作**——同步开销、系统调用开销、不必要的数据拷贝。以此为出发点，构建基础、可高度配置的 Runtime 框架。

在此基础上，**依据硬件资源和业务负载，构建排队模型并推导最优选择**，让一切行为透明、可解释、可量化。不靠经验值猜测，靠排队论和数据说话。

## 设计目标

- **高性能存储资源池** — 支持 KV、列存、行存等多引擎适配，统一抽象接口
- **C++20 协程无阻塞** — 基于 folly::coro 的全链路异步非阻塞 IO，shared-nothing 线程模型
- **智能调度** — 排队理论指导下的最优化资源配置与调度策略，自适应 Direct/Dispatch 模式

## 架构

```
RPC Layer (DPDK/RDMA) → Consistency Layer → Pluggable Engine → IO Backend (io_uring/libaio/SPDK)
                                    ↕
                          Execution Runtime (C++20 Coroutines)
```

### 核心模块

| 模块 | 状态 | 说明 |
|------|------|------|
| **Runtime** | ✔ | Online (RTC) + Offline (Worker-Stealing) 协程调度 |
| **IO Adapter** | ✔ | io_uring / libaio / SPDK 可插拔后端 |
| **Perf & Trace** | ✔ | 三级计数 + 纳秒直方图 + 请求级 Trace |
| **Decision System** | ✔ | 排队论驱动的自适应 Dispatch 模式 |
| **RPC Layer** | ⏳ | DPDK TCP / RDMA userspace |
| **Consistency** | ⏳ | Quorum / Consensus |
| **Pluggable Engine** | ⏳ | LSM / B+Tree / Bitcask |

### 关键特性

- **零阻塞 IO 路径**：co_await → io_uring submit → Scheduler poll → 回调 → AffinityBaton → affine queue (P0) → 协程恢复
- **自适应分发**：排队理论容量分析，自动选择 Direct（per-worker NIC）或 Dispatch（独立 poller）
- **三级计数**：PerfLevel kNone(0指令) / kCount(~3) / kTrace(~12)，rdtsc 纳秒精度
- **Shared-Nothing**：每 Worker 独占 IO 队列、内存池、CPU + NUMA 绑定

## Performance (NVMe QD=1, Solidigm P41 Plus 1TB)

| Backend | Framework P50 | fio P50 | Notes |
|---------|-------------|---------|-------|
| io_uring (SQPOLL) | 30μs | 47μs | Non-blocking beats blocking |
| libaio | 26μs | 19μs | Inline submit wins |

- Scheduler cycle: 328ns (drain_p0 41ns + drain_all 289ns)
- P1 IO coroutine: nanosecond suspend/resume, zero syscall
- QD=4 SLC cache: 10-17μs P50 all backends
- Full analysis: [docs/2026-06-10/24-io-perf-analysis.md](docs/2026-06-10/24-io-perf-analysis.md)

## 设计文档

详见 [docs/design.md](docs/design.md)

## Benchmarking

### Framework benchmark (CoroutinePipeline)
```bash
# Full multi-QD matrix (1-256)
./scripts/bench.sh

# Quick QD=1 test
./scripts/bench.sh

# Custom QDs: edit tests/stress/test_benchmark.cpp
#   std::vector<int> qds = {1, 4, 8};
```

Output includes per-QD: RIOP(K), LIOP(K), P50/90/99/999/9999(μs), BW(MB/s), and Probe(ns) breakdown (submit, flush, co_await, baton_rt, producer).

### fio comparison
```bash
# Compare io_uring at all QDs
./scripts/fio_compare.sh /mnt/nvme_test/fio_test 1G io_uring

# Compare libaio
./scripts/fio_compare.sh /mnt/nvme_test/fio_test 1G libaio
```

### Metrics collected
| Metric | Description |
|--------|-------------|
| RIOP(K) | Real IOPS (wall-clock, thousands) |
| LIOP(K) | Latency-derived IOPS (1/P50 average) |
| P50/90/99/999/9999 | Latency percentiles (μs) |
| BW(MB/s) | Bandwidth |
| Probe submit | SQE fill time (ns) |
| Probe flush | Submission time (ns, ~65ns with SQPOLL) |
| Probe co_await | Coroutine suspend→resume time (ns, includes IO wait) |
| Probe baton_rt | Baton round-trip time (ns) |
| Probe producer | Full per-batch producer time (ns) |
| Scheduler probe | avg ns/iter for drain_p0 / drain_all / total |

## 构建

```bash
cmake -B build && cmake --build build
ctest --test-dir build
```

要求：GCC 13+, CMake 3.20+, folly, hwloc, GTest, jemalloc, liburing, libaio
