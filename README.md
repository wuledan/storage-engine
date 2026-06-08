# Storage Engine

高性能存储引擎，构建可插拔的多引擎存储资源池。

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
| **Runtime** | ✔ 已实现 | Online (RTC) + Offline (Worker-Stealing) 协程调度 |
| **IO Adapter** | 📋 设计中 | 可插拔的 io_uring / libaio / SPDK 后端 |
| **RPC Layer** | ⏳ 待设计 | DPDK TCP / RDMA userspace 网络传输 |
| **Consistency** | ⏳ 待设计 | 一致性协议层（Quorum / Consensus） |
| **Pluggable Engine** | ⏳ 待设计 | 多引擎抽象接口（LSM / B+Tree / Bitcask） |

### 关键特性

- **零阻塞 IO 路径**：协程提交 → co_await 挂起 → IO 完成 → affine queue (P0) → 协程恢复，全程无系统调用
- **自适应分发**：基于排队理论的容量分析，自动选择 Direct（per-worker NIC）或 Dispatch（独立 poller）模式
- **三级计数系统**：PerfLevel kNone(0 开销) / kCount(~3指令) / kTrace(~12指令)，纳秒级延迟直方图
- **Shared-Nothing**：每 Worker 独占 IO 队列、内存池、CPU 核心

## Benchmark 概要

| 维度 | 数值 |
|------|------|
| 在线 Worker 批量吞吐 | 3.1 M ops/s |
| 协程恢复延迟 (active) P50 | 982 ns |
| 纯队列调度 P50 | 683 ns |
| Worker 启动 P50 | 63 μs |

完整 benchmark 报告：[docs/2026-06-08/08-benchmark-report.md](docs/2026-06-08/08-benchmark-report.md)

## 设计文档

详见 [docs/design.md](docs/design.md)

## 构建

```bash
cmake -B build && cmake --build build
ctest --test-dir build          # 154/154 pass
```

要求：GCC 13+, CMake 3.20+, folly, hwloc, GTest, jemalloc
