# 系统架构概览

> 日期：2026-06-07

## 1. 分层架构

```
┌─────────────────────────────────────────────────┐
│                  RPC Layer                        │
│         (dpdk tcp / rdma userspace)              │
│              ─ C++20 Coroutines ─                │
├─────────────────────────────────────────────────┤
│              Consistency Layer                    │
│    (quorum / consensus / replication log)        │
├─────────────────────────────────────────────────┤
│             Pluggable Engine Interface            │
│    ┌──────────┬──────────┬──────────┐            │
│    │  Engine  │  Engine  │  Engine  │            │
│    │    A     │    B     │    C     │            │
│    └──────────┴──────────┴──────────┘            │
├─────────────────────────────────────────────────┤
│           Pluggable IO Backend                    │
│         (spdk / libaio / io_uring)               │
├─────────────────────────────────────────────────┤
│              Execution Runtime                    │
│     (Online RTC + Offline Worker-Stealing)       │
└─────────────────────────────────────────────────┘
```

## 2. 模块概要

| 模块 | 职责 | 状态 |
|------|------|------|
| **Execution Runtime** | 线程模型、协程调度、IO 轮询、定时器 | 🔴 设计中 |
| **RPC Layer** | 高性能网络传输，支持 DPDK TCP 栈和 RDMA 用户态栈 | ⏳ 待设计 |
| **Consistency Layer** | 数据一致性保证（副本复制、共识、恢复） | ⏳ 待设计 |
| **Pluggable Engine** | 可插拔存储引擎抽象接口及实现 | ⏳ 待设计 |
| **IO Backend** | 可插拔的异步 IO 后端（SPDK / libaio / io_uring） | ⏳ 待设计 |
| **Observability** | 指标采集、日志、链路追踪 | ⏳ 待设计 |

## 3. 模块间数据流

```
Client Request
     │
     ▼
┌─────────┐     ┌──────────────┐     ┌────────────────┐
│  RPC    │────►│ Consistency  │────►│ Pluggable      │
│  Layer  │     │ Layer        │     │ Engine         │
│         │◄────│              │◄────│                │
└─────────┘     └──────────────┘     └───────┬────────┘
                                             │
                                    ┌────────▼────────┐
                                    │   IO Backend    │
                                    │                 │
                                    └────────┬────────┘
                                             │
                                    ┌────────▼────────┐
                                    │    Storage      │
                                    │   (NVMe/SSD)    │
                                    └─────────────────┘
```

所有请求在 Execution Runtime 的调度下流转，保证全链路无阻塞。

## 4. 执行层概述

详见 [03-execution-layer.md](03-execution-layer.md)。

- **Online Worker Group**：Shared-Nothing RTC 模型，处理实时读写请求
- **Offline Worker Group**：Worker-Stealing 模型，处理 Compaction / EC 等长尾任务
- **IO Poller**：异步 IO 完成轮询，回调路由回原 Worker

## 5. 关键技术选型（草案）

| 维度 | 候选 | 倾向 |
|------|------|------|
| 协程框架 | C++20 原生 / folly::coro / libcoro | folly::coro（成熟，生态完善） |
| 无锁队列 | ChaseLev / MPMC / SPMC | ChaseLevDeque（参考 quant/infra） |
| NUMA 拓扑 | hwloc | hwloc |
| 构建系统 | CMake / Bazel | CMake |
| 测试框架 | CTest + GTest | GTest |
| 内存分配 | 系统 allocator / jemalloc / tcmalloc | jemalloc（多线程友好） |
