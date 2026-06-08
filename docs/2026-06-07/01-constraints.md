# 核心约束与设计原则

> 日期：2026-06-07

## 1. 编程模型

### 1.1 无阻塞编程
- 全 IO 路径（网络 + 存储）均为异步非阻塞
- 不允许任何线程因 IO 操作而挂起
- 所有阻塞点通过协程挂起/恢复机制处理

### 1.2 C++20 协程
- 基于 `co_await` / `co_return` 构建异步编程模型
- 避免回调地狱，保持代码可读性
- 协程恢复时路由回原线程（线程亲和）

### 1.3 Shared-Nothing 线程模型
- 每个线程独立拥有自己的资源：内存池、IO 队列、网络连接
- 线程间通过消息传递通信，消除锁竞争
- 数据分片按 key 路由到固定线程，保证单线程访问

## 2. 性能导向原则

- 设计及编码时始终以性能为第一优先级
- 关键路径避免内存分配（预分配 + 内存池）
- 数据拷贝最小化（零拷贝、分散-聚集 IO）
- Cache-friendly 数据结构设计
- 减少系统调用和上下文切换
- NUMA 感知的内存分配和调度

## 3. 可观测性要求

- 每个模块暴露指标（Prometheus 兼容格式）
- 关键路径埋点：延迟分布、吞吐量、错误率
- 结构化日志，支持分级输出
- 链路追踪 ID 贯穿请求全生命周期
- 线程级统计（无锁聚合）

## 4. 可插拔架构约束

### 4.1 存储引擎
- 定义统一的引擎接口
- 支持编译期或运行期切换
- 目标引擎类型：LSM-Tree / B+Tree / 自定义

### 4.2 IO 后端
- 抽象异步 IO 接口
- 支持：SPDK / libaio / io_uring
- 编译期可选，每 worker 可独立配置

### 4.3 RPC 传输
- 抽象网络传输接口
- 支持：DPDK TCP 栈 / RDMA 用户态栈
- 零拷贝收发路径

## 5. 编码规范

- 语言标准：C++20
- 头文件组织：模块化，接口与实现分离
- 错误处理：统一 Result<T> 类型，禁止异常用于控制流
- 日志：结构化输出，包含 thread_id / coroutine_id / trace_id
- 命名：snake_case 文件名，PascalCase 类型名，snake_case 函数/变量名

## 6. 性能交付约束

### 6.1 任务级性能预期

每个开发任务必须明确性能预期指标。Review 时验证实际数据与预期的一致性。

| 阶段 | 要求 |
|------|------|
| **任务设计** | 明确 P50/P99 延迟、吞吐量、内存开销的预期值 |
| **代码提交** | 附带 benchmark 数据，与预期对比 |
| **Review** | 实际值偏离预期 > 2x 视为不通过，需根因分析 |
| **修复后** | 重跑 benchmark，确认修复有效 |

### 6.2 基准数据（持续更新）

| 指标 | 当前值 | 预期目标 | 来源 |
|------|--------|---------|------|
| 纯队列调度 P50 | 683 ns | < 1 μs | `BenchmarkDetail.SchedulingLatencyInternal` |
| Active 跨线程 RTT P50 | 982 ns | < 2 μs | `BenchmarkDetail.CrossThreadActiveWorker` |
| Idle PARK 唤醒 P50 | 16.4 μs | < 25 μs | `BenchmarkDetail.CrossThreadIdleWorker` |
| io_uring Write P50 | 4.75 μs | < 10 μs | `BenchmarkIO.IoUringLatency` |
| io_uring IOPS | 292 K/s | > 200 K/s | `BenchmarkIO.IoUringIOPs` |
| Worker 启动 P50 | 63 μs | < 100 μs | `BenchmarkCoroutine.WorkerLifecycleLatency` |

### 6.3 偏离处理

```
实测值 > 预期 × 2:
  → Review 不通过
  → 分析根因（flame graph / perf counter / 队列深度）
  → 修复后重新 benchmark
  → 更新预期值（如果预期设定不当）

实测值 ≤ 预期:
  → Review 通过
  → 如显著优于预期(> 2x)，考虑收紧预期值
```

## 7. 待明确

- [ ] 一致性模型（强一致 / 最终一致 / 会话一致）
- [ ] 数据模型（KV / 列存 / 行存 / 混合）
- [ ] 存储引擎的具体类型（LSM-Tree / B+Tree / Bitcask 等）
- [ ] RPC 协议选型（自定义二进制 / gRPC 兼容 / thrift）
- [ ] 构建系统和第三方依赖管理策略
- [ ] MVP 功能范围和里程碑
- [ ] 代码仓库管理策略（Git 工作流）
