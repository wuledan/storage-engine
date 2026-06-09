# Storage Engine 设计文档

## 文档索引

| 日期 | 文档 | 内容 |
|------|------|------|
| 2026-06-07 | [01-constraints.md](2026-06-07/01-constraints.md) | 核心约束与设计原则 |
| 2026-06-07 | [02-architecture-overview.md](2026-06-07/02-architecture-overview.md) | 系统架构概览 |
| 2026-06-07 | [03-execution-layer.md](2026-06-07/03-execution-layer.md) | 执行层设计：Online/Offline Worker Group |
| 2026-06-08 | [04-runtime-layer.md](2026-06-08/04-runtime-layer.md) | Runtime 层详细设计：多队列、调度策略、自适应空闲 |
| 2026-06-08 | [05-runtime-plan.md](2026-06-08/05-runtime-plan.md) | Runtime 实现计划 v2：20任务/55h + 三级测试体系 |
| 2026-06-08 | [07-perf-counter-design.md](2026-06-08/07-perf-counter-design.md) | 性能计数与 Trace 系统设计 |
| 2026-06-08 | [08-benchmark-report.md](2026-06-08/08-benchmark-report.md) | Benchmark 报告 |

## 项目定位

性能优先的存储引擎，采用 shared-nothing 线程模型，全链路无阻塞，支持可插拔的存储后端和网络传输层。

## 快速链接

- **核心约束**：[编程模型、性能原则、可观测性](2026-06-07/01-constraints.md)
- **架构概览**：[分层架构、模块职责、数据流](2026-06-07/02-architecture-overview.md)
- **执行层**：[Online RTC Worker / Offline Worker-Stealing / IO Poller](2026-06-07/03-execution-layer.md)
- **Runtime 设计**：[多队列模型、调度策略 API、自适应空闲](2026-06-08/04-runtime-layer.md)
- **实现计划**：[20 任务 / 55 工时 / 三级测试](2026-06-08/05-runtime-plan.md)
- **设计修正**：[Online 原子开销优化 / Offline 复用 quant/infra](2026-06-08/06-runtime-refinement.md)
TODO: huge pages
