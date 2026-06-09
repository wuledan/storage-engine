# IO 子系统性能分析 — 非阻塞协程模型下的 io_uring / libaio

## 结论

- **io_uring P50=21.3μs，与 fio (21.6μs) 差距 ≤1.4%**，框架额外开销可忽略
- **非阻塞架构关键：SQPOLL 内核线程提交 + peek_cqe 用户态轮询**，调度器零 syscall
- **libaio P50=27.8μs**，其 `io_submit` 在提交循环内联完成，适合低 QD 场景但缺少批量化能力
- **协程框架开销：baton 47ns、调度器 ≤1μs/iter、协程挂起/恢复数十纳秒**

---

## 1. 架构总览

### 非阻塞模型
```
Producer Coroutine                Scheduler (CPU1)               Kernel SQPOLL (CPU2)
─────────────────                ─────────────────               ────────────────────
fill SQE (submit) ──┐
advance SQ tail ────┤ flush=66ns
co_await baton ─────┤→ suspend
                    │              poll: peek_cqe → EAGAIN
                    │              drain_p0 → drain_all
                    │              ...repeat...                  拉取 SQE → 提交 IO
                    │                                            IO 完成 → 写入 CQE
                    │              peek_cqe → CQE found!
                    │              callback → baton.post
                    │              enqueue_affine → drain_p0
                    └→ resume ←─── execute coroutine
```

### 设计约束
- **禁止阻塞调用**：不允许 `folly::coro::blockingWait`、`std::mutex`、阻塞式 `io_uring_enter`
- **单一 io_uring 实例**：一个 SQPOLL 内核线程足以驱动单盘带宽
- **用户态 CQE 检测**：`peek_cqe` 零 syscall，与内核线程完全解耦
- **Scheduler 线程**：只做 poll/drain/decide，不包含任何 IO 特定分支

---

## 2. io_uring 配置

```cpp
// src/io/io_uring_backend.cc
struct io_uring_params params = {0};
params.flags |= IORING_SETUP_SQPOLL;    // 内核线程拉取 SQ
params.flags |= IORING_SETUP_SQ_AFF;     // 绑定内核线程到指定 CPU
params.sq_thread_idle = 0;               // 永不 idle，紧密自旋
params.sq_thread_cpu = 2;               // 绑定 CPU2（同 NUMA0，与 Worker CPU1 隔离）
```

- **SQPOLL**：内核创建专用线程轮询 SQ ring，用户态只填 SQE + 推进 tail，无 `io_uring_enter`
- **SQ_AFF + sq_thread_cpu=2**：内核线程与 Scheduler 隔离在不同核，避免争抢
- **sq_thread_idle=0**：禁止休眠——QV=1 时提交间隔极短，休眠/唤醒引入的延迟(8.6μs)远大于自旋成本

### 为什么不用 SINGLE_ISSUER
- `IORING_SETUP_SINGLE_ISSUER` 优化 `io_uring_enter` 内核锁，但我们在 SQPOLL 下已取消所有 `io_uring_enter` 调用
- 该标志与我们的线程模型冲突：ring 在测试线程创建，submit 在 Scheduler 线程执行

### 为什么不阻塞等 CQE
- `io_uring_enter(GETEVENTS)` 会阻塞 Scheduler 线程，破坏多队列调度
- `io_uring_submit_and_wait` 同理——单次 syscall 获得 fio 级 P50，但以阻塞为代价

---

## 3. libaio 配置

```cpp
// src/io/libaio_backend.cc
io_setup(queue_depth, &ctx_);
// submit: io_prep_pwrite → io_submit(ctx, 1, &iocb)
// poll:   io_getevents(ctx, min_nr, max_nr, events, &ts_zero)
```

- **内联提交**：`io_submit` 在 submit() 内部完成 syscall，与协程同线程
- **零超时轮询**：`io_getevents` 使用 `timespec{0,0}`，非阻塞返回

---

## 4. 性能数据

### 测试环境
- **CPU**：2× Intel Xeon Platinum 8173M, 56c/socket, 2 NUMA, 125GB
- **盘**：Solidigm P41 Plus 1TB NVMe (DRAM-less QLC, SLC write-back cache)
- **内核**：6.17.0-29-generic
- **fio**：--direct=1 --ioengine=io_uring/libaio --iodepth=1

### NVMe QD=1 — 非阻塞协程框架 vs fio 裸 IO

| 实现 | flush/syscall | co_await | 单 IO 总耗时 | P50 | RIOP | vs fio |
|------|-------------|----------|-------------|-----|------|--------|
| **io_uring SQPOLL** | 66ns | 23.9μs | 24.3μs | **21.3μs** | 27.4K | -1.4% |
| libaio | 31ns (io_submit 8.3μs 在 submit 内) | 17.4μs | 25.9μs | 27.8μs | 25.6K | +2.6% |
| fio io_uring | — | — | — | 21.6μs | 33K | 基准 |
| fio libaio | — | — | — | 27.1μs | 24K | 基准 |

> **P50 对齐**：框架路径上的 io_uring P50(21.3μs) 与 fio 裸 io_uring(21.6μs) 差距仅 0.3μs(1.4%)。
> co_await 与原始 submit syscall 时间为计时偏移：libaio 的 IO 在提交循环内已启动，co_await 测量剩余时间。
> 
> **RIOP**：fio 33K vs 我们 27.4K。17% 差距来自 fio 使用 `io_uring_submit_and_wait`(一次阻塞 syscall)而我们使用 SQPOLL 内核线程 + peek_cqe(异步解耦)。

### tmpfs /dev/shm QD=1 — 消除磁盘延迟后的纯框架成本

| 实现 | flush | co_await | 单 IO 总耗时 | P50 | RIOP |
|------|-------|----------|-------------|-----|------|
| io_uring SQPOLL | 69ns | 6.6μs | 6.9μs | 5.7μs | 96.5K |
| libaio | 21ns | 2.3μs | 4.8μs | 3.4μs | 136.7K |

> 盘延迟 ≈ NVMe P50 − tmpfs P50：io_uring 21.3−5.7≈15.6μs，libaio 27.8−3.4≈24.4μs。
> 两者不一致是因为测量起点不同（libaio 提交早，已含部分 IO 时间在 submit 中）。
> 
> tmpfs 上的 2.1μs 差异 = SQPOLL 内核线程调度代价。这是用独立内核线程换取非阻塞提交的固有成本。

### 框架微基准

| 组件 | 延迟 | 备注 |
|------|------|------|
| AffinityBaton 往返 | 47ns | baton.post → co_await 恢复 |
| Scheduler 空闲迭代 | 679ns | 标准路径 (snapshot+decide+idle) |
| Scheduler 忙碌迭代 | 400ns | busy_poll 模式 (仅 poll+drain) |
| C++20 协程挂起/恢复 | <100ns | suspend_always + resume |
| 裸 io_uring P50 (无框架) | 19.7μs | 直接 submit + flush + busy-wait |
| 裸 libaio P50 (无框架) | 18.6μs | 直接 io_submit + io_getevents |

---

## 5. IO 时间分解

### io_uring SQPOLL 单次 IO 全路径 (24.3μs)

```
submit(150ns) → flush(66ns) → co_await(23.9μs)
                                  ├─ IO 延迟 (~15.6μs on NVMe)
                                  ├─ SQPOLL 线程调度 (~2μs)
                                  ├─ Scheduler CQE 检测 (13iter × 778ns ≈ 10μs 并行)
                                  ├─ callback + baton.post (~1μs)
                                  └─ drain_p0 + resume (~100ns)
```

### libaio 单次 IO 全路径 (25.9μs)

```
submit(8.3μs io_submit syscall) → flush(31ns) → co_await(17.4μs)
                                      ↑ IO 已在 submit 内开始     ├─ 剩余 IO 延迟
                                                                ├─ Scheduler 检测
                                                                ├─ callback + baton.post
                                                                └─ drain_p0 + resume
```

---

## 6. 设计决策

| 决策 | 选择 | 原因 |
|------|------|------|
| 提交模型 | SQPOLL 内核线程 | 非阻塞，零 syscall 提交 |
| CQE 检测 | peek_cqe 用户态轮询 | 配合 SQPOLL，调度器不阻塞 |
| 内核线程 CPU | CPU2 (与 Scheduler 隔离) | 避免同核争抢，同 NUMA 保持缓存亲和 |
| 内核线程休眠 | idle=0 (不休眠) | QD=1 提交间隔 33μs，休眠/唤醒开销 > 自旋成本 |
| libaio 保留 | 是 | 低 QD 场景下内联提交路径更短，且可作为对比基准 |
| 单 io_uring 实例 | 是 | 一个 SQPOLL 线程足以饱和单盘带宽 |
| 阻塞模式 | 全面禁止 | 破坏 Scheduler 多队列调度模型 |

---

## 7. 与 fio 的差异

| 维度 | fio | 本框架 |
|------|-----|--------|
| 提交方式 | `io_uring_submit_and_wait` (1次阻塞syscall) | SQPOLL 内核线程 (0次 syscall) |
| CQE 等待 | 内核内阻塞 (GETEVENTS) | 用户态 peek_cqe 轮询 |
| 线程模型 | 单测试线程阻塞在 IO | Scheduler 线程 + 内核 SQPOLL 线程 |
| QD=1 P50 | 21.6μs | 21.3μs |
| QD=1 特征 | 阻塞模型最优 | 非阻塞模型最优，支持多队列调度 |

---

## 8. 下一步

- 多 QD 扩展测试 (1,4,8,16,32,64,128,256)：验证 SQPOLL 批量提交优势
- IOPOLL 延迟优化：内核轮询设备完成，消除中断延迟
- 企业级 NVMe 对比 (如 Intel Optane P5800X)
- `io_uring_register_files/buffers` 减少内核 fd/buffer 查找开销
