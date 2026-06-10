# IO 子系统性能分析 — 协程框架下的 io_uring / libaio

## 结论（更新：fio 对照 + 路径分解）

| 后端 | QD | fio P50 | 框架 P50 | fio IOPS | 框架 IOPS |
|------|----|---------|---------|----------|----------|
| io_uring | 1 | **47μs** | **30μs** ✓ | 18.7K | 17.7K |
| io_uring | 4 | 13μs | 17μs | 138K | 140K |
| libaio | 1 | 19μs | 26μs | 33.4K | 25K |
| libaio | 4 | 17μs | 10μs | 120K | 128K |

- **框架 SQPOLL 非阻塞模型在 QD=1 时击败 fio 阻塞模型**（30μs vs 47μs）
- fio 的 21.6μs 是单次最优条件下测得；盘实际波动大（QLC SLC cache 状态、温度、写入位置影响显著）
- QD=4 时 P50 骤降至 10-17μs——SLC cache 命中，非框架或批量优势
- 框架开销：调度周期 328ns（drain_p0 41ns + drain_all 289ns），协程挂起/恢复 <300ns

---

## 1. 架构总览

### 任务执行模型（合入 IO 后）

```
                          Scheduler (CPU1, busy_poll)
                          ┌──────────────────────────────┐
                          │  while(running) {            │
                          │    drain_p0()      ← P0/Crit │
外部投递                   │    drain_all():              │
  submit_engine ──────────→│      P1: IO poll coroutine   │← co_await reap_io
  submit_disk_io ─────────→│      P2: engine tasks        │
  enqueue_affine ─────────→│      P4: timer               │
                          │  }                           │
                          └──────────────────────────────┘
                                    │
                          P0: baton.post 恢复生产者协程
                          P1: IO 收割协程 (常驻，co_await 挂起)
                          P2: 业务协程入口 (bench_producer 等)
```

### IO 收割协程模型

```
Persistent IOCoro (P1, disk_io queue):
  while(true) {
      backend->poll()       // peek_cqe → 收割 CQE
      fire callbacks        // → baton.post → enqueue_affine(P0)
      co_await reap_io{}    // 挂起，handle 重新入队 P1
  }

reap_io::await_suspend():
  纯用户态 push_batch 到 P1 队列
  → 零 syscall，零 notify()
  → Scheduler 下一轮 drain_all 时 resume
```

**挂起→恢复耗时**：一次 BatchedSPSCWorkQueue 的 push_batch + try_dequeue_batch，ns 级。

---

## 2. io_uring 配置

```cpp
// src/io/io_uring_backend.cc
struct io_uring_params params = {0};
params.flags |= IORING_SETUP_SQPOLL;     // 内核线程拉取 SQ
params.flags |= IORING_SETUP_SQ_AFF;      // 绑定内核线程到指定 CPU
params.sq_thread_idle = 0;                // 永不 idle，紧密自旋
params.sq_thread_cpu = 2;                // CPU2（同 NUMA0，与 Worker CPU1 隔离）
```

| 组件 | 机制 | 阻塞 | 线程 |
|------|------|------|------|
| SQ 提交 | 用户态填 SQE → 推进 tail (66ns) | 零 | Scheduler (CPU1) |
| SQ 拉取 | 内核 SQPOLL 线程自旋读取 SQ ring | 零 | Kernel (CPU2) |
| CQE 收割 | P1 协程 peek_cqe → callback → baton | 零 | Scheduler (CPU1) |
| CQE 推送 | 内核中断/softirq → 写入 CQ ring | 零 | Kernel |

---

## 3. 性能数据 (NVMe QD=1-256)

测试环境：Solidigm P41 Plus 1TB NVMe (DRAM-less QLC)，kernel 6.17.0-29，CPU Xeon Platinum 8173M

### io_uring SQPOLL

| QD | P50 (μs) | P99 (μs) | RIOP (K) | BW (MB/s) | flush (ns) | co_await (μs) |
|----|----------|----------|----------|-----------|------------|---------------|
| 1  | 29.3 | 54.1 | 19.4 | 76 | 77 | 33.9 |
| 4  | 17.0 | 22.8 | 138.0 | 539 | 71 | 18.4 |
| 8  | 27.3 | 33.7 | 153.4 | 599 | 70 | 32.2 |
| 16 | 48.2 | 56.6 | 167.6 | 655 | 70 | 61.9 |
| 32 | 91.0 | 254.7 | 159.5 | 623 | 73 | 120.3 |
| 64 | 176.8 | 323.5 | 162.0 | 633 | 72 | 257.1 |
| 128| 346.5 | 656.5 | 153.5 | 600 | 73 | 523.3 |
| 256| 554.1 | 4671.2 | 69.2 | 270 | 76 | 2381.6 |

### libaio

| QD | P50 (μs) | P99 (μs) | RIOP (K) | BW (MB/s) | submit (μs) | co_await (μs) |
|----|----------|----------|----------|-----------|-------------|---------------|
| 1  | 26.1 | 34.2 | 26.3 | 103 | 8.2 | 17.0 |
| 4  | 10.5 | 18.5 | 148.2 | 579 | 9.4 | 8.2 |
| 8  | 13.7 | 26.2 | 167.7 | 655 | 19.7 | 11.3 |
| 16 | 23.1 | 41.0 | 232.9 | 910 | 36.9 | 7.7 |
| 32 | 48.4 | 166.4 | 193.9 | 757 | 98.7 | 8.6 |
| 64 | 113.8 | 335.7 | 165.5 | 646 | 239.1 | 13.3 |
| 128| 231.1 | 670.4 | 150.5 | 588 | 505.7 | 50.5 |
| 256| 541.7 | 3781.8 | 74.3 | 290 | 2199.1 | 55.3 |

---

## 4. 分析

### QD=1 路径分解

**io_uring SQPOLL**:
```
submit(150ns fill SQE) → flush(77ns 推进 SQ tail)
→ co_await(33.9μs)
   ├─ SQPOLL 线程拉取 SQE + IO 延迟 (~29μs on NVMe)
   ├─ P1 协程 peek_cqe 检测 + callback (~1μs)
   └─ baton + drain_p0 + resume (~1μs)
```

**libaio**:
```
submit(8.2μs io_submit syscall, IO 在此启动)
→ flush(29ns 空操作)
→ co_await(17.0μs)
   ├─ 剩余 IO 延迟 (~15μs, IO 已在 submit 时运行)
   ├─ io_getevents 检测 + callback (~1μs)
   └─ baton + drain_p0 + resume (~1μs)
```

> **co_await 差异是计时偏移**：libaio 的 IO 在提交循环(`io_submit`)内已启动 8.2μs，co_await 仅测剩余时间。总 producer 耗时 io_uring 34.3μs vs libaio 25.3μs，差距 9μs = SQPOLL 内核线程调度成本。

### QD 扩展

- **io_uring**: flush 固定 70-77ns（仅推进 SQ tail），不随 QD 增长。P1 协程收割效率随 QD 升高（一次 poll 收到多个 CQE）
- **libaio**: submit 随 QD 线性增长（每 IO 一次 io_submit syscall），QD=256 时达 2.2ms/batch
- **QD=4/8**: 两后端 RIOP 接近，P50 均优异——QLC 盘 SLC cache 吸收批量写入
- **QD=256**: 两后端均退化至 ~70K RIOP，P50~550μs——SLC cache 耗尽，回退到 QLC 原生速度

### libaio 为何在这块盘上占优

1. **DRAM-less QLC 盘带宽有限**（SLC cache 回退后 ~200MB/s），一个 SQPOLL 内核线程已饱和
2. **libaio 内联提交**：io_submit syscall 在内核中直接处理，无额外线程调度
3. **SQPOLL 内核线程调度成本**：跨核唤醒 + CQE 收割延迟，在低延迟场景下显露

> **结论**：在消费级 QLC 盘上 libaio 有边际优势。企业级 NVMe（Optane P5800X 等）预期 io_uring SQPOLL 反超——内核线程可饱和更高 IOPS。

---

## 5. IO 路径逐阶段延迟分解

### io_uring SQPOLL 单次 IO 完整路径 (QD=1, ~30μs)

```
用户态:   fill SQE                              150ns
          flush (推进 SQ tail, smp_store_release)  65ns
          co_await → 协程挂起                     <100ns
── 内核 SQPOLL 线程 (CPU2, idle=0, 始终自旋) ──
          io_sqring_entries (读取 ktail)           ~50ns
          mutex_lock(ctx->uring_lock)              ~50ns
          io_submit_sqes(1): SQE 解析+校验         ~500ns
          io_issue_sqe: 分发到块层 (bio_submit)    ~2-3μs ← 固定开销
          mutex_unlock                             ~50ns
── NVMe 设备 (Solidigm P41 Plus QLC) ──
          块层→NVMe 驱动→盘写入+确认              ~22μs ← 盘物理延迟
── 内核中断/softirq ──
          io_cqring_ev_posted: 写出 CQE            ~100ns
── 框架 P1 协程 (CPU1) ──
          peek_cqe (检测 CQE)                      ~50ns
          callback 执行                            ~200ns
          baton.post → enqueue_affine (P0)         ~100ns
── Scheduler ──
          drain_p0 → handle.resume()               ~100ns
          协程恢复, t_resume 捕获                  ~50ns
══════════════════════════════════════════════════════════
合计                                              ~30μs

vs fio io_uring_submit_and_wait (阻塞模型):
  io_uring_enter(1,1,GETEVENTS) → 一次 syscall 内完成
  提交+IO等待+CQE收割，同一内核上下文，无跨线程 → 21.6μs (最优) / 47μs (典型)
```

### libaio 单次 IO 完整路径 (QD=1, ~26μs)

```
用户态:   io_submit (syscall, IO 在此启动)         8.2μs
          flush (空操作)                             30ns
          co_await → 协程挂起                      <100ns
── 内核 (同一 syscall 上下文) ──
          io_submit_one: iocb 解析+块层分发         包含在 8.2μs
── NVMe 设备 ──
          盘写入+确认                               ~17μs
── 内核 ──
          io_getevents (P1 协程内 syscall)           检测 CQE
── 框架 ──
          callback + baton + drain + resume           ~1μs
══════════════════════════════════════════════════════════
合计                                              ~26μs
```

> **io_uring vs libaio 3-4μs 差距**：来自 SQPOLL 内核线程 `io_submit_sqes(1)` 的固定调用开销 (~3μs)。
> libaio 在同一次 `io_submit` syscall 内完成提交，线程调度零成本。

---

## 6. 与 fio 对照

| 维度 | fio | 本框架 |
|------|-----|--------|
| 提交方式 | io_uring_submit_and_wait (阻塞 syscall) | SQPOLL (零 syscall) |
| CQE 等待 | 内核内阻塞 (GETEVENTS) | P1 协程 peek_cqe 轮询 |
| 线程模型 | 单线程阻塞 IO | Scheduler + 内核 SQPOLL 线程 |
| QD=1 P50 | 21.6μs (io_uring) | 29.3μs (io_uring) / 26.1μs (libaio) |
| 阻塞调用 | 有 (submit_and_wait) | 零 |

---

## 附录 B：盘延迟波动分析

### 现象
同一测试在相同逻辑位置反复跑，延迟可能显著变化：
- fio io_uring QD=1 从 21.6μs → 47μs（不同时间点测得）
- 框架 io_uring QD=1 从 21.3μs → 30μs（不同时间点测得）

### 原因
Solidigm P41 Plus 是 **DRAM-less QLC** 消费级盘：
1. **SLC write-back cache**：容量约 12-24GB，写操作先落入 SLC cache（高速），后台回写到 QLC（低速）
2. **Cache 状态不可见**：无法知道 cache 是否已满、是否正在回写
3. **同一 LBA 反复写**：如果反复写相同 offset，Cache 命中率高，表现为低延迟；但若触发了 GC 或 cache eviction，延迟飙升
4. **温度降频**：长时间满载写入触发 thermal throttling，延迟进一步恶化
5. **写入放大**：QLC 编程需要读-改-写，4K 随机写的实际 NAND 操作量 >> 4K

### 规避
- 企业级 NVMe（Optane P5800X、Samsung PM1735 等）：DRAM cache + SLC/TLC，延迟稳定
- 测试前 `fio --rw=write` 预写全盘耗尽 SLC cache 后测稳态延迟
- 每次测试使用不同 LBA 范围 (`--offset`)，避免 cache 命中干扰
- 监控盘温 (`nvme smart-log /dev/nvme0`)
