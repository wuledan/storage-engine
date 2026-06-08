# IO 提交模型分析：当前 vs 预期 vs 业界

> 日期：2026-06-09

## 1. 预期模式 vs 当前实现

### 业界 (fio)

```
IO 线程 (独占核):
  loop:
    1. 填充 N 个 SQE (一次性, 纯用户态)
    2. io_uring_submit (1 次 syscall)
    3. io_uring_wait_cqe / peek_cqe (0 或 1 syscall)
    4. 每收割一个 CQE → 立即填充新 SQE (维持 QD)
    
  → 零调度开销, 纯 IO 紧循环
  → 不能同时做其他事
```

### 我们的 Scheduler (当前)

```
Worker 线程:
  loop:
    1. flush_submissions()          ← 批量提交所有已填充 SQE (1 syscall)
    2. drain CQEs until EAGAIN      ← 持续收割 (0 syscall)
    3. drain_p0 (协程恢复)           ← 优先处理
    4. snapshot + decide             ← 调度开销 (~300ns)
    5. dequeue P1/P2                 ← 处理低优先级任务
    6. execute tasks                 ← 可能填充新 SQE
    7. drain_p0 (连锁 P0)            ← 再次优先处理
```

### 关键差异

| 维度 | fio | 我们的 Scheduler |
|------|-----|-----------------|
| IO 批提交 | ✅ 1 syscall per batch | ✅ flush_submissions = 1 syscall per iteration |
| CQ 收割 | ✅ 连续收割 | ✅ drain loop until EAGAIN (已修复) |
| 流水线 | ✅ 收割→立即填充→维持 QD | ⚠️ 依赖外部 submitter 线程填充速度 |
| 额外开销 | 0 | snapshot+decide+queue process (~300ns) |
| 多任务能力 | 无 | P0 优先 + P1/P2 + 协程 |
| 亲和路由 | 无 | AffinityBaton → affine → Scheduler |

## 2. 当前瓶颈

Scheduler 每轮迭代做：flush → drain CQ → drain P0 → **snapshot+decide** → dequeue P1/P2 → execute。

**snapshot+decide 在 IO 密集型场景中是纯开销**。fio 无需这些，所以 IOPS 更高。

但这是有意为之——我们牺牲少量峰值 IOPS 换取：
- P0 优先处理（协程恢复优先于 bulk IO）
- 多队列公平调度（网络 IO 不被磁盘 IO 饿死）
- 可观测性（snapshot 是 perf counter 的来源）

## 3. 优化方向（不急于实现）

### Phase 1: 自适应 snapshot

```
if queue 为空 and io_backend 有活跃 IO:
    skip snapshot, 继续 poll IO  ← 省 200ns/iter
else:
    正常 snapshot + decide
```

### Phase 2: 高 QD 批量填充

```
// Scheduler 直接从 pending IO 队列填充 SQE
// 不依赖外部 submitter 线程逐个 fill
size_t fill_sqes_from_queue(P2_engine_queue, batch_size) {
    for (i = 0; i < batch_size; i++) {
        IORequest req = engine_queue.dequeue();
        sqe = io_uring_get_sqe();
        io_uring_prep_write(sqe, req...);
    }
    return batch_size;
}
```

### Phase 3: io_uring SQPOLL

```
IORING_SETUP_SQPOLL  // 内核线程 polling SQ ring
→ submit() 零 syscall, 内核自动提交
→ Scheduler 只需收割 CQ
```

## 4. 结论

**当前设计已支持批量提交**（`flush_submissions`），问题不在提交模式而在调度开销和 benchmark 驱动方式。CQ drain loop 修复后，io_uring P99 已对齐业界结论。

**不需要大改架构**。优化方向是减少 IO 密集场景下的调度开销（adaptive snapshot），而非重写提交模型。
