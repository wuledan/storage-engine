# IO 批量提交：测试场景与验证计划

> 日期：2026-06-09
> 设计文档：17-batch-submit-design.md

## 测试矩阵

### 正确性测试

| # | 场景 | 输入 | 预期 | 验证方法 |
|---|------|------|------|---------|
| C1 | **BatchExactQD** | submit_batch(128 reqs), QD=128 | 全部完成，1 次 io_uring_submit | 计数 pending_sqe 在 flush 后归零，回调全部执行 |
| C2 | **BatchPartialUnderrun** | submit_batch(30 reqs), QD=128 | 30 全部完成，≤3 轮后强制 flush | 检查 age_iterations ≤ 3 时触发 flush |
| C3 | **BatchOverflow** | buffer=30 + submit_batch(100) | 先 flush 30，后处理 100 | buffer 排空顺序验证 |
| C4 | **PriorityAging** | P3 buffer 等 3 轮 + 新到 P2 | P3 先于 P2 flush | effective_priority 计算验证 |
| C5 | **SingleRequest** | submit(1 req) | 下一轮 flush | deadline=1，检查无延迟提交 |
| C6 | **P0Preempt** | buffer=50(P2) + submit_batch(1, P0) | P0 立即触发全部 flush | P0 到达 → effective≤0 → flush_all |
| C7 | **ConcurrentSubmitters** | 4 线程各 submit_batch(100) | 400 全部完成，无丢失无重复 | 总数校验，TSAN=0 |
| C8 | **BufferDrainOnStop** | Worker stop 前 buffer=50 | 全部 flush 后再退出 | stop 后 pending=0 |
| C9 | **MixedBackend** | io_uring + libaio 各跑 C1-C5 | 行为一致，差异在性能 | 对比测试 |

### 性能测试

| # | 场景 | 参数 | 对比基线 | 预期 |
|---|------|------|---------|------|
| P1 | **QD Scaling io_uring** | QD=1,4,16,64,128,256 | fio QD scaling | IOPS 接近 fio (±20%) |
| P2 | **QD Scaling libaio** | 同上 | fio | 同上 |
| P3 | **Batch vs Per-Req** | QD=128, 同 workload | 当前 per-request submit | 吞吐提升 ≥ 15% |
| P4 | **P50/P99 分布** | 各 QD 下延迟百分位 | fio P50/P99 | P99 ≤ 2× fio P99 |
| P5 | **Syscall 计数** | QD=128, io_uring | 理论值 1 | 实际 1-2 次 io_uring_enter |

### 压力测试

| # | 场景 | 参数 | 验收 |
|---|------|------|------|
| S1 | **持续高吞吐 30s** | QD=128, 不间断提交 | 无崩溃, IOPS 抖动 < 10% |
| S2 | **QD 动态切换** | QD: 1→128→1→128 循环 | 无卡死, buffer 正确排空 |
| S3 | **内存泄漏** | S1 后检查 RSS | 增长 < 5MB |

## 预期指标

| 指标 | 目标 | 判定 |
|------|------|------|
| io_uring QD=128 IOPS | ≥ 130K | 接近 fio 155K，扣除调度开销 |
| 吞吐提升 (vs per-req) | ≥ 15% | P3 对比 |
| io_uring_enter 次数 QD=128 | 1-2/batch | 验证批量提交生效 |
| P99 尾延迟 | < 100μs @ QD=128 | 无异常长尾 |
| 老化 fairness | P3 等待 ≤ 3 轮必 flush | 防饥饿 |

## 偏离分析流程

```
实测 != 预期
  → 收集数据: IOPS, P50/P99, syscall 计数, buffer age 分布
  → 分类:
      IOPS 低于预期 → analysis: Scheduler 开销占比, submitter 填充速率
      P99 高于预期 → analysis: CQ 收割是否完整, buffer 老化是否正常
      syscall 多于预期 → analysis: flush_pending 是否重复调用
  → 输出: 根因 + 数据支撑 + 修复方案
  → 修复后重跑 → 确认收敛
```
