---
name: io-bench
description: IO backend benchmark SOP — multi queue depth, ping-pong, batch throughput testing
compatibility: opencode
metadata:
  audience: developers
  workflow: benchmark
---

## IO Benchmark SOP

IO 子系统性能基准测试标准操作流程。每次 IO 层变更后必须执行。

### 前置条件

```bash
# 1. 构建
cd /home/wuledan/work/proj/storage-engine
cmake -B build && cmake --build build

# 2. 确认环境
lscpu | grep "Model name\|NUMA"
cat /proc/sys/fs/aio-max-nr
uname -r
free -h | head -2
```

### 测试套件

#### 1. 批量模式 (fire-and-forget)

测量 N 个请求同时提交后的总吞吐和平均延迟。

```bash
./build/tests/stress/test_benchmark --gtest_filter='IoUringIOPs*:IoUringLatency*:IoUringVsLibaio*'
```

#### 2. Ping-Pong 模式 (无队列堆积)

串行提交，每次等待完成后再提交下一个。测量单请求真实延迟。

```bash
./build/tests/stress/test_benchmark --gtest_filter='*PingPong*'
```

#### 3. 队列深度扩展 (SPDK-style pipelined)

保持固定 QD 在飞行中，测试不同并发度下的 IOPS/延迟曲线。

```bash
./build/tests/stress/test_benchmark --gtest_filter='*QueueDepthScaling*'
```

后端和深度测试矩阵：

| 后端 | 测试深度 | 数据目标 |
|------|---------|---------|
| io_uring | 1,4,8,16,32,64,128,256 | IOPS, P50/P99/P999, Avg |
| libaio | 1,4,8,16,32,64,128,256 | 同上 |

#### 4. 全量

```bash
./build/tests/stress/test_benchmark --gtest_filter='BenchmarkIO*'
```

### 数据采集模板

每次测试后，将数据填入以下模板更新 `docs/2026-06-08/08-benchmark-report.md`：

```
=== [后端名] Queue Depth Scaling ===
  QD    |  IOPS(K)  |  P50(us)  |  P99(us)  |  P999(us) |  Avg(us)
  ------|-----------|-----------|-----------|-----------|----------
  1     |  XXXXX.X  |  XX.XX    |  XX.XX    |  XXX.XX   |  XX.XX
  4     |  XXXXX.X  |  XX.XX    |  XX.XX    |  XXX.XX   |  XX.XX
  ...
```

### 验收准则

| 指标 | io_uring 阈值 | libaio 阈值 | 判定 |
|------|-------------|-----------|------|
| QD=1 P50 | < 15 μs | < 10 μs | 通过/不通过 |
| QD=128 IOPS | > 20 M | > 20 M | 同上 |
| QD=256 IOPS | > 40 M | > 40 M | 同上 |
| Ping-Pong P50 | < 15 μs | < 10 μs | 同上 |

> 实际值偏离预期 > 2x 时 Review 不通过，需提交根因分析。

### 环境要求

- 内核 ≥ 5.10 (io_uring 完整支持)
- liburing-dev / libaio-dev
- /proc/sys/fs/aio-max-nr ≥ 65536
- 测试文件系统：本地 NVMe SSD 或 tmpfs
- 不与其他 IO 密集型任务并行

### 输出产物

每次完整测试后更新：
1. `docs/2026-06-08/08-benchmark-report.md` — 数据表格 + 分析
2. `README.md` — 关键指标（如有变化）
3. git commit message 中包含变更前后的性能对比
