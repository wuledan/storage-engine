# Work-Stealing Benchmark

## 测试方法

```
bthread 风格的 work-stealing 基准：
- N 个 submitter 线程并发提交任务到 executor
- 每个任务做固定时长的 CPU busy-spin（模拟计算负载）
- 测量总吞吐、每 worker 偷取数、park 次数
```

## 测试用例

### ThroughputScaling
4 submitter × 100K tasks，每任务 work_us ∈ {0, 10, 50, 100}

### StealEfficiency
4000 tasks 全推 worker-0，验证其他 worker 偷取

### UnevenLoadStealing
1000 tasks × 200μs 全推 worker-0，验证负载不均时偷取均衡

## 结果

| 工作负载 | 1核 | 2核 | 4核 | 8核 |
|---------|-----|-----|-----|-----|
| 0μs | 835K | 1.59M | 1.02M | 1.03M |
| 10μs | 95K | 182K | 358K | 702K |
| 50μs | 19.7K | 39.5K | 79K | 157K |
| 100μs | 9.9K | 19.9K | 39.8K | 79.3K |

- 0μs 时吞吐受 MPMC ring 的 CAS 提交限制
- 10-100μs 计算负载：近完美线性扩展（每倍增 ~1.85-2.0x）
- NUMA-aware 偷取仅在同节点（2 worker per NUMA node），跨节点不偷

## 偷取验证

```
1000 tasks × 200μs → worker-0 only:
  worker-0: 500 (自消费)
  worker-1: 500 (偷取)
  worker-2: 0   (不同 NUMA 节点)
  worker-3: 0   (不同 NUMA 节点)
```

## 运行

```bash
./tests/stress/test_work_stealing_bench
```
