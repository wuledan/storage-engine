# Benchmark 统计对齐 fio

## 差异分析

| 指标 | fio | 当前我们 |
|------|-----|---------|
| **IOPS** | `total_ios / wall_clock_seconds` | `1/avg_latency × QD` (延迟推导) |
| **BW** | `total_bytes / wall_clock_seconds` | `IOPS × block_size` (依赖 IOPS) |
| **延迟** | clat/slat/lat 分离 | 仅 P50/P99 (rdtsc) |
| **百分位** | P1,P5,P10,...,P99.99 | 仅 P50,P99 |

## 改动方法

### 1. 新增真实 IOPS/BW

```cpp
// 在 while(done) 循环前记录 t_start
auto t_start = std::chrono::steady_clock::now();

// ... benchmark loop ...

auto t_end = std::chrono::steady_clock::now();
double wall_sec = std::chrono::duration<double>(t_end - t_start).count();

// 真实 IOPS (对齐 fio)
double real_iops = total_ops / wall_sec;
double real_bw   = total_ops * block_size / wall_sec;  // bytes/sec

// 延迟推导 IOPS (框架效率参考)
double lat_iops = 1e9 / avg_ns * qd;
```

### 2. 新增更多百分位

```cpp
std::sort(lats.begin(), lats.end());
size_t n = lats.size();

printf("  %-3d | %7.1f | %7.1f | %7.2f | %7.2f | %7.2f | %7.2f | %7.2f | %6.0f | %6.0f\n",
    qd,
    real_iops/1000.0,           // 真实 IOPS (K)
    lat_iops/1000.0,            // 延迟 IOPS (K)
    (lats[n/2]/ghz/1000.0),     // P50 (us)
    (lats[n*90/100]/ghz/1000.0),// P90
    (lats[n*99/100]/ghz/1000.0),// P99
    (lats[n*999/1000]/ghz/1000.0),// P999
    (lats[n*9999/10000]/ghz/1000.0),// P9999
    real_bw/1024/1024,          // 真实 BW (MB/s)
    real_bw/1024/1024/1024      // 真实 BW (GB/s)
);
```

### 3. 输出表头

```
  QD | RIOP(K) | LIOP(K) | P50(us) | P90(us) | P99(us) | P999    | P9999   | BW(MB/s)
  ---|---------|---------|---------|---------|---------|---------|---------|---------
```

- RIOP = Real IOPS (wall clock, 对齐 fio)
- LIOP = Latency IOPS (延迟推导, 框架效率)

### 4. 改动文件

`tests/stress/test_benchmark.cpp` 中 `CoroutinePipeline` 函数：
- 添加 `wall_sec` 计算
- 添加 `real_iops` / `real_bw`
- 扩展 printf 百分位到 P9999
- 更新表头

### 5. 预期效果

对齐后 RIOP 应接近 fio IOPS（wall clock），LIOP 作为框架 overhead 参考（应 > RIOP）。
