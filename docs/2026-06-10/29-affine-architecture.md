# 亲和性队列架构 — Online/Offline 统一路由

## 决策

三类队列解耦亲和性与负载均衡：

| 队列 | 入队 | 可偷 | 并发 | 唤醒顺序 |
|------|------|------|------|---------|
| **affine_queue** | baton/mutex 唤醒、timer 到期、亲和性任务 | ❌ | MPSC | 最先 |
| **local_deque** | 自产任务、自 yield | ✅ (offline间) | 无锁 SP | 次之 |
| **global entry** | `add()` 外部提交 | — | MPMC ring | 再次 |

## 路由统一

`RouteFunc` 通过 `WorkerRegistry::get_by_id(global_id)` 查找目标 worker，调用 `affine_queue().push()`：

```
触发方                 等待方              RouteFunc 路由到
Online coro A          Online coro B       B 的 affine_queue
Online coro A          Offline coro D      D 的 affine_queue
Offline coro E         Online coro B       B 的 affine_queue
Timer 线程             任意 coro           原 worker 的 affine_queue
```

不再区分 Online/Offline——WorkerRegistry 分配全局唯一 ID。

## 调度循环差异

Online:  affine_queue → P0 → P1(IO) → P2(engine) → P4(timer)
Offline: affine_queue → local_deque → steal → park

## 实施任务

| 任务 | 描述 |
|------|------|
| T1: WorkerRegistry 统一 | register_offline_worker(), 全局 ID |
| T2: Offline 加 affine_queue | MPSC ring (RingWorkQueue) per worker |
| T3: RouteFunc 改全局 lookup | make_universal_route_func() |
| T4: local_deque 去锁 | 删除 push_pop_lock, 恢复 ChaseLev 无锁 |
| T5: add() 改 global MPMC | MPMCRing 替换直推 local_deque |
| T6: 跨组测试 | online↔online, online↔offline mutex/baton/timer/sem |
| T7: 压力测试 | 长时稳定运行 |

## 下一步方向

多级调度策略完善——自适应降级（spin→yield→park 的参数化配置、per-queue 优先级权重调整）。
