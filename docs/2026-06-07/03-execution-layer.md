# 执行层设计：Online/Offline Worker Group

> 日期：2026-06-07
> 参考项目：`/home/wuledan/work/proj/quant_invest/cpp/quant/infra`（Chase-Lev Deque + Affinity 同步原语 + Worker-Stealing 调度器）

## 1. 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                      Execution Runtime                       │
├─────────────────────────────────────────────────────────────┤
│  ┌───────────────────────┐   ┌───────────────────────────┐  │
│  │   Online Worker Group  │   │   Offline Worker Group    │  │
│  │   (RTC / Shared-Nothing)│   │   (Worker-Stealing Pool) │  │
│  │                       │   │                           │  │
│  │  Worker-0  Worker-1   │   │  Worker-0  Worker-1 ...   │  │
│  │  ┌──────┐ ┌──────┐   │   │  ┌──────┐ ┌──────┐        │  │
│  │  │local │ │local │   │   │  │local │ │local │  steal  │  │
│  │  │deque │ │deque │   │   │  │deque │◄┼─►│deque │◄──────┼──│
│  │  └──────┘ └──────┘   │   │  └──────┘ └──────┘        │  │
│  │    (无窃取，隔离)      │   │    (跨线程窃取，负载均衡)   │  │
│  └───────────────────────┘   └───────────────────────────┘  │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              Affinity Synchronization Primitives       │   │
│  │   AffinityBaton / AffinityMutex / AffinitySharedMutex │   │
│  │          (所有协程恢复路由回原线程)                      │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌──────────────┐  ┌──────────────────┐                     │
│  │ Timer Engine │  │ IO Poller Pool   │                     │
│  │ (HHWheel)    │  │ (spdk/libaio/    │                     │
│  │              │  │  io_uring)       │                     │
│  └──────────────┘  └──────────────────┘                     │
└─────────────────────────────────────────────────────────────┘
```

## 2. Online Worker Group（RTC，Shared-Nothing）

### 2.1 核心原则

每个 Online Worker 是一个独立的执行单元，拥有自己的全部资源，遵循 Shared-Nothing：

- 独立的 Chase-Lev 本地队列（LIFO，仅 owner push/pop）
- 独立的 MPMC 亲和队列（接收跨线程协程恢复）
- 独立的 IO 提交/完成队列
- 独立的内存池（避免跨线程分配竞争）
- **不参与工作窃取**

### 2.2 任务获取顺序（优先级从高到低）

```
1. 亲和队列 (affine_queue)       — 跨线程的协程恢复，必须优先处理
2. IO 完成队列                     — IO 事件通知
3. 本地队列 (local_deque)         — 本线程产生的新任务
```

### 2.3 无窃取保证

- Online Worker 绝不从其他 worker 窃取任务
- 保证 Shared-Nothing 数据访问模式：每个分片的数据只被一个 worker 访问
- 上游 RPC 层通过请求的 key/分片 ID 做一致性哈希，将请求直接投递到目标 worker 的本地队列

### 2.4 CPU 绑定

- 独占物理核，隔离操作系统干扰
- Online 与 Offline 使用不同的 CPU set
- 每个 worker 绑定到一个物理 CPU core（跳过 HT 兄弟）

## 3. Offline Worker Group（Worker-Stealing）

### 3.1 核心原则

复用参考项目 quant/infra 的 worker-stealing 模型，处理长尾后台任务：

- 每个 worker 有 Chase-Lev 本地队列 + MPMC 亲和队列
- 空闲时从同 NUMA 节点的其他 worker 窃取任务（FIFO）
- 默认不窃取 Online Worker 的任务（物理隔离）

### 3.2 任务获取顺序

```
1. 亲和队列 (affine_queue)        — 协程恢复
2. 本地队列 (local_deque)         — 本地 push/pop (LIFO)
3. 全局提交队列 (global_queue)    — 外部提交
4. NUMA-local 工作窃取            — FIFO, lock-free
5. 退避: SPIN → YIELD → FUTEX PARK
```

### 3.3 自适应退避

| 阶段 | 机制 | 耗时 |
|------|------|------|
| SPIN | x86 `PAUSE` / ARM `YIELD` 指令 | ~100ns/次，最多 10μs |
| YIELD | `std::this_thread::yield()` | ~1-10μs/次，最多 3 轮 |
| PARK | `condition_variable::wait` + Futex | CPU ≈ 0%，直到被唤醒 |

**丢失唤醒防护**：park 前持有 mutex 重新检查亲和队列和全局队列。

## 4. Online vs Offline 对比

| 维度 | Online (RTC) | Offline |
|------|-------------|---------|
| **线程模型** | Shared-Nothing，每个 worker 独立 | Worker-Stealing，工作窃取 |
| **资源隔离** | 每个 worker 独占 IO 队列/内存池/连接 | 共享资源池，按需分配 |
| **任务获取** | 仅本地队列 + 亲和队列 | 本地队列 → NUMA 窃取 → 全局队列 |
| **负载均衡** | 无（上游通过一致性哈希/分片路由） | 有（空闲 worker 主动窃取） |
| **协程恢复** | 严格路由回原线程 | 允许窃取，但优先回原线程 |
| **CPU 绑定** | 独占物理核，隔离干扰 | 绑定但允许窃取 |
| **优先级** | 最高（实时响应） | 低于 Online |
| **典型负载** | RPC 读写请求、同步等待 | Compaction、EC 计算、GC |

## 5. 协程同步原语

从参考项目移植，保持线程亲和路由能力：

| 组件 | 作用 | 关键机制 |
|------|------|---------|
| **ChaseLevDeque** | 无锁工作窃取双端队列 | Owner LIFO push/pop, Thief FIFO steal，单次 CAS 竞争 |
| **AffinityBaton** | 协程信号量 | `co_await` 挂起记录 worker_id → `post()` 通过 `add_to_worker()` 路由回原线程 |
| **AffinityMutex** | 协程互斥锁 | 单原子变量编码锁+waiter栈，`unlock()` 时路由唤醒 |
| **AffinitySharedMutex** | 协程读写锁 | 写者优先防止饥饿，批量唤醒连续读者 |

### 5.1 线程亲和路由机制

```
co_await baton
    │
    ├─ await_suspend:  node.worker_id = current_worker_id()
    │                  CAS 入队到 waiter 链
    │
    ▼  ... 协程挂起 ...
    │
    ├─ baton.post(executor)
    │      └─ resume_chain:
    │            for each waiter:
    │              executor.add_to_worker(waiter.worker_id, handle.resume)
    │
    ▼
目标 worker: affine_queue.try_dequeue() → handle.resume()
    └─ 协程在原始 worker 线程上恢复执行
```

## 6. IO Poller 设计

```
Online Worker (RTC)              Offline Worker
      │                                │
      │ submit IO request              │ submit background IO
      ▼                                ▼
┌─────────────────┐          ┌─────────────────┐
│  IO Ring/Bdev   │          │  IO Ring/Bdev   │
│ (per-worker)    │          │ (shared pool)   │
│  io_uring SQ    │          │  io_uring SQ    │
└────────┬────────┘          └────────┬────────┘
         │ poll completion            │ poll completion
         ▼                            ▼
    completion callback          completion callback
    → 唤醒协程 (affine_queue)    → 唤醒协程 (affine_queue)
    → 原 worker 恢复执行          → 原 worker 恢复执行
```

- Online Worker 拥有独立的 io_uring/spdk 队列
- IO 完成回调通过 `add_to_worker(worker_id, handler)` 路由回原线程
- 不阻塞 worker 主循环 — 提交后立即 `co_await` 挂起，完成时恢复

## 7. 目录结构草案

```
src/
├── runtime/
│   ├── worker.h/cc              # Worker 基类
│   ├── chase_lev_deque.h        # 无锁双端队列
│   ├── online_worker.h/cc       # Online RTC Worker
│   ├── online_group.h/cc        # Online Worker 组管理
│   ├── offline_worker.h/cc      # Offline Worker (含窃取逻辑)
│   ├── offline_group.h/cc       # Offline Worker 组管理
│   ├── runtime.h/cc             # Runtime 总控（启动/停止）
│   ├── io_poller.h/cc           # IO 轮询抽象
│   ├── io_backend.h             # IO 后端接口 (spdk/libaio/io_uring)
│   └── sync/
│       ├── affinity_baton.h/cc
│       ├── affinity_mutex.h/cc
│       └── affinity_shared_mutex.h/cc
├── timer/
│   └── timer_scheduler.h/cc     # 定时器调度
└── memory/
    ├── object_pool.h/cc         # 对象池
    └── memory_pool.h/cc         # 分层内存池
```

## 8. 关键设计决策

1. **Online 与 Offline 物理隔离** — 两组 worker 使用不同的 CPU set，Online 独占核心避免竞争
2. **Online 无窃取** — 保证 Shared-Nothing 数据访问，请求通过 key 路由到固定 worker
3. **协程亲和路由** — 所有同步原语记录挂起时的 worker_id，恢复时通过 affine_queue 路由回原线程
4. **IO 路径零阻塞** — IO 提交→挂起→完成回调→恢复，全程无阻塞等待
5. **复用参考实现** — ChaseLevDeque、Affinity 同步原语、自适应退避等从 quant/infra 移植

## 9. 详细设计延续

Runtime 层的多队列模型、调度策略 API、自适应空闲控制详见：
→ [docs/2026-06-08/04-runtime-layer.md](../2026-06-08/04-runtime-layer.md)

## 10. 待深入设计

- [ ] 任务调度优先级策略（Online 任务抢占 Offline）
- [ ] Worker 数量的动态调整机制
- [ ] IO Poller 的具体实现（io_uring 事件循环集成）
- [ ] 对象池/内存池与 Worker 的绑定关系
- [ ] Worker 间消息传递的接口设计
