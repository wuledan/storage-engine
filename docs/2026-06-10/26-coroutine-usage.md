# Coroutine Usage Guide

## 1. 快速开始 — 像用线程一样用协程

```cpp
#include "runtime/coro_api.h"
using namespace storage::runtime;

// 启动 Worker
Worker::Config cfg; cfg.cpu_id = 0;
auto* w = new OnlineWorker(cfg);
w->start();

// 创建协程 — 完全对齐 pthread_create
coro_thread thr([]() -> void* {
    printf("hello from coroutine\n");
    return nullptr;
});

// 等待完成 — 挂起调用协程而非自旋
void* result = co_await thr.join();

w->stop(); w->join(); delete w;
```

## 2. 协程基础原语

### yield — 让出 CPU，回到调度队列
```cpp
co_await yield();                    // 回到来源队列
co_await yield_to(QueueType::kDiskIO); // 回到指定队列
co_await yield_to(idx_disk_io_);    // 回到指定索引队列
```

### Baton — 协程间信号
```cpp
AffinityBaton baton;

// 等待方
co_await baton;           // 挂起直到 post

// 唤醒方 (IO callback 中)
baton.post(route_func);   // 通过 RouteFunc 路由回原 worker
```

### Mutex — 协程互斥锁
```cpp
AffinityMutex mtx;

co_await mtx.co_lock();          // 协程获取锁
auto guard = co_await mtx.co_scoped_lock(); // RAII
mtx.unlock();                    // 释放, 唤醒下一个等待者

// 非协程路径可用 std::lock_guard (仅单 worker 安全)
std::lock_guard<AffinityMutex> guard(mtx);
```

### Semaphore — 计数信号量
```cpp
AffinitySemaphore sem(4);   // 最多 4 并发

co_await sem.acquire();      // 获取许可, count=0时挂起
sem.release();              // 释放许可, 唤醒一个等待者
sem.try_acquire();          // 非阻塞
sem.available();            // 查询可用数
```

## 3. 数据生产与消费 — 完整流水线模式

```cpp
// 生产者协程: 提交 IO → 等待完成 → 生产下一批
static ProducerCoro producer(OnlineWorker* w, int fd, void* buf, int qd) {
    while (true) {
        AffinityBaton baton;
        std::atomic<size_t> pending{0};

        for (int q = 0; q < qd; ++q) {
            IORequest req{
                IORequest::kWrite, fd, offset, buf, 4096, 0,
                [&](IOCompletion c) {
                    if (pending.fetch_sub(1) == 1)
                        baton.post(w->make_route_func());
                }};
            pending.fetch_add(1);
            w->io_backend()->submit(std::move(req));
        }

        w->io_backend()->flush_submissions();
        co_await baton;  // 挂起, 所有 IO 完成后恢复
    }
}

// 启动生产者
w->submit_engine(WorkItem::make_func([] {
    auto coro = producer(w, fd, buf, 4);
    (void)coro;
}));
```

## 4. 协程调度模型

### Online Worker — 优先级队列调度

```
Scheduler 每轮迭代 (busy_poll, ~328ns):
  drain_p0()          ← P0: baton.post 唤醒的生产者协程
  drain disk_io       ← P1: IO 收割协程 (peek CQE → fire callback → baton.post)
  drain engine        ← P2: 业务协程 (producer, 新请求)
  drain net_io        ← P1: 网络 IO
  drain timer         ← P4: 定时器
```

### 执行顺序保证: P0(最先) → P1(IO收割) → P2(新请求) → ...

### Offline Worker — 工作偷取

```
Worker 主循环:
  1. 自己的 local_deque.pop()         ← LIFO, 最优局部性
  2. NUMA 同节点随机 victim.steal()    ← FIFO, CAS
  3. park (spin → yield → cv_wait)   ← 三级回退
```

## 5. coro_thread — 完整线程等价

```cpp
// 创建 + 传参
coro_thread thr([](int x, const char* s) -> void* {
    printf("%d %s\n", x, s);
    return (void*)(intptr_t)42;
}, 1, "hello");

// 移动语义
coro_thread t2 = std::move(thr);

// 等待
void* ret = co_await t2.join();  // ret = 42

// 分离
t2.detach();  // 自动清理, 不能再 join

// 查询
thr.joinable();      // 是否可 join
thr.get_id();        // 句柄
thr.native_handle(); // coro_t 原生句柄 (供 C API)

// 自省
coro_thread::current_id();  // 当前协程 ID (同 std::this_thread::get_id)
this_coro::get_id();        // 同上
this_coro::yield();         // co_await 让出 CPU
```

## 6. coro_t C API — 最小依赖接口

```cpp
coro_t c;
coro_create(&c, nullptr, my_func, arg);   // pthread_create
void* ret = co_await coro_join(c);        // co_await 版本
coro_detach(c);                            // 分离, 不 join
coro_yield();                              // 让出 (宏, 展开为 co_await yield())
```

## 7. 迁移对照表

| 多线程 | 协程框架 |
|--------|---------|
| `std::thread t(fn, args)` | `coro_thread thr(fn, args)` |
| `t.join()` | `co_await thr.join()` |
| `t.detach()` | `thr.detach()` |
| `std::mutex m; m.lock()` | `co_await mtx.co_lock()` |
| `std::unique_lock<M> lk(m)` | `auto g = co_await mtx.co_scoped_lock()` |
| `sem_wait(&sem)` | `co_await sem.acquire()` |
| `sem_post(&sem)` | `sem.release()` |
| `std::this_thread::yield()` | `co_await this_coro::yield()` |
| `condition_variable.wait(lk)` | `co_await baton` |
| `pread(fd, buf, len, off)` | `co_await w.co_read(fd, off, buf, len)` |

## 8. 禁止使用的 API

| 禁止 | 原因 | 替代 |
|------|------|------|
| `folly::coro::blockingWait` | 阻塞 Worker 线程 | `co_await` |
| `std::mutex` / `std::condition_variable` | 阻塞, 非协程友好 | `AffinityMutex` / `AffinityBaton` |
| `folly::coro::Task` (跨 worker) | 无 RouteFunc 路由 | `co_submit<R>` |
| `sleep()` / `usleep()` | 阻塞线程 | `co_await timer.co_sleep()` |
