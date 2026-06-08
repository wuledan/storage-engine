# IO Adapter 层设计

> 日期：2026-06-08
> 约束：shared-nothing, C++20 协程, 零阻塞, per-worker IO 实例

## 1. 架构概览

```
┌───────────── OnlineWorker ──────────────────────┐
│                                                  │
│  Engine: co_await io_backend_->co_read(fd,...)  │
│       │                                          │
│       ▼  submit(req) → io_uring SQ              │
│       │  co_await baton (挂起协程)               │
│       │                                          │
│  Scheduler 调度循环:                              │
│    ├─ poll affine_queue (P0)                     │
│    ├─ poll net_io     (P1)                       │
│    ├─ poll engine     (P2)                       │
│    ├─ poll disk_io    (P2)                       │
│    └─ poll_io()  ←── 新增：poll IO completions   │
│         │                                        │
│         ▼                                        │
│    IO完成 → 回调 baton.post() → enqueue affine   │
│       │                                          │
│       ▼                                          │
│    Scheduler 下轮 poll affine → handle.resume()  │
│       │                                          │
│       ▼                                          │
│    co_await 返回 → Engine 拿到 IO 结果            │
│                                                  │
├──────────────────────────────────────────────────┤
│              IIOBackend (per-worker)              │
│  ┌─────────────────────────────────────────┐     │
│  │  virtual void submit(IORequest) = 0;     │     │
│  │  virtual size_t poll(IOCompletion*,N)=0; │     │
│  └─────────────────────────────────────────┘     │
│  ┌──────────┬───────────┬──────────┐            │
│  │ io_uring │  libaio   │   SPDK   │            │
│  └──────────┴───────────┴──────────┘            │
└──────────────────────────────────────────────────┘
```

## 2. IO 路径（协程亲和）

```
Engine 协程 (Worker 线程):
  future = io_backend_->co_read(fd, offset, buf, len)
    └─ req.callback = [baton](result) { baton.post(route); }
    └─ io_backend_->submit(req)                     ← 非阻塞
    └─ co_await *baton                              ← 挂起，记录 worker_id
        │
        ▼  ... 协程挂起中，Scheduler 继续调度循环 ...
        │
Scheduler::poll_io():
  n = io_backend_->poll(completions, 64)            ← 非阻塞 poll
  for each completion:
    completion.callback(completion)                  ← 调用 baton.post(route)
        │
        ▼  route → enqueue_affine(handle)
        │
Scheduler 下轮: poll affine_queue (P0)
  → deque → handle.resume()
  → co_await 返回 → Engine 拿到 IOCompletion.result
```

**零 futex，零系统调用在协程唤醒路径上。IO 完成 → 回调 → affine queue → resume，全链路无阻塞。**

## 3. 接口定义

### 3.1 IORequest / IOCompletion

```cpp
struct IORequest {
    enum Op : uint8_t { kRead, kWrite, kFlush, kTrim };

    Op op;
    int fd;             // 文件描述符
    uint64_t offset;    // 读写偏移
    void* buf;          // 缓冲区（必须对齐到 512）
    size_t len;         // 长度
    uint32_t trace_id;  // trace ID

    using Callback = std::function<void(struct IOCompletion)>;
    Callback callback;  // 完成回调
    // 内部：不要求回调可拷贝，必要时用 shared_ptr 包装
};

struct IOCompletion {
    uint64_t user_data;  // 请求标识（后端正交）
    int64_t result;      // 成功：字节数，失败：-errno
    IORequest::Callback callback; // 与请求相同
};
```

### 3.2 IIOBackend 抽象

```cpp
class IIOBackend {
public:
    virtual ~IIOBackend() = default;

    virtual std::string_view name() const noexcept = 0;

    // 提交 IO 请求（非阻塞，O(1)）
    virtual void submit(IORequest req) = 0;

    // 轮询完成事件（非阻塞，批量返回）
    // 返回实际完成数，0 ≤ n ≤ max
    virtual size_t poll(IOCompletion* out, size_t max) = 0;

    // 协程友好的 IO 操作（Engine 使用的 API）
    // 内部：submit + co_await AffinityBaton
    folly::coro::Task<IOCompletion> co_read(
        int fd, uint64_t offset, void* buf, size_t len);

    folly::coro::Task<IOCompletion> co_write(
        int fd, uint64_t offset, const void* buf, size_t len);

protected:
    // 创建 RouteFunc（子类或 IOEngine 注入）
    // Worker::make_route_func() 桥接
    using RouteFn = std::function<void(size_t, std::coroutine_handle<>)>;
    RouteFn route_fn_;

    // 协程 IO 的通用实现
    folly::coro::Task<IOCompletion> co_io(IORequest req);
};
```

### 3.3 IOEngine（工厂 + 生命周期）

```cpp
struct IOBackendConfig {
    std::string type;           // "io_uring" | "libaio" | "spdk"
    size_t queue_depth{256};    // 提交队列深度
    size_t max_file_size{0};    // SPDK: bdev 大小
    std::string bdev_name;      // SPDK: block device name
};

class IOEngine {
public:
    static std::unique_ptr<IIOBackend> create(
        const IOBackendConfig& cfg,
        IIOBackend::RouteFn route_fn);
};
```

## 4. 后端实现要点

### 4.1 IOUringBackend

```cpp
class IOUringBackend : public IIOBackend {
public:
    explicit IOUringBackend(size_t queue_depth, RouteFn route);
    ~IOUringBackend() override;

    void submit(IORequest req) override;
    size_t poll(IOCompletion* out, size_t max) override;

private:
    struct io_uring ring_;
    // SQE 到 IORequest 的映射（通过 user_data 索引）
    std::array<IORequest, QueueDepth> pending_;
};
```

提交路径：
```
submit(req):
  sqe = io_uring_get_sqe(&ring_)
  io_uring_prep_rw(sqe, req.op, req.fd, req.offset, req.buf, req.len)
  sqe->user_data = allocated_index   // 用于关联 completion 和 request
  pending_[index] = req              // 保存回调
  io_uring_submit(&ring_)            // 非阻塞提交
```

轮询路径：
```
poll(out, max):
  n = 0
  while (cqe = io_uring_peek_cqe(&ring_)) and n < max:
    idx = cqe->user_data
    out[n].result = cqe->res
    out[n].callback = std::move(pending_[idx].callback)
    io_uring_cqe_seen(&ring_, cqe)
    n++
  return n
```

### 4.2 LibaioBackend

```cpp
class LibaioBackend : public IIOBackend {
public:
    explicit LibaioBackend(size_t queue_depth, RouteFn route);
    ~LibaioBackend() override;

    void submit(IORequest req) override;
    size_t poll(IOCompletion* out, size_t max) override;

private:
    io_context_t ctx_{0};
    std::array<IORequest, QueueDepth> pending_;
};
```

提交：`io_prep_pwritev / io_prep_preadv` + `io_submit`  
轮询：`io_getevents(ctx_, 0, max, events, nullptr)`（timeout=0，非阻塞）

### 4.3 SPDKBackend（骨架）

```cpp
class SPDKBackend : public IIOBackend {
    // 依赖 SPDK 初始化环境（NVMe probe、memory registration 等）
    // 第一版：接口声明 + 文档说明初始化前置条件
    // submit 和 poll 存根实现
};
```

## 5. Worker 集成

### 5.1 OnlineWorker 改造

```cpp
class OnlineWorker : public Worker {
    // 构造时根据 config 创建 IO backend
    IOBackendConfig io_cfg;
    std::unique_ptr<IIOBackend> io_backend_;

    // Scheduler 注册 poll_io 回调
    // 或在 scheduler 主循环中直接调用 io_backend_->poll()
};
```

### 5.2 Scheduler 改造

```cpp
void Scheduler::set_io_backend(IIOBackend* io) { io_backend_ = io; }

folly::coro::Task<void> Scheduler::run() {
    while (running_) {
        // ... existing queue polling ...
        
        // IO 轮询（在调度循环中）
        if (io_backend_) {
            IOCompletion comps[64];
            size_t n = io_backend_->poll(comps, 64);
            for (size_t i = 0; i < n; ++i) {
                // 调用完成回调（→ baton.post → enqueue_affine）
                comps[i].callback(comps[i]);
            }
        }
        
        // ... idle handling ...
    }
}
```

IO poll 放在**所有队列 poll 之后、idle 之前**。因为 IO 完成会触发 affine queue (P0) 的协程恢复，下一轮调度循环会优先处理。

## 6. 使用示例

```cpp
// Engine 中：
folly::coro::Task<ReadResult> Engine::read_page(uint64_t page_id) {
    void* buf = pool_->allocate(PAGE_SIZE);
    
    auto comp = co_await io_backend_->co_read(
        fd_, page_id * PAGE_SIZE, buf, PAGE_SIZE);
    
    if (comp.result < 0) {
        pool_->deallocate(buf);
        throw IoError(-comp.result);
    }
    
    co_return ReadResult{buf, static_cast<size_t>(comp.result)};
}
```

## 7. 实现计划

| Task | 内容 | 工时 | 依赖 |
|------|------|------|------|
| I1 | IORequest/IOCompletion 类型 | 1h | 无 |
| I2 | IIOBackend 接口 + IOEngine 工厂 | 2h | I1 |
| I3 | IOUringBackend 完整实现 | 4h | I2 |
| I4 | LibaioBackend 完整实现 | 3h | I2 |
| I5 | SPDKBackend 骨架 + 文档 | 1h | I2 |
| I6 | Worker/Scheduler 集成 + co_read/co_write | 3h | I2 |
| I7 | 测试：MockBackend + 协程 IO 全链路 | 3h | I6 |
| **总计** | **7 任务** | **17h** | |

## 8. 关键设计决策

| 决策 | 理由 |
|------|------|
| IO poll 在 Scheduler 循环内（非独立线程） | shared-nothing：每个 Worker 独占 IO 队列 |
| 完成回调 → AffinityBaton → affine queue | 保证协程恢复在原 Worker 线程 |
| io_uring 作为首选项，libaio 作为降级 | io_uring 性能更好（SQPOLL、零拷贝），libaio 兼容老内核 |
| SPDK 第一版仅骨架 | SPDK 需要完整初始化环境（hugepages、NVMe probe），后续单独集成 |
