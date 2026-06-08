# 存储引擎统一错误码体系

> 日期：2026-06-08

## 1. 分段编码

| 段 | 范围 | 模块 | 说明 |
|------|------|------|------|
| 0x0001 | `0x0001xxxx` | **Generic** | 通用错误（InvalidArgument, Timeout 等） |
| 0x0010 | `0x0010xxxx` | **Runtime** | Worker, Scheduler, Queue, AdaptiveIdle |
| 0x0020 | `0x0020xxxx` | **IO** | IIOBackend, io_uring, libaio, SPDK |
| 0x0030 | `0x0030xxxx` | **RPC** | DPDK, RDMA 网络层 |
| 0x0040 | `0x0040xxxx` | **Engine** | 存储引擎（LSM, B+Tree 等） |
| 0x0050 | `0x0050xxxx` | **Consistency** | 一致性协议层 |

## 2. 错误码定义

```cpp
enum class StorageError : uint32_t {
    OK = 0,

    // ── Generic (0x0001xxxx) ──
    Unknown             = 0x00010001,
    InvalidArgument     = 0x00010002,
    Timeout             = 0x00010003,
    OutOfResource       = 0x00010004,
    NotInitialized      = 0x00010005,
    Cancelled           = 0x00010006,
    InternalError       = 0x00010007,
    NotImplemented      = 0x00010008,

    // ── Runtime (0x0010xxxx) ──
    WorkerStartFailed    = 0x00100001,
    WorkerAlreadyRunning = 0x00100002,
    WorkerStopTimeout    = 0x00100003,
    QueueFull            = 0x00100004,
    QueueOverflow        = 0x00100005,
    SchedulerStopped     = 0x00100006,
    PolicyInvalidName    = 0x00100007,
    MemoryPoolExhausted  = 0x00100008,
    CPUBindFailed        = 0x00100009,

    // ── IO (0x0020xxxx) ──
    IORingSetupFailed    = 0x00200001,
    IORingSubmitFailed   = 0x00200002,
    IOQueueFull          = 0x00200003,
    IOInvalidFD          = 0x00200004,
    IOBackendNotAvail    = 0x00200005,
    IOResultError        = 0x00200006,   // errno 编码在消息中
    IOAioSetupFailed     = 0x00200007,
    IOAioSubmitFailed    = 0x00200008,
    IOAioGetEventsFail   = 0x00200009,
    IOSpdkNotReady       = 0x0020000A,
    IOPollError          = 0x0020000B,

    // ── RPC (0x0030xxxx) ──
    RPCConnectionRefused  = 0x00300001,
    RPCConnectionTimeout  = 0x00300002,
    RPCProtocolError      = 0x00300003,

    // ── Engine (0x0040xxxx) ──
    EngineNotSupported    = 0x00400001,
    EngineCorruption      = 0x00400002,
};

// ErrorCode → std::error_code 适配
std::error_code make_error_code(StorageError e);

// ErrorCategory（定位到具体模块）
class StorageErrorCategory : public std::error_category {
public:
    const char* name() const noexcept override { return "storage-engine"; }
    std::string message(int code) const override;
    // 内部有一个数组，按段映射模块名
    // 0x0010 → "runtime", 0x0020 → "io", etc.
};
```

## 3. ErrorNode（链式追踪，定位到源码位置）

复用 `adapt/error_codes.h` 的 `ErrorNode`，保留 `std::source_location` 能力：

```cpp
// 使用方式：
return make_error(
    StorageError::IORingSetupFailed,
    "io_uring_queue_init failed: " + std::string(strerror(-ret)),
    std::source_location::current());
```

输出示例：
```
[io] IORingSetupFailed at io_uring_backend.cc:35
    message: io_uring_queue_init failed: Cannot allocate memory
```

## 4. Result<T>（强制错误检查）

```cpp
template<typename T>
class Result {
    T value_;
    std::unique_ptr<ErrorNode> error_;
public:
    bool ok() const noexcept;
    T& value();           // throws if !ok
    const ErrorNode* error() const noexcept;
    // map / and_then for monadic chaining
};
```

## 5. IO 层错误处理集成

### 5.1 IOUringBackend

```cpp
// 构造失败 → 异常或返回 Result
IOUringBackend::IOUringBackend(size_t queue_depth, RouteFn route) {
    // ...
    struct io_uring_params params{};
    int ret = io_uring_queue_init_params(queue_depth_, &ring_, &params);
    if (ret < 0) {
        throw std::runtime_error(
            "IORingSetupFailed: " + std::string(strerror(-ret)));
    }
}

void IOUringBackend::submit(IORequest req) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
        // 背压：队列满
        if (req.callback) {
            IOCompletion comp;
            comp.result = -EBUSY;
            comp.callback = std::move(req.callback);
            comp.callback(comp);
        }
        return;  // 不丢请求，已通过 callback 通知
    }
    // ...
}

size_t IOUringBackend::poll(IOCompletion* out, size_t max) {
    // ...
    int ret = io_uring_peek_cqe(&ring_, &cqe);
    if (ret == -EAGAIN) break;     // 正常：无完成事件
    if (ret < 0) {
        // 异常：记录日志并终止 poll
        break;
    }
    // ...
}
```

### 5.2 IIOBackend::co_read / co_write

```cpp
folly::coro::Task<IOCompletion> IIOBackend::co_read(...) {
    // ...
    co_await *baton;
    // 正常返回（result >= 0）或错误（result < 0 时 errno 编码在 result 中）
    co_return result;
}
```

上层 Engine 处理：
```cpp
auto comp = co_await io_backend_->co_read(fd, offset, buf, len);
if (comp.result < 0) {
    co_return make_error(StorageError::IOResultError,
        "read failed: " + std::string(strerror(-comp.result)),
        std::source_location::current());
}
```

## 6. 实现任务

| Task | 内容 | 工时 |
|------|------|------|
| E1 | `StorageError` 枚举 + `StorageErrorCategory` + `make_error_code` | 1h |
| E2 | IO 层 `submit/poll` 错误处理（满队列/构造失败/io_uring 错误） | 2h |
| E3 | 全系统 throw/return 迁移到 StorageError | 2h |
| E4 | 测试：错误码正确性 + ErrorNode 链式追踪 + Result<T> 语义 | 2h |
