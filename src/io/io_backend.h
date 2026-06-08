#pragma once
#include "io_request.h"
#include <cstddef>
#include <string_view>
#include <memory>
#include <functional>

#include <folly/coro/Task.h>

namespace storage::runtime::adapt {
class AffinityBaton;
}  // namespace storage::runtime::adapt

namespace storage::io {

class IIOBackend {
public:
    using RouteFn = std::function<void(size_t, std::coroutine_handle<>)>;

    virtual ~IIOBackend() = default;

    // 后端名称
    virtual std::string_view name() const noexcept = 0;

    // 提交 IO 请求（非阻塞）
    virtual void submit(IORequest req) = 0;
    virtual void flush_submissions() {}  // 默认空操作

    // 轮询 IO 完成事件（非阻塞，返回实际完成数）
    virtual size_t poll(IOCompletion* out, size_t max) = 0;

    // 设置协程恢复路由函数（Worker 注入）
    void set_route_fn(RouteFn fn) { route_fn_ = std::move(fn); }

    // 协程友好的 IO API
    folly::coro::Task<IOCompletion> co_read(
        int fd, uint64_t offset, void* buf, size_t len);

    folly::coro::Task<IOCompletion> co_write(
        int fd, uint64_t offset, const void* buf, size_t len);

protected:
    RouteFn route_fn_;
};

}  // namespace storage::io
