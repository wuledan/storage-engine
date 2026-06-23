#pragma once
#include "io_request.h"
#include <cstddef>
#include <string_view>
#include <memory>
#include <vector>

#include "runtime/coro_task.h"

namespace storage::runtime::adapt {
class AffinityBaton;
}  // namespace storage::runtime::adapt

namespace storage::io {
using runtime::adapt::Task;

class IIOBackend {
public:
    virtual ~IIOBackend() = default;

    // 后端名称
    virtual std::string_view name() const noexcept = 0;

    // 提交 IO 请求（非阻塞）
    virtual void submit(IORequest req) = 0;
    virtual void submit_batch(std::vector<IORequest> requests) {
        for (auto& req : requests) submit(std::move(req));
    }
    virtual void flush_pending() {}
    virtual size_t pending_count() const noexcept { return 0; }
    virtual void flush_submissions() {}  // 默认空操作
    virtual bool has_sqe_submitted() const { return false; }  // 是否有 SQE 已提交但 CQE 尚未处理
    virtual void flush_cqe_task_work() {}  // 触发内核 task_work 以推送 CQE 到环形缓冲区

    // 轮询 IO 完成事件（非阻塞，返回实际完成数）
    virtual size_t poll(IOCompletion* out, size_t max) = 0;

    // 协程友好的 IO API
    Task<IOCompletion> co_read(
        int fd, uint64_t offset, void* buf, size_t len);

    Task<IOCompletion> co_write(
        int fd, uint64_t offset, const void* buf, size_t len);
};

}  // namespace storage::io
