#pragma once
#include "io_backend.h"
#include <liburing.h>
#include <array>
#include <vector>
#include <cstring>

namespace storage::io {

class IOUringBackend : public IIOBackend {
public:
    explicit IOUringBackend(size_t queue_depth = 256, IIOBackend::RouteFn route = {});
    ~IOUringBackend() override;

    std::string_view name() const noexcept override { return "io_uring"; }

    void submit(IORequest req) override;
    void flush_submissions();  // 批量提交所有待处理 SQE
    size_t poll(IOCompletion* out, size_t max) override;

private:
    size_t queue_depth_;
    struct io_uring ring_;
    std::vector<IORequest> pending_;
    size_t submit_count_{0};
    size_t pending_sqe_count_{0};
};

}  // namespace storage::io
