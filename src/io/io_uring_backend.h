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
    size_t poll(IOCompletion* out, size_t max) override;

private:
    size_t queue_depth_;
    struct io_uring ring_;
    std::vector<IORequest> pending_;  // 按 user_data 索引
    size_t submit_count_{0};          // 计数器用于 user_data
};

}  // namespace storage::io
