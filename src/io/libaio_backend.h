#pragma once
#include "io_backend.h"
#include <libaio.h>
#include <vector>
#include <cstring>

namespace storage::io {

class LibaioBackend : public IIOBackend {
public:
    explicit LibaioBackend(size_t queue_depth = 256, IIOBackend::RouteFn route = {});
    ~LibaioBackend() override;

    std::string_view name() const noexcept override { return "libaio"; }

    void submit(IORequest req) override;
    size_t poll(IOCompletion* out, size_t max) override;

private:
    size_t queue_depth_;
    io_context_t ctx_{0};
    std::vector<IORequest> pending_;
    std::vector<struct iocb> iocbs_;  // iocb 数组（每次 submit 构建）
    uint64_t submit_count_{0};
};

}  // namespace storage::io
