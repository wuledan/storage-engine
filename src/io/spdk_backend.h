#pragma once
#include "io_backend.h"
#include "io_engine.h"
#include <cstddef>

namespace storage::io {

// SPDK 后端骨架
// 需要: DPDK + SPDK 初始化环境 (hugepages, NVMe probe, memory registration)
// 第一版只提供接口定义，submit/poll 返回空操作
class SPDKBackend : public IIOBackend {
public:
    explicit SPDKBackend(const IOBackendConfig& cfg, IIOBackend::RouteFn route = {});
    ~SPDKBackend() override;

    std::string_view name() const noexcept override { return "spdk"; }

    void submit(IORequest req) override;
    size_t poll(IOCompletion* out, size_t max) override;

private:
    IOBackendConfig cfg_;
};

}  // namespace storage::io
