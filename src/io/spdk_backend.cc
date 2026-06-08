#include "spdk_backend.h"
#include <cerrno>
#include <stdexcept>

namespace storage::io {

SPDKBackend::SPDKBackend(const IOBackendConfig& cfg, IIOBackend::RouteFn route)
    : cfg_(cfg) {
    set_route_fn(std::move(route));
    // SPDK 初始化需要:
    // 1. spdk_env_opts_init + spdk_env_init (hugepages)
    // 2. spdk_nvme_probe (枚举 NVMe 设备)
    // 3. spdk_nvme_ctrlr_alloc_io_qpair
    // 第一版：骨架，不做实际初始化
}

SPDKBackend::~SPDKBackend() {
    // spdk_nvme_detach
}

void SPDKBackend::submit(IORequest req) {
    // TODO: spdk_nvme_ns_cmd_read / spdk_nvme_ns_cmd_write
    // 通过 callback 返回 -ENOSYS
    if (req.callback) {
        IOCompletion comp;
        comp.result = -ENOSYS;  // Function not implemented
        comp.user_data = 0;
        comp.callback = req.callback;
        req.callback = nullptr;
        comp.callback(comp);
    }
}

size_t SPDKBackend::poll(IOCompletion* out, size_t max) {
    // TODO: spdk_nvme_qpair_process_completions
    return 0;
}

}  // namespace storage::io
