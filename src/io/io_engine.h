#pragma once
#include "io_backend.h"
#include <string>
#include <memory>

namespace storage::io {

struct IOBackendConfig {
    std::string type{"io_uring"};    // "io_uring" | "libaio" | "spdk"
    size_t queue_depth{256};
    size_t max_file_size{0};         // SPDK bdev 大小
    std::string bdev_name;           // SPDK block device name
    int sq_poll_group{-1};           // -1 = own kernel thread (N=M), >=0 = shared group ID
};

class IOEngine {
public:
    static std::unique_ptr<IIOBackend> create(
        const IOBackendConfig& cfg,
        IIOBackend::RouteFn route_fn);
};

}  // namespace storage::io
