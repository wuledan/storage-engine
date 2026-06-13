#include "io_engine.h"
#include "io_uring_backend.h"
#include "libaio_backend.h"
#include "spdk_backend.h"
#include <stdexcept>

namespace storage::io {

std::unique_ptr<IIOBackend> IOEngine::create(
    const IOBackendConfig& cfg,
    IIOBackend::RouteFn route_fn) {

    if (cfg.type == "io_uring") {
        return std::make_unique<IOUringBackend>(cfg, std::move(route_fn));
    }
    if (cfg.type == "libaio") {
        return std::make_unique<LibaioBackend>(cfg.queue_depth, std::move(route_fn));
    }
    if (cfg.type == "spdk") {
        return std::make_unique<SPDKBackend>(cfg, std::move(route_fn));
    }

    throw std::runtime_error("Unknown IO backend type: " + cfg.type);
}

}  // namespace storage::io
