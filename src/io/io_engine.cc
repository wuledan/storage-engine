#include "io_engine.h"
#include "io_uring_backend.h"
#include <stdexcept>

namespace storage::io {

std::unique_ptr<IIOBackend> IOEngine::create(
    const IOBackendConfig& cfg,
    IIOBackend::RouteFn route_fn) {

    if (cfg.type == "io_uring") {
        return std::make_unique<IOUringBackend>(cfg.queue_depth, std::move(route_fn));
    }
    if (cfg.type == "libaio") {
        // TODO: implement LibaioBackend
        throw std::runtime_error("libaio backend not yet implemented");
    }
    if (cfg.type == "spdk") {
        // TODO: implement SPDKBackend
        throw std::runtime_error("SPDK backend not yet implemented");
    }

    throw std::runtime_error("Unknown IO backend type: " + cfg.type);
}

}  // namespace storage::io
