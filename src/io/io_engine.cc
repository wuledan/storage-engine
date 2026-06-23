#include "io_engine.h"
#include "io_uring_backend.h"
#include "libaio_backend.h"
#include "spdk_backend.h"
#include <stdexcept>

namespace storage::io {

std::unique_ptr<IIOBackend> IOEngine::create(
    const IOBackendConfig& cfg) {

    if (cfg.type == "io_uring") {
        return std::make_unique<IOUringBackend>(cfg);
    }
    if (cfg.type == "libaio") {
        return std::make_unique<LibaioBackend>(cfg.queue_depth);
    }
    if (cfg.type == "spdk") {
        return std::make_unique<SPDKBackend>(cfg);
    }

    throw std::runtime_error("Unknown IO backend type: " + cfg.type);
}

}  // namespace storage::io
