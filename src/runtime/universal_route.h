#pragma once
#include "worker_registry.h"
#include "affinity_baton.h"

namespace storage::runtime {
namespace adapt {

// Create a RouteFunc that routes through the global WorkerRegistry,
// supporting both online and offline workers transparently.
inline RouteFunc make_universal_route() {
    return [](size_t global_worker_id, std::coroutine_handle<> h) {
        auto* handle = WorkerRegistry::instance().get_handle(global_worker_id);
        if (handle) {
            handle->push_affine(WorkItem::make_coro(h));
        }
    };
}

}  // namespace adapt
}  // namespace storage::runtime
