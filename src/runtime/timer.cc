#include "timer.h"
#include "online_worker.h"
#include "work_stealing_executor.h"

namespace storage::runtime {

void SleepAwaiter::await_suspend(std::coroutine_handle<> h) noexcept {
    node.handle = h;
    node.source_queue_idx = (tls_source_queue_idx != SIZE_MAX)
        ? tls_source_queue_idx : SIZE_MAX;

    auto* ow = current_online_worker();
    if (ow) {
        // Online: per-worker timer (no lock needed)
        ow->timer_state().insert(std::move(node));
        return;
    }

    // Offline: route to the WorkStealingExecutor's global timer wheel
    auto* exec = WorkStealingExecutor::current_executor();
    if (exec) {
        node.source_worker_id = exec->current_worker_id();
        exec->timer_wheel().insert(std::move(node));
        return;
    }

    // No known context — leak the coroutine handle (should not happen)
}

}  // namespace storage::runtime
