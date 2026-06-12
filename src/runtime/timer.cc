#include "timer.h"
#include "online_worker.h"
#include "work_stealing_executor.h"
#include "worker_registry.h"
#include <thread>
#include <atomic>
#include <mutex>

namespace storage::runtime {

// ── Global offline timer ──
// Shared across all WorkStealingExecutors.
// Single timer thread that sleeps on a condition_variable until the next
// deadline, avoiding busy-wait.
//
// Uses a RAII wrapper so the thread is joined on static destruction,
// preventing std::terminate() from the std::thread destructor.
static OfflineTimerWheel g_offline_timer;
static std::once_flag g_timer_started;

namespace {
struct GlobalTimerThread {
    std::thread thread;
    std::atomic<bool> stop{false};

    ~GlobalTimerThread() {
        stop.store(true, std::memory_order_release);
        g_offline_timer.notify_one();  // wake timer thread so it sees stop
        if (thread.joinable()) thread.join();
    }
};
static GlobalTimerThread g_timer;
}  // anonymous namespace

void start_global_timer_thread() {
    std::call_once(g_timer_started, [] {
        g_timer.thread = std::thread([] {
            while (!g_timer.stop.load(std::memory_order_acquire)) {
                auto now = Clock::now();
                auto expired = g_offline_timer.expire(now);
                for (auto& node : expired) {
                    if (node.on_expire) {
                        node.on_expire();
                    } else {
                        auto* handle = WorkerRegistry::instance().get_handle(node.source_worker_id);
                        if (handle) handle->push_affine(WorkItem::make_coro(node.handle));
                    }
                }
                // Sleep precisely until the next deadline (or indefinitely if
                // no timers are pending).  insert() will notify_one() if a new
                // earlier deadline appears.
                g_offline_timer.sleep_until_next();
            }
        });
    });
}

void stop_global_timer_thread() {
    g_timer.stop.store(true, std::memory_order_release);
    g_offline_timer.notify_one();  // wake timer thread so it exits the loop
    if (g_timer.thread.joinable()) g_timer.thread.join();
}

OfflineTimerWheel& global_offline_timer() {
    return g_offline_timer;
}

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

    // Offline: route to the global offline timer wheel
    auto* exec = WorkStealingExecutor::current_executor();
    if (exec) {
        node.source_worker_id = WorkerRegistry::instance().get_global_id_for_offline(
            exec, exec->current_worker_id());
        global_offline_timer().insert(std::move(node));
        return;
    }

    // No known context — leak the coroutine handle (should not happen)
}

}  // namespace storage::runtime
