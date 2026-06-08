// affinity_baton.cc -- Implementation of routed Baton
//
// The core mechanism:
//   post(route): atomically drain waiter chain, route each waiter's
//                continuation via the provided RouteFunc
//   post_direct():  drain and resume inline (no routing)

#include "cpp/quant/infra/affinity_baton.h"

namespace storage::runtime::adapt {

// ── Worker ID resolution ──
//
// WorkStealingExecutor sets this function pointer during start() to enable
// thread affinity. The default returns SIZE_MAX (no worker affinity),
// which causes post_direct()-style direct resume.

namespace {
    size_t default_worker_id() { return SIZE_MAX; }
}

namespace detail {
    size_t (*get_current_worker_id)() = default_worker_id;
}

size_t AffinityBaton::current_worker_id() {
    return detail::get_current_worker_id();
}

// ── post(route): routed resume ──

void AffinityBaton::post(RouteFunc route) {
    // Atomically swap waiters_ to kPostedBit (drain list + set posted).
    // Single atomic operation — no window between "drain" and "set state".
    auto* old = waiters_.exchange(
        reinterpret_cast<WaiterNode*>(kPostedBit),
        std::memory_order_acq_rel);

    resume_chain(clear_posted(old), &route);
}

// ── post_direct(): inline resume (no executor) ──

void AffinityBaton::post_direct() noexcept {
    auto* old = waiters_.exchange(
        reinterpret_cast<WaiterNode*>(kPostedBit),
        std::memory_order_acq_rel);

    resume_chain(clear_posted(old), nullptr);
}

// ── resume_chain: walk the linked list and resume each waiter ──

void AffinityBaton::resume_chain(WaiterNode* waiters,
                                  RouteFunc* route) {
    while (waiters) {
        auto* next = waiters->next;
        auto handle = waiters->handle;
        auto worker_id = waiters->worker_id;

        if (route && worker_id != SIZE_MAX) {
            // Route to the waiter's original worker via callback
            (*route)(worker_id, handle);
        } else {
            // No route or no affinity: resume inline
            handle.resume();
        }

        waiters = next;
    }
}

}  // namespace storage::runtime::adapt
