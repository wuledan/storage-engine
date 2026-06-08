#include "dispatch_poller.h"
#include "work_item.h"
#include <chrono>

namespace storage::runtime {

DispatchPoller::DispatchPoller(const Config& cfg, DispatchFn dispatch)
    : cfg_(cfg), dispatch_fn_(std::move(dispatch)) {}

DispatchPoller::~DispatchPoller() { stop(); }

void DispatchPoller::start() {
    running_.store(true);
    thread_ = std::thread([this] { poll_loop(); });
    if (cfg_.cpu_id > 0) {
        // TODO: hwloc bind
    }
}

void DispatchPoller::stop() {
    running_.store(false);
    if (thread_.joinable()) thread_.join();
}

void DispatchPoller::poll_loop() {
    while (running_.load(std::memory_order_acquire)) {
        // TODO: poll NIC (future IO backend integration)
        // Stub: sleep + increment counter
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        packets_received_.fetch_add(1, std::memory_order_relaxed);
        // dispatch_fn_(key, item);
    }
}

}  // namespace storage::runtime
