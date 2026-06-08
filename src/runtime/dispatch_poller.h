#pragma once
#include "dispatch_types.h"
#include "work_item.h"
#include <thread>
#include <atomic>
#include <functional>
#include <cstdint>

namespace storage::runtime {

class OnlineWorkerGroup;

// ── DispatchPoller ──
//
// 网络轮询器骨架。从 NIC 接收包，通过 DispatchFn 投递到目标 worker。
// 实际网络 poll 逻辑后续集成 IO backend。
class DispatchPoller {
public:
    struct Config {
        uint32_t cpu_id{0};
        size_t poller_id{0};
    };

    using DispatchFn = std::function<void(uint64_t key, WorkItem item)>;

    explicit DispatchPoller(const Config& cfg, DispatchFn dispatch);
    ~DispatchPoller();

    void start();
    void stop();

    uint64_t packets_received() const { return packets_received_.load(); }
    uint64_t tasks_dispatched() const { return tasks_dispatched_.load(); }

private:
    void poll_loop();

    Config cfg_;
    DispatchFn dispatch_fn_;
    std::thread thread_;
    std::atomic<bool> running_{false};
    std::atomic<uint64_t> packets_received_{0};
    std::atomic<uint64_t> tasks_dispatched_{0};
};

}  // namespace storage::runtime
