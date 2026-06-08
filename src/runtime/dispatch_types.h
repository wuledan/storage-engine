#pragma once
#include <cstdint>
#include <cstddef>
#include <string>
#include <vector>
#include <algorithm>
#include <unistd.h>

namespace storage::runtime {

// ── Dispatch strategy ──
enum class DispatchStrategy : uint8_t {
    kDirectAll,
    kDispatchSingle,
    kDispatchMultiple,
    kMixed,
};

// ── Dispatch plan ──
struct DispatchPlan {
    DispatchStrategy strategy{DispatchStrategy::kDirectAll};
    size_t direct_workers{0};
    size_t dispatch_threads{0};
    size_t consumers_per_dispatch{0};

    bool operator==(const DispatchPlan& o) const {
        return strategy == o.strategy && direct_workers == o.direct_workers
            && dispatch_threads == o.dispatch_threads
            && consumers_per_dispatch == o.consumers_per_dispatch;
    }
    bool operator!=(const DispatchPlan& o) const { return !(*this == o); }
};

// ── Hardware topology ──
struct HardwareTopology {
    size_t cpu_cores{0};
    size_t numa_nodes{0};
    size_t nic_queues{0};
    double nic_bandwidth_gbps{0};
    size_t disk_count{0};
    size_t disk_io_queues{0};
    double disk_bandwidth_gbps{0};
    double eng_ops_per_core{0};  // 应用层单核吞吐

    static HardwareTopology probe();  // 桩实现，后续集成各网络栈
};

// ── Bottleneck analysis ──
struct BottleneckAnalysis {
    enum Stage : uint8_t { kNet, kEngine, kDisk, kBalanced };
    double c_net{0};   // NIC 容量 (ops/s)
    double c_eng{0};   // CPU 容量
    double c_disk{0};  // 磁盘容量
    double c_total{0}; // 系统有效容量
    Stage bottleneck{Stage::kBalanced};
    double headroom{0}; // 余量比例

    Stage identify_bottleneck() {
        if (c_net < c_eng && c_net < c_disk) return kNet;
        if (c_eng < c_net && c_eng < c_disk) return kEngine;
        if (c_disk < c_net && c_disk < c_eng) return kDisk;
        return kBalanced;
    }

    static BottleneckAnalysis compute(double net, double eng, double disk);
};

inline BottleneckAnalysis BottleneckAnalysis::compute(double net, double eng, double disk) {
    BottleneckAnalysis ba;
    ba.c_net = net;
    ba.c_eng = eng;
    ba.c_disk = disk;
    ba.c_total = std::min({net, eng, disk});
    ba.bottleneck = ba.identify_bottleneck();
    // headroom = (capacity - bottleneck) / capacity
    double max_cap = std::max({net, eng, disk});
    ba.headroom = (max_cap > 0) ? (max_cap - ba.c_total) / max_cap : 0;
    return ba;
}

inline HardwareTopology HardwareTopology::probe() {
    HardwareTopology hw;
    hw.cpu_cores = static_cast<size_t>(sysconf(_SC_NPROCESSORS_ONLN));
    hw.numa_nodes = 1;   // 简化
    hw.nic_queues = 4;   // 简化
    hw.nic_bandwidth_gbps = 10.0;
    hw.disk_count = 1;
    hw.disk_io_queues = 1;
    hw.disk_bandwidth_gbps = 0.5;
    hw.eng_ops_per_core = 50000.0;  // 应用层每核 50K ops/s
    return hw;
}

}  // namespace storage::runtime
