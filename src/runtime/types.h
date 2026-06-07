#pragma once
#include <cstdint>

namespace storage::runtime {

enum class QueueType : uint8_t {
    kLocal,       // 非原子本地队列 (worker-only)
    kAffine,      // 协程恢复亲和队列
    kNetIO,       // 网络IO完成
    kDiskIO,      // 磁盘IO完成
    kEngine,      // 引擎操作
    kTimer,       // 定时器
    kBackground,  // 后台维护
};

enum class Priority : uint8_t {
    kCritical = 0,  // P0
    kHigh     = 1,  // P1
    kMedium   = 2,  // P2
    kLow      = 3,  // P3
};

enum class QueueSemantic : uint8_t {
    kLocal,  // 非原子，单线程访问
    kSPSC,   // 单生产者单消费者
    kMPSC,   // 多生产者单消费者
    kMPMC,   // 多生产者多消费者
};

}  // namespace storage::runtime
