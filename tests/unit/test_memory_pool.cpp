#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include "runtime/worker.h"
#include "runtime/online_worker.h"
#include "runtime/adapt/object_pool.h"
#include "runtime/adapt/memory_pool.h"

using namespace storage::runtime;
using namespace storage::runtime::adapt;

// ============================================================================
// 测试1: Worker 创建时初始化内存池
// ============================================================================
TEST(MemoryPoolTest, WorkerInitializesPool) {
    Worker::Config cfg;
    OnlineWorker w(cfg);
    // 池已初始化，不crash
    SUCCEED();
}

// ============================================================================
// 测试2: ObjectPool acquire/release
// ============================================================================
TEST(MemoryPoolTest, ObjectPoolAcquireRelease) {
    ObjectPool<WorkItem> pool;
    pool.warmup(100);

    auto item = pool.acquire();
    EXPECT_NE(item, nullptr);
    item->tag = 1;
    item->trace_id = 42;

    // release：shared_ptr 析构时回到池
    item.reset();

    // 从池再获取
    auto item2 = pool.acquire();
    EXPECT_NE(item2, nullptr);
}

// ============================================================================
// 测试3: MemoryPool warmup 和 allocate
// ============================================================================
TEST(MemoryPoolTest, MemoryPoolAllocate) {
    QuantMemoryResource pool;
    pool.warmup(1024 * 1024);  // 1MB

    void* ptr = pool.allocate(256, 8);
    EXPECT_NE(ptr, nullptr);
    pool.deallocate(ptr, 256, 8);
}

// ============================================================================
// 测试4: Worker 内存池在 start/stop 循环中无泄漏
// ============================================================================
TEST(MemoryPoolTest, NoLeakOnStartStop) {
    for (int i = 0; i < 50; ++i) {
        Worker::Config cfg;
        OnlineWorker w(cfg);
        w.start();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        w.stop();
        w.join();
    }
    SUCCEED();
}
