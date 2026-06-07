#include <gtest/gtest.h>
#include <thread>
#include <atomic>
#include <chrono>
#include "runtime/adaptive_idle.h"

using namespace storage::runtime;

// AdaptiveIdle::enter_idle() advances one phase per call:
//   1st call (kActive) → spin phase (~10μs), sets kYield, returns
//   2nd call (kYield)  → yield phase (~a few μs), sets kPark, returns
//   3rd call (kPark)   → park phase, blocks on condition_variable until notify
//
// Tests that verify park/wake must call enter_idle three times.

// ============================================================================
// enter_idle transitions correctly through phases
// ============================================================================
TEST(AdaptiveIdleTest, TransitionsThroughPhases) {
    AdaptiveIdle idle;
    EXPECT_EQ(idle.current_level(), IdleLevel::kActive);

    // Phase 1: spin
    idle.enter_idle();
    EXPECT_EQ(idle.current_level(), IdleLevel::kYield);

    // Phase 2: yield
    idle.enter_idle();
    EXPECT_EQ(idle.current_level(), IdleLevel::kPark);

    // Phase 3: park (blocks until notify)
    // We'll test this in NotifyWakeupPark
}

// ============================================================================
// notify wakes up a parked worker
// ============================================================================
TEST(AdaptiveIdleTest, NotifyWakeupPark) {
    AdaptiveIdle idle;
    std::atomic<bool> woken{false};
    std::atomic<bool> started_park{false};

    std::thread worker([&]() {
        idle.enter_idle();  // kActive → spin → kYield
        idle.enter_idle();  // kYield → yield → kPark
        started_park = true;
        idle.enter_idle();  // kPark → park (blocks)
        woken = true;
    });

    // Wait for the worker to reach parked state
    while (!started_park.load()) {
        std::this_thread::yield();
    }
    // Small delay to ensure park has started
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Worker should be blocked in park
    EXPECT_FALSE(woken.load());

    // Notify wakes the worker
    idle.notify();

    worker.join();
    EXPECT_TRUE(woken.load());
}

// ============================================================================
// enter_idle returns after notify (pre-notified eventfd)
// ============================================================================
TEST(AdaptiveIdleTest, EnterIdleReturnsAfterNotify) {
    AdaptiveIdle idle;
    std::atomic<bool> exited{false};

    std::thread worker([&]() {
        // First two calls advance to park state
        idle.enter_idle();
        idle.enter_idle();
        // Third call blocks in park
        idle.enter_idle();
        exited = true;
    });

    // Give worker time to reach park
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Notify should wake the worker
    idle.notify();

    worker.join();
    EXPECT_TRUE(exited.load());
}

// ============================================================================
// Multiple notify calls are safe
// ============================================================================
TEST(AdaptiveIdleTest, MultipleNotifySafe) {
    AdaptiveIdle idle;

    // Calling notify before anyone is waiting should be safe (no crash)
    idle.notify();
    idle.notify();
    idle.notify();

    // Verify we can still use the idle normally: start a worker that parks,
    // then notify wakes it up. The pre-existing eventfd data from the three
    // notifies gets consumed by the spin/yield phase reads.
    std::thread worker([&]() {
        idle.enter_idle();  // spin phase: reads eventfd (consumes accumulated value)
        idle.enter_idle();  // yield phase: no eventfd data → sets kPark
        idle.enter_idle();  // park phase: blocks on CV until notified
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    idle.notify();  // wake the worker from park
    worker.join();
}

// ============================================================================
// current_level tracking
// ============================================================================
TEST(AdaptiveIdleTest, CurrentLevelInitiallyActive) {
    AdaptiveIdle idle;
    EXPECT_EQ(idle.current_level(), IdleLevel::kActive);
}

TEST(AdaptiveIdleTest, CurrentLevelAfterParkWake) {
    AdaptiveIdle idle;

    std::thread worker([&]() {
        idle.enter_idle();
        idle.enter_idle();
        idle.enter_idle();  // park (blocks until notify)
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    idle.notify();
    worker.join();

    // After enter_idle returns from park, level goes back to Active
    EXPECT_EQ(idle.current_level(), IdleLevel::kActive);
}

// ============================================================================
// notify_fd returns valid fd
// ============================================================================
TEST(AdaptiveIdleTest, NotifyFdIsValid) {
    AdaptiveIdle idle;
    int fd = idle.notify_fd();
    EXPECT_GE(fd, 0);
}
