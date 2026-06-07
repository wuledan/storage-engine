#include <gtest/gtest.h>
#include <coroutine>
#include "runtime/types.h"
#include "runtime/work_item.h"

using namespace storage::runtime;

// ============================================================================
// QueueType enum values
// ============================================================================
TEST(TypesTest, QueueTypeValues) {
    EXPECT_EQ(static_cast<uint8_t>(QueueType::kLocal), 0);
    EXPECT_EQ(static_cast<uint8_t>(QueueType::kAffine), 1);
    EXPECT_EQ(static_cast<uint8_t>(QueueType::kNetIO), 2);
    EXPECT_EQ(static_cast<uint8_t>(QueueType::kDiskIO), 3);
    EXPECT_EQ(static_cast<uint8_t>(QueueType::kEngine), 4);
    EXPECT_EQ(static_cast<uint8_t>(QueueType::kTimer), 5);
    EXPECT_EQ(static_cast<uint8_t>(QueueType::kBackground), 6);
}

// ============================================================================
// Priority enum values
// ============================================================================
TEST(TypesTest, PriorityValues) {
    EXPECT_EQ(static_cast<uint8_t>(Priority::kCritical), 0);
    EXPECT_EQ(static_cast<uint8_t>(Priority::kHigh), 1);
    EXPECT_EQ(static_cast<uint8_t>(Priority::kMedium), 2);
    EXPECT_EQ(static_cast<uint8_t>(Priority::kLow), 3);
}

// ============================================================================
// QueueSemantic enum values
// ============================================================================
TEST(TypesTest, QueueSemanticValues) {
    EXPECT_EQ(static_cast<uint8_t>(QueueSemantic::kLocal), 0);
    EXPECT_EQ(static_cast<uint8_t>(QueueSemantic::kSPSC), 1);
    EXPECT_EQ(static_cast<uint8_t>(QueueSemantic::kMPSC), 2);
    EXPECT_EQ(static_cast<uint8_t>(QueueSemantic::kMPMC), 3);
}

// ============================================================================
// WorkItem::make_func / execute (function path)
// ============================================================================
TEST(TypesTest, WorkItemMakeFunc) {
    int called = 0;
    auto fn = +[]() { /* no-op, just check call below */ };
    WorkItem item = WorkItem::make_func(fn);
    EXPECT_EQ(item.tag, 0);
    // Verify the function pointer was stored
    // We can't easily compare function pointers to lambdas, so verify via execute
    static int func_executed = 0;
    func_executed = 0;
    auto test_fn = +[]() { func_executed = 1; };
    WorkItem item2 = WorkItem::make_func(test_fn);
    item2.execute();
    EXPECT_EQ(func_executed, 1);
}

TEST(TypesTest, WorkItemExecuteFunc) {
    int counter = 0;
    auto fn = +[](void* ctx) { (*static_cast<int*>(ctx))++; };
    // Store state in a static for the C-linkage function
    static int* capture = nullptr;
    static int local = 0;
    local = 0;
    capture = &local;

    WorkItem item = WorkItem::make_func(+[]() {
        if (capture) ++(*capture);
    });
    item.execute();
    EXPECT_EQ(local, 1);
    capture = nullptr;
}

// ============================================================================
// WorkItem::make_coro / execute (coroutine path)
// ============================================================================
static bool g_coro_done = false;

struct TestCoro {
    struct promise_type {
        TestCoro get_return_object() noexcept {
            return TestCoro{
                std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept {
            g_coro_done = true;
            return {};
        }
        void return_void() noexcept {}
        void unhandled_exception() noexcept { std::terminate(); }
    };
    std::coroutine_handle<promise_type> handle;
};

static TestCoro make_test_coro() {
    co_return;
}

TEST(TypesTest, WorkItemMakeCoro) {
    g_coro_done = false;
    auto task = make_test_coro();
    WorkItem item = WorkItem::make_coro(task.handle);
    EXPECT_EQ(item.tag, 1);
    EXPECT_FALSE(g_coro_done) << "coroutine should not be done before resume";

    item.execute();
    EXPECT_TRUE(g_coro_done) << "coroutine should complete after execute";
}

TEST(TypesTest, WorkItemExecuteCoro) {
    g_coro_done = false;
    auto task = make_test_coro();
    WorkItem item = WorkItem::make_coro(task.handle);
    item.execute();
    EXPECT_TRUE(g_coro_done);
}

// ============================================================================
// WorkItem default construction
// ============================================================================
TEST(TypesTest, WorkItemDefault) {
    WorkItem item;
    EXPECT_EQ(item.func, nullptr);
    EXPECT_EQ(item.tag, 0);
    EXPECT_EQ(item.trace_id, 0u);
}
