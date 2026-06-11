#include <gtest/gtest.h>
#include <folly/coro/Task.h>
#include <folly/coro/BlockingWait.h>
#include "runtime/online_worker.h"
#include <thread>

using namespace storage::runtime;

TEST(CoSubmit, ReturnsValue) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    OnlineWorker w(cfg);
    w.start();

    auto task = w.co_submit<int>([] { return 42; });
    int result = folly::coro::blockingWait(std::move(task));
    EXPECT_EQ(result, 42);

    w.stop();
    w.join();
}

TEST(CoSubmit, ReturnsVoid) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    OnlineWorker w(cfg);
    w.start();

    bool called = false;
    auto task = w.co_submit<void>([&called] { called = true; });
    folly::coro::blockingWait(std::move(task));
    EXPECT_TRUE(called);

    w.stop();
    w.join();
}

TEST(CoSubmit, PropagatesException) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    OnlineWorker w(cfg);
    w.start();

    auto task = w.co_submit<int>([]() -> int {
        throw std::runtime_error("test error");
    });
    EXPECT_THROW(folly::coro::blockingWait(std::move(task)), std::runtime_error);

    w.stop();
    w.join();
}
