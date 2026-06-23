#include <gtest/gtest.h>
#include "runtime/coro_task.h"
#include "runtime/online_worker.h"

using namespace storage::runtime;

TEST(CoSubmit, CaptureLambda) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    OnlineWorker w(cfg);
    w.start();

    int val = 42;
    auto task = w.co_submit<int>([&val] { return val; });
    int result = adapt::blockingWait(std::move(task));
    EXPECT_EQ(result, 42);

    w.stop();
    w.join();
}
