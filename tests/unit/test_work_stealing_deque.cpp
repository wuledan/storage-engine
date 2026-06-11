#include <gtest/gtest.h>
#include "runtime/work_stealing_deque.h"
#include <thread>
#include <vector>

using namespace storage::runtime;

TEST(WorkStealingDeque, PushPop) {
    WorkStealingDeque dq(8);
    for (int i = 0; i < 5; ++i) {
        WorkItem item;
        item.tag = i;
        dq.push(std::move(item));
    }
    EXPECT_EQ(dq.size(), 5);

    for (int i = 4; i >= 0; --i) {  // LIFO
        WorkItem out;
        ASSERT_TRUE(dq.pop(out));
        EXPECT_EQ((int)out.tag, i);
    }
    EXPECT_TRUE(dq.empty());
}

TEST(WorkStealingDeque, Steal) {
    WorkStealingDeque dq(8);
    for (int i = 0; i < 5; ++i) {
        WorkItem item; item.tag = i;
        dq.push(std::move(item));
    }

    WorkItem stolen;
    ASSERT_TRUE(dq.steal(stolen));
    EXPECT_EQ((int)stolen.tag, 0);  // FIFO — oldest first

    ASSERT_TRUE(dq.steal(stolen));
    EXPECT_EQ((int)stolen.tag, 1);
}

TEST(WorkStealingDeque, ConcurrentSteal) {
    WorkStealingDeque dq(64);
    for (int i = 0; i < 50; ++i) {
        WorkItem item; item.tag = i;
        dq.push(std::move(item));
    }

    std::atomic<int> stolen_count{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < 3; ++t) {
        threads.emplace_back([&] {
            for (int attempt = 0; attempt < 50; ++attempt) {
                WorkItem out;
                if (dq.steal(out)) {
                    stolen_count.fetch_add(1);
                }
            }
        });
    }
    for (auto& t : threads) t.join();

    EXPECT_LE(stolen_count.load(), 50);
    EXPECT_GT(stolen_count.load(), 0);
}
