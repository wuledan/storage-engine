#include <gtest/gtest.h>
#include <folly/Executor.h>

TEST(BuildTest, SanityCheck) {
    EXPECT_EQ(1, 1);
}

TEST(BuildTest, FollyLink) {
    EXPECT_TRUE(true);
}
