#include <gtest/gtest.h>
#include "runtime/strict_priority_policy.h"
#include "runtime/weighted_fair_policy.h"
#include "runtime/round_robin_policy.h"
#include "runtime/policy_factory.h"

using namespace storage::runtime;

// ============================================================================
// StrictPriorityPolicy
// ============================================================================

TEST(StrictPriorityPolicyTest, P0Wins) {
    StrictPriorityPolicy policy(64);

    std::vector<QueueSnapshot> queues = {
        {QueueType::kEngine, Priority::kHigh,   5, 0, 0},   // P1
        {QueueType::kEngine, Priority::kCritical, 3, 0, 0}, // P0
        {QueueType::kEngine, Priority::kMedium,  2, 0, 0},  // P2
    };

    auto decision = policy.decide(queues, {});
    EXPECT_FALSE(decision.idle);
    EXPECT_EQ(decision.queue_index, 1u);  // P0 queue (index 1)
    EXPECT_EQ(decision.batch_size, 3u);   // min(approx_count=3, max_batch=64)
}

TEST(StrictPriorityPolicyTest, P1Degraded) {
    StrictPriorityPolicy policy(64);

    std::vector<QueueSnapshot> queues = {
        {QueueType::kEngine, Priority::kHigh,   5, 0, 0},   // P1 → chosen
        {QueueType::kEngine, Priority::kMedium,  2, 0, 0},  // P2
        {QueueType::kEngine, Priority::kLow,     1, 0, 0},  // P3
    };

    auto decision = policy.decide(queues, {});
    EXPECT_FALSE(decision.idle);
    EXPECT_EQ(decision.queue_index, 0u);  // P1 (highest available)
    EXPECT_EQ(decision.batch_size, 5u);
}

TEST(StrictPriorityPolicyTest, AllEmptyIdle) {
    StrictPriorityPolicy policy(64);

    std::vector<QueueSnapshot> queues = {
        {QueueType::kEngine, Priority::kCritical, 0, 0, 0},
        {QueueType::kEngine, Priority::kHigh,     0, 0, 0},
        {QueueType::kEngine, Priority::kMedium,   0, 0, 0},
    };

    auto decision = policy.decide(queues, {});
    EXPECT_TRUE(decision.idle);
    EXPECT_EQ(decision.batch_size, 0u);
}

TEST(StrictPriorityPolicyTest, BatchCap) {
    StrictPriorityPolicy policy(4);  // max_batch = 4

    std::vector<QueueSnapshot> queues = {
        {QueueType::kEngine, Priority::kCritical, 100, 0, 0},
    };

    auto decision = policy.decide(queues, {});
    EXPECT_FALSE(decision.idle);
    EXPECT_EQ(decision.queue_index, 0u);
    EXPECT_EQ(decision.batch_size, 4u);  // capped by max_batch
}

// ============================================================================
// WeightedFairPolicy
// ============================================================================

TEST(WeightedFairPolicyTest, FairDistribution) {
    // Equal weights → roughly even distribution
    WeightedFairPolicy policy({}, 64);

    std::vector<QueueSnapshot> queues = {
        {QueueType::kEngine, Priority::kCritical, 10, 0, 0},
        {QueueType::kEngine, Priority::kHigh,     10, 0, 0},
    };

    // First decision: both have virtual time 0, pick first non-empty (idx 0)
    auto d1 = policy.decide(queues, {});
    EXPECT_FALSE(d1.idle);
    EXPECT_EQ(d1.queue_index, 0u);
    EXPECT_EQ(d1.batch_size, 10u);  // all 10 from idx 0

    // Now virtual_time[0] = 10/1.0 = 10, virtual_time[1] = 0
    // Second decision: pick idx 1 (lower virtual time)
    auto d2 = policy.decide(queues, {});
    EXPECT_FALSE(d2.idle);
    EXPECT_EQ(d2.queue_index, 1u);
    EXPECT_EQ(d2.batch_size, 10u);
}

TEST(WeightedFairPolicyTest, WeightImpact) {
    // Queue 0 has weight 2.0 (gets more), Queue 1 has weight 1.0
    WeightedFairPolicy policy({2.0, 1.0}, 64);

    std::vector<QueueSnapshot> queues = {
        {QueueType::kEngine, Priority::kCritical, 10, 0, 0},
        {QueueType::kEngine, Priority::kHigh,     10, 0, 0},
    };

    // First: both vt=0, pick idx 0
    auto d1 = policy.decide(queues, {});
    EXPECT_EQ(d1.queue_index, 0u);
    EXPECT_EQ(d1.batch_size, 10u);
    // vt[0] = 10/2.0 = 5.0

    // Second: vt[0]=5.0, vt[1]=0 → pick idx 1
    auto d2 = policy.decide(queues, {});
    EXPECT_EQ(d2.queue_index, 1u);
    EXPECT_EQ(d2.batch_size, 10u);
    // vt[1] = 10/1.0 = 10.0

    // Third: vt[0]=5.0, vt[1]=10.0 → pick idx 0 again
    auto d3 = policy.decide(queues, {});
    EXPECT_EQ(d3.queue_index, 0u);

    // Verify queue 0 was picked more often (higher weight)
    // After d1: vt[0]=5.0, vt[1]=0
    // After d2: vt[0]=5.0, vt[1]=10.0
    // After d3: vt[0]=5.0+10/2=10.0, vt[1]=10.0 → equal
    // After d4: pick idx 0 or 1 (whichever is lower, they're equal → idx 0)
}

TEST(WeightedFairPolicyTest, AllEmptyIdle) {
    WeightedFairPolicy policy({}, 64);

    std::vector<QueueSnapshot> queues = {
        {QueueType::kEngine, Priority::kCritical, 0, 0, 0},
        {QueueType::kEngine, Priority::kMedium,   0, 0, 0},
    };

    auto decision = policy.decide(queues, {});
    EXPECT_TRUE(decision.idle);
}

// ============================================================================
// RoundRobinPolicy
// ============================================================================

TEST(RoundRobinPolicyTest, RoundRobin) {
    RoundRobinPolicy policy(64);

    std::vector<QueueSnapshot> queues = {
        {QueueType::kEngine, Priority::kCritical, 5, 0, 0},
        {QueueType::kEngine, Priority::kHigh,     3, 0, 0},
        {QueueType::kEngine, Priority::kMedium,   2, 0, 0},
    };

    // Round 1: last_index_=0, searching from (0+1)%3=1 → picks queue 1
    auto d1 = policy.decide(queues, {});
    EXPECT_EQ(d1.queue_index, 1u);
    EXPECT_FALSE(d1.idle);

    // Round 2: last_index_=1, searching from (1+1)%3=2 → picks queue 2
    auto d2 = policy.decide(queues, {});
    EXPECT_EQ(d2.queue_index, 2u);

    // Round 3: last_index_=2, searching from (2+1)%3=0 → picks queue 0
    auto d3 = policy.decide(queues, {});
    EXPECT_EQ(d3.queue_index, 0u);

    // Round 4: last_index_=0, searching from (0+1)%3=1 → picks queue 1
    auto d4 = policy.decide(queues, {});
    EXPECT_EQ(d4.queue_index, 1u);
}

TEST(RoundRobinPolicyTest, SkipEmptyQueues) {
    RoundRobinPolicy policy(64);

    std::vector<QueueSnapshot> queues = {
        {QueueType::kEngine, Priority::kHigh,   0, 0, 0},  // empty
        {QueueType::kEngine, Priority::kCritical, 5, 0, 0},
        {QueueType::kEngine, Priority::kMedium, 0, 0, 0},  // empty
        {QueueType::kEngine, Priority::kLow,    3, 0, 0},
    };

    // Start: last_index=0. Search from 1 → idx 1 has items
    auto d1 = policy.decide(queues, {});
    EXPECT_EQ(d1.queue_index, 1u);
    EXPECT_FALSE(d1.idle);

    // last_index=1. Search from 2,3,0 → idx 0 empty, idx 1 is last=skip, idx 2 empty, idx 3 has items
    auto d2 = policy.decide(queues, {});
    EXPECT_EQ(d2.queue_index, 3u);

    // last_index=3. Search from 0→empty, 1→has items
    auto d3 = policy.decide(queues, {});
    EXPECT_EQ(d3.queue_index, 1u);
}

TEST(RoundRobinPolicyTest, AllEmptyIdle) {
    RoundRobinPolicy policy(64);

    std::vector<QueueSnapshot> queues = {
        {QueueType::kEngine, Priority::kCritical, 0, 0, 0},
        {QueueType::kEngine, Priority::kHigh,     0, 0, 0},
    };

    auto decision = policy.decide(queues, {});
    EXPECT_TRUE(decision.idle);
}

TEST(RoundRobinPolicyTest, SingleQueue) {
    RoundRobinPolicy policy(64);

    std::vector<QueueSnapshot> queues = {
        {QueueType::kEngine, Priority::kCritical, 7, 0, 0},
    };

    auto d1 = policy.decide(queues, {});
    EXPECT_EQ(d1.queue_index, 0u);
    EXPECT_EQ(d1.batch_size, 7u);
    EXPECT_FALSE(d1.idle);

    // Still one queue
    auto d2 = policy.decide(queues, {});
    EXPECT_EQ(d2.queue_index, 0u);
}

TEST(RoundRobinPolicyTest, NoQueues) {
    RoundRobinPolicy policy(64);
    std::vector<QueueSnapshot> queues;
    auto decision = policy.decide(queues, {});
    EXPECT_TRUE(decision.idle);
}

// ============================================================================
// PolicyFactory
// ============================================================================

TEST(PolicyFactoryTest, CreateStrictPriority) {
    PolicyConfig cfg;
    cfg.name = "strict_priority";
    cfg.max_batch = 16;
    auto policy = make_policy(cfg);
    EXPECT_EQ(policy->name(), "strict_priority");
}

TEST(PolicyFactoryTest, CreateWeightedFair) {
    PolicyConfig cfg;
    cfg.name = "weighted_fair";
    cfg.weights = {1.0, 2.0};
    cfg.max_batch = 32;
    auto policy = make_policy(cfg);
    EXPECT_EQ(policy->name(), "weighted_fair");
}

TEST(PolicyFactoryTest, CreateRoundRobin) {
    PolicyConfig cfg;
    cfg.name = "round_robin";
    cfg.max_batch = 8;
    auto policy = make_policy(cfg);
    EXPECT_EQ(policy->name(), "round_robin");
}

TEST(PolicyFactoryTest, UnknownNameThrows) {
    PolicyConfig cfg;
    cfg.name = "nonexistent_policy";
    EXPECT_THROW(make_policy(cfg), std::runtime_error);
}
