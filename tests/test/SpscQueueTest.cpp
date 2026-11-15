#include "../../src/base/Define.hpp"

#include <nexusflow/common/ConcurrentQueue.hpp>
#include <nexusflow/common/SpscQueue.hpp>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <type_traits>
#include <utility>

namespace {

using namespace nexusflow;

TEST(SpscQueueTest, FifoOrderIsPreserved) {
    SpscQueue<int> queue(4);
    ASSERT_TRUE(queue.push(1));
    ASSERT_TRUE(queue.push(2));
    ASSERT_TRUE(queue.push(3));
    EXPECT_EQ(queue.getSize(), 3u);
    int value = 0;
    ASSERT_TRUE(queue.tryPop(value));
    EXPECT_EQ(value, 1);
    ASSERT_TRUE(queue.tryPop(value));
    EXPECT_EQ(value, 2);
    ASSERT_TRUE(queue.tryPop(value));
    EXPECT_EQ(value, 3);
    EXPECT_TRUE(queue.isEmpty());
}

TEST(SpscQueueTest, BoundedQueueBlocksProducerUntilSpaceFrees) {
    SpscQueue<int> queue(1);
    ASSERT_TRUE(queue.push(1));

    std::atomic<bool> secondPushDone{false};
    std::thread producer([&queue, &secondPushDone]() {
        queue.push(2);
        secondPushDone.store(true, std::memory_order_relaxed);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    EXPECT_FALSE(secondPushDone.load(std::memory_order_relaxed));

    int value = 0;
    ASSERT_TRUE(queue.waitAndPop(value));
    EXPECT_EQ(value, 1);

    producer.join();
    EXPECT_TRUE(secondPushDone.load(std::memory_order_relaxed));
    ASSERT_TRUE(queue.waitAndPop(value));
    EXPECT_EQ(value, 2);
}

TEST(SpscQueueTest, TryPushRejectsFullQueue) {
    SpscQueue<int> queue(2);
    EXPECT_TRUE(queue.tryPush(1));
    EXPECT_TRUE(queue.tryPush(2));
    EXPECT_FALSE(queue.tryPush(3));
}

TEST(SpscQueueTest, TryPopOnEmptyReturnsEmptyOptional) {
    SpscQueue<int> queue(2);
    Optional<int> item = queue.tryPop();
    EXPECT_FALSE(item.hasValue());
}

TEST(SpscQueueTest, PushForTimesOutWhenQueueStaysFull) {
    SpscQueue<int> queue(1);
    ASSERT_TRUE(queue.push(1));
    EXPECT_FALSE(queue.pushFor(2, std::chrono::milliseconds(20)));
}

TEST(SpscQueueTest, ShutdownWakesBlockedConsumer) {
    SpscQueue<int> queue(2);
    std::atomic<bool> popped{true};
    std::thread consumer([&queue, &popped]() {
        int value = 0;
        popped.store(queue.waitAndPop(value), std::memory_order_relaxed);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    queue.shutdown();
    consumer.join();
    EXPECT_FALSE(popped.load(std::memory_order_relaxed));
}

TEST(SpscQueueTest, ShutdownWakesBlockedProducer) {
    SpscQueue<int> queue(1);
    ASSERT_TRUE(queue.push(1));

    std::atomic<bool> pushed{true};
    std::thread producer([&queue, &pushed]() { pushed.store(queue.push(2), std::memory_order_relaxed); });

    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    queue.shutdown();
    producer.join();
    EXPECT_FALSE(pushed.load(std::memory_order_relaxed));

    // Items queued before shutdown are still drained.
    int value = 0;
    ASSERT_TRUE(queue.tryPop(value));
    EXPECT_EQ(value, 1);
}

TEST(SpscQueueTest, StatsTrackCountersAndCapacity) {
    SpscQueue<int> queue(3);
    ASSERT_TRUE(queue.push(1));
    ASSERT_TRUE(queue.push(2));
    ASSERT_TRUE(queue.push(3));
    int value = 0;
    ASSERT_TRUE(queue.tryPop(value));

    const SpscQueue<int>::QueueCounters stats = queue.GetStats();
    EXPECT_EQ(stats.size, 2u);
    EXPECT_EQ(stats.capacity, 3u);
    EXPECT_EQ(stats.highWaterMark, 3u);
    EXPECT_EQ(stats.pushed, 3u);
    EXPECT_EQ(stats.popped, 1u);
}

TEST(SpscQueueTest, UnboundedCapacityFallsBackToTheLockedQueue) {
    SpscQueue<int> queue(-1);
    EXPECT_FALSE(queue.hasLockFreeStorage());
    for (int i = 0; i < 100; ++i) {
        ASSERT_TRUE(queue.push(i));
    }
    for (int i = 0; i < 100; ++i) {
        int value = 0;
        ASSERT_TRUE(queue.tryPop(value));
        EXPECT_EQ(value, i);
    }
    const SpscQueue<int>::QueueCounters stats = queue.GetStats();
    EXPECT_EQ(stats.capacity, 0u);
    EXPECT_EQ(stats.size, 0u);
}

namespace {
struct MoveOnly {
    explicit MoveOnly(int initial) : value(initial) {}
    MoveOnly(MoveOnly&&)                 = default;
    MoveOnly& operator=(MoveOnly&&)      = default;
    MoveOnly(const MoveOnly&)            = delete;
    MoveOnly& operator=(const MoveOnly&) = delete;
    int value                            = 0;
};
}    // namespace

TEST(SpscQueueTest, MoveOnlyPayloadWorks) {
    SpscQueue<MoveOnly> queue(2);
    ASSERT_TRUE(queue.push(MoveOnly(7)));
    MoveOnly out(0);
    ASSERT_TRUE(queue.tryPop(out));
    EXPECT_EQ(out.value, 7);
}

TEST(SpscQueueTest, MessageQueueUsesTheLockFreeStorage) {
    static_assert(std::is_same<MessageQueue, SpscQueue<Message>>::value, "graph edges should use the SPSC queue");
    MessageQueue queue(5);
    EXPECT_TRUE(queue.hasLockFreeStorage());
    EXPECT_EQ(queue.capacity(), 5);
}

}    // namespace
