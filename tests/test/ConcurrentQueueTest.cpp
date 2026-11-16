#include <nexusflow/common/ConcurrentQueue.hpp>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

namespace {

using namespace std::chrono;

TEST(ConcurrentQueueTest, PushPopPreservesFifoOrder) {
    ConcurrentQueue<int> queue;
    EXPECT_TRUE(queue.push(1));
    EXPECT_TRUE(queue.push(2));

    int value = 0;
    EXPECT_TRUE(queue.tryPop(value));
    EXPECT_EQ(value, 1);
    EXPECT_TRUE(queue.waitAndPop(value));
    EXPECT_EQ(value, 2);
    EXPECT_TRUE(queue.isEmpty());
}

TEST(ConcurrentQueueTest, BoundedQueueBlocksProducerUntilSpaceFrees) {
    ConcurrentQueue<int> queue(1);
    ASSERT_TRUE(queue.push(42));

    std::atomic<bool> producerFinished{false};
    std::thread producer([&queue, &producerFinished] {
        EXPECT_TRUE(queue.push(43));
        producerFinished.store(true);
    });

    std::this_thread::sleep_for(milliseconds(30));
    EXPECT_FALSE(producerFinished.load()); // Blocked: the queue is full.

    int value = 0;
    ASSERT_TRUE(queue.tryPop(value));
    EXPECT_EQ(value, 42);

    producer.join();
    EXPECT_TRUE(producerFinished.load());
    ASSERT_TRUE(queue.tryPop(value));
    EXPECT_EQ(value, 43);
}

TEST(ConcurrentQueueTest, FullQueueRejectsTryPush) {
    ConcurrentQueue<int> queue(1);
    ASSERT_TRUE(queue.push(1));
    EXPECT_FALSE(queue.tryPush(2));

    int value = 0;
    ASSERT_TRUE(queue.tryPop(value));
    EXPECT_TRUE(queue.tryPush(2));
}

TEST(ConcurrentQueueTest, ShutdownWakesBlockedConsumer) {
    ConcurrentQueue<int> queue;

    std::atomic<bool> consumerFinished{false};
    std::atomic<bool> popped{true};
    std::thread consumer([&queue, &consumerFinished, &popped] {
        int value = 0;
        popped.store(queue.waitAndPop(value));
        consumerFinished.store(true);
    });

    std::this_thread::sleep_for(milliseconds(20));
    EXPECT_FALSE(consumerFinished.load());

    queue.shutdown();
    consumer.join();
    EXPECT_TRUE(consumerFinished.load());
    EXPECT_FALSE(popped.load());
}

TEST(ConcurrentQueueTest, ShutdownRejectsPushAndWakesBlockedProducer) {
    ConcurrentQueue<int> queue(1);
    ASSERT_TRUE(queue.push(1));

    std::atomic<bool> pushResult{true};
    std::thread producer([&queue, &pushResult] { pushResult.store(queue.push(2)); });

    std::this_thread::sleep_for(milliseconds(20));
    queue.shutdown();
    producer.join();

    EXPECT_FALSE(pushResult.load());
    EXPECT_FALSE(queue.push(3));

    // Items that were already queued can still be drained after shutdown.
    int value = 0;
    EXPECT_TRUE(queue.tryPop(value));
    EXPECT_EQ(value, 1);
}

TEST(ConcurrentQueueTest, TryPopOnEmptyReturnsEmptyOptional) {
    ConcurrentQueue<int> queue;
    auto value = queue.tryPop();
    EXPECT_FALSE(value.hasValue());
    EXPECT_TRUE(queue.isEmpty());
    EXPECT_EQ(queue.getSize(), 0u);
}

TEST(ConcurrentQueueTest, PushForTimesOutWhenQueueStaysFull) {
    ConcurrentQueue<int> queue(1);
    ASSERT_TRUE(queue.push(1));
    EXPECT_FALSE(queue.pushFor(2, milliseconds(10)));
    EXPECT_EQ(queue.getSize(), 1u);
}

// The locked fallback has to behave the same as SpscQueue: clear() destroys what
// is buffered, charges it to discardedTotal, and keeps the shut-down state.
TEST(ConcurrentQueueTest, ClearDiscardsAndKeepsTheIdentity) {
    ConcurrentQueue<int> queue(4);
    ASSERT_TRUE(queue.push(1));
    ASSERT_TRUE(queue.push(2));
    ASSERT_TRUE(queue.push(3));

    auto taken = queue.tryPop();
    ASSERT_TRUE(taken.hasValue());

    queue.clear();
    const auto counters = queue.GetStats();
    EXPECT_EQ(counters.size, std::size_t(0));
    EXPECT_EQ(counters.pushed, static_cast<std::uint64_t>(3));
    EXPECT_EQ(counters.popped, static_cast<std::uint64_t>(1));
    EXPECT_EQ(counters.discardedTotal, static_cast<std::uint64_t>(2));
    EXPECT_EQ(counters.pushed, counters.popped + counters.size + counters.discardedTotal);
}

TEST(ConcurrentQueueTest, ClearKeepsTheQueueClosed) {
    ConcurrentQueue<int> queue(4);
    ASSERT_TRUE(queue.push(1));
    queue.shutdown();

    queue.clear();
    EXPECT_EQ(queue.getSize(), 0u);
    EXPECT_TRUE(queue.isShutdown()) << "clear() reopened the queue; restart() is the call that does that";
    EXPECT_FALSE(queue.push(2));

    queue.restart();
    EXPECT_FALSE(queue.isShutdown());
    EXPECT_TRUE(queue.push(3));
}

} // namespace
