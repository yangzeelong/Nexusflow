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

} // namespace
