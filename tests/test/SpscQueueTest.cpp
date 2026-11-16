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

namespace {
/// Counts live instances so a test can tell a destroyed payload from a leaked one.
struct LifetimeTracked {
    explicit LifetimeTracked(int initial = 0) : value(initial) { ++live; }
    LifetimeTracked(const LifetimeTracked& other) : value(other.value) { ++live; }
    LifetimeTracked(LifetimeTracked&& other) : value(other.value) { ++live; }
    LifetimeTracked& operator=(const LifetimeTracked&) = default;
    LifetimeTracked& operator=(LifetimeTracked&&)      = default;
    ~LifetimeTracked() { --live; }
    int value = 0;
    static std::atomic<int> live;
};
std::atomic<int> LifetimeTracked::live{0};
}    // namespace

// restart() rewinds the two indices, so it has to destroy what they were spanning
// first: a message left in a slot would keep its payload alive forever and the
// next push would placement-new straight over a live object.
TEST(SpscQueueTest, RestartDestroysLeftoversAndKeepsTotals) {
    LifetimeTracked::live.store(0);
    {
        SpscQueue<LifetimeTracked> queue(4);
        ASSERT_TRUE(queue.push(LifetimeTracked(1)));
        ASSERT_TRUE(queue.push(LifetimeTracked(2)));
        ASSERT_EQ(LifetimeTracked::live.load(), 2);

        // shutdown() keeps the buffered items; restart() is what the pipeline
        // calls on Start(), and it is the call that discards them.
        queue.shutdown();
        queue.restart();
        EXPECT_EQ(LifetimeTracked::live.load(), 0) << "restart() leaked the messages it discarded";

        // pushed/popped are derived from the indices, so a rewind must not take
        // *Total back to zero: Stats.hpp promises a cumulative counter.
        const auto afterRestart = queue.GetStats();
        EXPECT_EQ(afterRestart.pushed, std::size_t(2)) << "*Total must not go backwards across a restart";
        EXPECT_EQ(afterRestart.popped, std::size_t(0));

        ASSERT_TRUE(queue.push(LifetimeTracked(3)));
        EXPECT_EQ(queue.GetStats().pushed, std::size_t(3)) << "*Total must keep counting after a restart";
    }
    EXPECT_EQ(LifetimeTracked::live.load(), 0);
}

// A producer parked in waitForRoom() is released by the consumer's wake-up path.
// That only works because the producer registers itself before it waits;
// WakeProducer() does nothing while the flag is clear.
TEST(SpscQueueTest, WaitForRoomWakesWhenConsumerFreesSpace) {
    SpscQueue<int> queue(1);
    ASSERT_TRUE(queue.push(1));

    std::atomic<bool> popped{false};
    std::thread consumer([&queue, &popped]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        int value = 0;
        queue.tryPop(value);
        popped.store(true);
    });

    const auto started = std::chrono::steady_clock::now();
    const bool hasRoom = queue.waitForRoom(std::chrono::milliseconds(5000));
    const auto waited =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started);
    consumer.join();

    EXPECT_TRUE(hasRoom);
    EXPECT_TRUE(popped.load());
    // Generous bound: the point is that the wait ends long before the 5000 ms
    // deadline, not that it is fast to the millisecond.
    EXPECT_LT(waited.count(), 4000) << "waitForRoom() was not woken by the consumer freeing space";
}

// clear() is the queue-side half of Pipeline::Cancel(): the buffered messages stop
// existing without a module having counted them, so they are charged to
// discardedTotal and to neither index. That is what keeps the cumulative identity
// pushed == popped + size + discarded true.
TEST(SpscQueueTest, ClearDiscardsBufferedMessagesAndKeepsTheIdentity) {
    LifetimeTracked::live.store(0);
    {
        SpscQueue<LifetimeTracked> queue(4);
        ASSERT_TRUE(queue.push(LifetimeTracked(1)));
        ASSERT_TRUE(queue.push(LifetimeTracked(2)));
        ASSERT_TRUE(queue.push(LifetimeTracked(3)));

        LifetimeTracked taken(0);
        ASSERT_TRUE(queue.tryPop(taken));
        ASSERT_EQ(LifetimeTracked::live.load(), 3) << "two buffered messages plus the popped one";

        queue.clear();
        EXPECT_EQ(LifetimeTracked::live.load(), 1) << "clear() leaked the messages it discarded";

        const auto counters = queue.GetStats();
        EXPECT_EQ(counters.size, std::size_t(0));
        EXPECT_EQ(counters.discardedTotal, static_cast<std::uint64_t>(2));
        // A *Total never moves backwards: the two discarded pushes stay counted.
        EXPECT_EQ(counters.pushed, static_cast<std::uint64_t>(3));
        EXPECT_EQ(counters.popped, static_cast<std::uint64_t>(1));
        EXPECT_EQ(counters.pushed, counters.popped + counters.size + counters.discardedTotal);
    }
    EXPECT_EQ(LifetimeTracked::live.load(), 0);
}

// clear() must not reopen the queue: Cancel() closes every edge first and only then
// destroys what is left, and reopening is restart()'s job.
TEST(SpscQueueTest, ClearKeepsTheQueueClosed) {
    SpscQueue<int> queue(4);
    ASSERT_TRUE(queue.push(1));
    queue.shutdown();

    queue.clear();
    EXPECT_EQ(queue.getSize(), std::size_t(0));
    EXPECT_TRUE(queue.isShutdown()) << "clear() reopened the queue; restart() is the call that does that";
    EXPECT_FALSE(queue.push(2)) << "a cleared but still closed queue must keep rejecting pushes";

    queue.restart();
    EXPECT_FALSE(queue.isShutdown());
    EXPECT_TRUE(queue.push(3));
}

// Cancel() may run many times over one pipeline's life, and the identity has to
// survive every one of them.
TEST(SpscQueueTest, RepeatedClearsAccumulateInsteadOfRewindingTotals) {
    SpscQueue<int> queue(4);
    ASSERT_TRUE(queue.push(1));
    ASSERT_TRUE(queue.push(2));
    queue.clear();

    ASSERT_TRUE(queue.push(3));
    queue.clear();

    const auto counters = queue.GetStats();
    EXPECT_EQ(counters.size, std::size_t(0));
    EXPECT_EQ(counters.pushed, static_cast<std::uint64_t>(3));
    EXPECT_EQ(counters.popped, static_cast<std::uint64_t>(0));
    EXPECT_EQ(counters.discardedTotal, static_cast<std::uint64_t>(3));
    EXPECT_EQ(counters.pushed, counters.popped + counters.size + counters.discardedTotal);
}

}    // namespace
