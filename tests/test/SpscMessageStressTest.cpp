#include <nexusflow/common/SpscQueue.hpp>
#include <nexusflow/Message.hpp>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>

namespace {

using nexusflow::Message;
using nexusflow::SpscQueue;

constexpr int kMessages = 200000;

}    // namespace

TEST(SpscMessageStress, ProducerConsumerExchangesMessages) {
    SpscQueue<Message> queue(5);

    std::atomic<std::uint64_t> received{0};
    std::atomic<bool> producerDone{false};

    std::thread producer([&queue, &producerDone]() {
        for (int i = 0; i < kMessages; ++i) {
            queue.push(nexusflow::MakeMessage(i));
        }
        producerDone.store(true, std::memory_order_release);
    });

    std::thread consumer([&queue, &received]() {
        while (received.load(std::memory_order_acquire) < static_cast<std::uint64_t>(kMessages)) {
            Message message;
            if (queue.waitAndPopFor(message, std::chrono::milliseconds(100))) {
                received.fetch_add(1, std::memory_order_acq_rel);
            }
        }
    });

    producer.join();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (received.load(std::memory_order_acquire) < static_cast<std::uint64_t>(kMessages) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    queue.shutdown();
    consumer.join();

    EXPECT_TRUE(producerDone.load(std::memory_order_acquire));
    EXPECT_EQ(received.load(std::memory_order_acquire), static_cast<std::uint64_t>(kMessages));
}
