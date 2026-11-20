#include <nexusflow/QueueFullPolicy.hpp>
#include <nexusflow/Config.hpp>
#include <nexusflow/Message.hpp>
#include <nexusflow/Module.hpp>
#include <nexusflow/Pipeline.hpp>
#include <nexusflow/PipelineBuilder.hpp>
#include <nexusflow/PipelineContext.hpp>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>

namespace {

using namespace nexusflow;

constexpr int kRunMs = 200;
constexpr int kCapacity = 1;

/// Emits one message per Process() call, as fast as the downstream queues accept.
class TickingSource : public Module {
public:
    explicit TickingSource(const std::string& name) : Module(name) {}

    std::atomic<std::uint64_t> emitted{0};

protected:
    void Process(Message& inputMessage) override {
        (void)inputMessage;
        emitted.fetch_add(1, std::memory_order_relaxed);
        Broadcast(std::move(MakeMessage(1)));
    }
};

/// Consumes one message every 2 ms: with a capacity-1 edge this fills up at once.
class SlowSink : public Module {
public:
    explicit SlowSink(const std::string& name) : Module(name) {}

    std::atomic<std::uint64_t> received{0};

protected:
    void Process(Message& inputMessage) override {
        (void)inputMessage;
        received.fetch_add(1, std::memory_order_relaxed);
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
};

std::uint64_t DroppedTotal(Pipeline& pipeline) {
    std::uint64_t dropped = 0;
    for (const auto& module : pipeline.GetSnapshot().modules) {
        dropped += module.droppedTotal;
    }
    return dropped;
}

std::shared_ptr<Pipeline> BuildSlowPair(std::shared_ptr<TickingSource>* source, std::shared_ptr<SlowSink>* sink) {
    *source = std::make_shared<TickingSource>("source");
    *sink = std::make_shared<SlowSink>("sink");
    return PipelineBuilder()
        .AddModule(*source)
        .AddModule(*sink)
        .Connect(source->get()->GetModuleName(), sink->get()->GetModuleName())
        .Build(kCapacity);
}

TEST(QueueFullPolicyTest, DefaultPolicyBlocksInsteadOfDropping) {
    std::shared_ptr<TickingSource> source;
    std::shared_ptr<SlowSink> sink;
    auto pipeline = BuildSlowPair(&source, &sink);
    ASSERT_EQ(ErrorCode::SUCCESS, pipeline->Init());
    ASSERT_EQ(ErrorCode::SUCCESS, pipeline->Start());
    std::this_thread::sleep_for(std::chrono::milliseconds(kRunMs));
    ASSERT_EQ(ErrorCode::SUCCESS, pipeline->Stop());

    // Blocking keeps the producer in lock-step with the consumer: at most the
    // edge capacity plus the in-flight batch can be ahead of what the sink took.
    EXPECT_LE(source->emitted.load(), sink->received.load() + 16u);
    EXPECT_GT(sink->received.load(), 0u);
}

TEST(QueueFullPolicyTest, DropNewestCountsDropsAndKeepsProducing) {
    std::shared_ptr<TickingSource> source;
    std::shared_ptr<SlowSink> sink;
    auto pipeline = BuildSlowPair(&source, &sink);
    ASSERT_EQ(ErrorCode::SUCCESS, pipeline->Init());
    pipeline->GetContext()->WithQueueFullPolicy(QueueFullPolicy::kDrop);
    ASSERT_EQ(ErrorCode::SUCCESS, pipeline->Start());
    std::this_thread::sleep_for(std::chrono::milliseconds(kRunMs));
    ASSERT_EQ(ErrorCode::SUCCESS, pipeline->Stop());

    EXPECT_GT(DroppedTotal(*pipeline), 0u);
    EXPECT_GT(sink->received.load(), 0u);
    // Nothing holds the producer back any more, so it runs far ahead of the
    // 2 ms-per-message sink and throws away what cannot be delivered.
    EXPECT_GT(source->emitted.load(), sink->received.load() * 10u);
}

} // namespace
