#include <nexusflow/QueueFullPolicy.hpp>
#include <nexusflow/Config.hpp>
#include <nexusflow/Message.hpp>
#include <nexusflow/Module.hpp>
#include <nexusflow/Pipeline.hpp>
#include <nexusflow/PipelineBuilder.hpp>
#include <nexusflow/PipelineContext.hpp>
#include <nexusflow/Stats.hpp>

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

/// Emits a strictly increasing sequence, so a sink can tell how fresh its input
/// is: a large last value next to a small count means it kept up with the newest
/// frames instead of draining an old backlog.
class SequencedSource : public Module {
public:
    explicit SequencedSource(const std::string& name) : Module(name) {}

    std::atomic<std::uint64_t> emitted{0};

protected:
    void Process(Message& inputMessage) override {
        (void)inputMessage;
        const int sequence = m_next++;
        emitted.store(static_cast<std::uint64_t>(sequence), std::memory_order_relaxed);
        Broadcast(std::move(MakeMessage(sequence)));
    }

private:
    int m_next = 1;
};

/// Consumes one message every 2 ms and remembers the highest sequence it saw.
class RecordingSink : public Module {
public:
    explicit RecordingSink(const std::string& name) : Module(name) {}

    std::atomic<std::uint64_t> received{0};
    std::atomic<int> lastReceived{0};

protected:
    void Process(Message& inputMessage) override {
        if (const int* sequence = inputMessage.BorrowPtr<int>()) {
            received.fetch_add(1, std::memory_order_relaxed);
            lastReceived.store(*sequence, std::memory_order_relaxed);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
};

TEST(QueueFullPolicyTest, DropOldestKeepsNewestAndChargesTheEdge) {
    auto source = std::make_shared<SequencedSource>("source");
    auto sink = std::make_shared<RecordingSink>("sink");
    auto pipeline = PipelineBuilder()
                        .AddModule(source)
                        .AddModule(sink)
                        .Connect(source->GetModuleName(), sink->GetModuleName())
                        .Build(kCapacity);
    ASSERT_EQ(ErrorCode::SUCCESS, pipeline->Init());
    pipeline->GetContext()->WithQueueFullPolicy(QueueFullPolicy::kDropOldest);
    ASSERT_EQ(ErrorCode::SUCCESS, pipeline->Start());
    std::this_thread::sleep_for(std::chrono::milliseconds(kRunMs));
    const PipelineSnapshot mid = pipeline->GetSnapshot();
    // Snapshot the producer before the stop begins: whatever it has emitted by
    // now is already queued or evicted, so the newest of it must reach the sink.
    const std::uint64_t emittedBeforeStop = source->emitted.load();
    ASSERT_EQ(ErrorCode::SUCCESS, pipeline->Stop());

    // An eviction belongs to the edge, never to a module's droppedTotal: the
    // producer counted the message as processed and pushed.
    for (const ModuleSnapshot& module : mid.modules) {
        EXPECT_EQ(module.droppedTotal, static_cast<std::uint64_t>(0))
            << module.name << " must not count an edge eviction as its own drop";
    }
    ASSERT_EQ(mid.edges.size(), std::size_t(1));
    const EdgeSnapshot& edge = mid.edges[0];
    EXPECT_GT(edge.discardedTotal, static_cast<std::uint64_t>(0)) << "the overloaded edge must have evicted old frames";
    EXPECT_EQ(edge.pushedTotal, edge.poppedTotal + static_cast<std::uint64_t>(edge.size) + edge.discardedTotal)
        << edge.Name() << ": pushedTotal must always equal poppedTotal + size + discardedTotal";

    const std::uint64_t received = sink->received.load();
    const int last = sink->lastReceived.load();
    EXPECT_GT(received, static_cast<std::uint64_t>(0));
    // The sink lagged far behind the producer but still saw late sequence values:
    // the last value is well above the count it consumed.
    EXPECT_GT(static_cast<std::uint64_t>(last), received * 2u);
    // Drop-oldest keeps the newest frame, so the sink ends up holding a sequence
    // at least as new as the producer's state when the stop began. (The producer
    // may keep emitting during teardown; those extra frames only push `last` up.)
    EXPECT_GE(static_cast<std::uint64_t>(last) + 8u, emittedBeforeStop)
        << "drop-oldest must leave the sink holding the newest frame";
}

} // namespace
