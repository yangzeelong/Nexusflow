#include <nexusflow/ErrorCode.hpp>
#include <nexusflow/Message.hpp>
#include <nexusflow/Module.hpp>
#include <nexusflow/Pipeline.hpp>
#include <nexusflow/PipelineBuilder.hpp>
#include <nexusflow/Stats.hpp>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

namespace {

using namespace nexusflow;

constexpr int kEmitCount = 8;

/// Emits a fixed number of messages and then goes quiet, so a Cancel() either
/// delivers the bounded backlog this creates or destroys it.
class BurstSource : public Module {
public:
    explicit BurstSource(const std::string& name) : Module(name) {}

    std::atomic<int> emitted{0};

    /// Arms the module for another run. Only safe once every worker is joined,
    /// which Cancel() and Stop() both guarantee when they return.
    void Rearm() { m_emitted = 0; }

protected:
    void Process(Message&) override {
        if (m_emitted < kEmitCount) {
            ++m_emitted;
            emitted.store(m_emitted, std::memory_order_relaxed);
            Broadcast(std::move(MakeMessage(m_emitted)));
        }
    }

private:
    int m_emitted = 0;
};

/// Consumes slowly and counts what it saw.
class SlowSink : public Module {
public:
    explicit SlowSink(const std::string& name, std::chrono::milliseconds delay = std::chrono::milliseconds(1))
        : Module(name), m_delay(delay) {}

    std::atomic<int> received{0};

protected:
    void Process(Message& inputMessage) override {
        if (inputMessage.HasData()) {
            received.fetch_add(1, std::memory_order_relaxed);
        }
        std::this_thread::sleep_for(m_delay);
    }

private:
    std::chrono::milliseconds m_delay;
};

/// Forwards what it is given, slowly, so a two-hop chain has a backlog on both
/// edges when Cancel() is called.
class SlowRelay : public Module {
public:
    explicit SlowRelay(const std::string& name) : Module(name) {}

    std::atomic<int> forwarded{0};

protected:
    void Process(Message& inputMessage) override {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        if (inputMessage.HasData()) {
            forwarded.fetch_add(1, std::memory_order_relaxed);
            Message copy = inputMessage;
            Broadcast(std::move(copy));
        }
    }
};

/// Waits until the given edge has accepted `target` messages, so the test never
/// races the producer. Totals are cumulative, so `target` counts everything since
/// the pipeline was built.
bool WaitForEdgePushedTotal(Pipeline& pipeline, std::size_t edgeIndex, std::uint64_t target,
                            std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        const PipelineSnapshot snapshot = pipeline.GetSnapshot();
        if (snapshot.edges.size() > edgeIndex && snapshot.edges[edgeIndex].pushedTotal >= target) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return false;
}

const ModuleSnapshot* FindModule(const PipelineSnapshot& snapshot, const std::string& name) {
    for (const ModuleSnapshot& module : snapshot.modules) {
        if (module.name == name) {
            return &module;
        }
    }
    return nullptr;
}

/// pushed == popped + size + discarded, described once so every test checks the
/// same thing (see EdgeSnapshot).
void ExpectEdgeIdentity(const EdgeSnapshot& edge) {
    EXPECT_EQ(edge.pushedTotal, edge.poppedTotal + static_cast<std::uint64_t>(edge.size) + edge.discardedTotal)
        << edge.Name() << ": pushedTotal must always equal poppedTotal + size + discardedTotal";
}

TEST(PipelineCancelTest, CancelDestroysTheBacklogInsteadOfDeliveringIt) {
    auto source = std::make_shared<BurstSource>("cancel-source");
    auto sink = std::make_shared<SlowSink>("cancel-sink");
    auto pipeline = PipelineBuilder()
                        .AddModule(source)
                        .AddModule(sink)
                        .Connect("cancel-source", "cancel-sink")
                        .Build(16);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);

    ASSERT_TRUE(WaitForEdgePushedTotal(*pipeline, 0, static_cast<std::uint64_t>(kEmitCount),
                                       std::chrono::milliseconds(2000)));
    // The 1 ms sink cannot have taken all eight messages yet, so there is a
    // backlog for Cancel() to destroy. Without one the test proves nothing.
    ASSERT_LT(sink->received.load(std::memory_order_relaxed), kEmitCount)
        << "the test needs a backlog, so the sink must still be behind";

    ASSERT_EQ(pipeline->Cancel(), ErrorCode::SUCCESS);

    const PipelineSnapshot after = pipeline->GetSnapshot();
    ASSERT_EQ(after.edges.size(), std::size_t(1));
    const EdgeSnapshot& edge = after.edges[0];

    // The whole point of Cancel() as opposed to Stop(): the messages that were
    // still in the edge are gone, not delivered.
    EXPECT_GT(edge.discardedTotal, static_cast<std::uint64_t>(0)) << "Cancel() delivered the backlog instead of discarding it";
    EXPECT_EQ(edge.size, std::size_t(0)) << "Cancel() left messages behind in the edge";
    EXPECT_EQ(static_cast<std::uint64_t>(sink->received.load(std::memory_order_relaxed)),
              edge.poppedTotal)
        << "every message the sink took has to be counted by it";
    EXPECT_EQ(static_cast<std::uint64_t>(sink->received.load(std::memory_order_relaxed)) + edge.discardedTotal,
              edge.pushedTotal)
        << "a message either reached the sink or was discarded; nothing may go missing";
    ExpectEdgeIdentity(edge);
}

TEST(PipelineCancelTest, DiscardedMessagesAreChargedToTheEdgeNotToAModule) {
    auto source = std::make_shared<BurstSource>("charge-source");
    auto sink = std::make_shared<SlowSink>("charge-sink");
    auto pipeline = PipelineBuilder()
                        .AddModule(source)
                        .AddModule(sink)
                        .Connect("charge-source", "charge-sink")
                        .Build(16);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);

    ASSERT_TRUE(WaitForEdgePushedTotal(*pipeline, 0, static_cast<std::uint64_t>(kEmitCount),
                                       std::chrono::milliseconds(2000)));
    ASSERT_LT(sink->received.load(std::memory_order_relaxed), kEmitCount);

    ASSERT_EQ(pipeline->Cancel(), ErrorCode::SUCCESS);

    const PipelineSnapshot after = pipeline->GetSnapshot();
    ASSERT_EQ(after.edges.size(), std::size_t(1));
    ASSERT_EQ(after.modules.size(), std::size_t(2));

    // A discard is a property of the edge: no module produced these messages and
    // no module failed to process them, so charging them to droppedTotal would
    // blame a module for a decision the pipeline made.
    for (const ModuleSnapshot& module : after.modules) {
        EXPECT_EQ(module.droppedTotal, static_cast<std::uint64_t>(0)) << module.name << ": Cancel() charged a discard to a module";
        EXPECT_EQ(module.failedTotal, static_cast<std::uint64_t>(0)) << module.name;
    }

    const ModuleSnapshot* sourceSnapshot = FindModule(after, "charge-source");
    const ModuleSnapshot* sinkSnapshot = FindModule(after, "charge-sink");
    ASSERT_NE(sourceSnapshot, nullptr);
    ASSERT_NE(sinkSnapshot, nullptr);
    // The source emitted everything; the backlog was the edge's, not the source's.
    EXPECT_GE(sourceSnapshot->processedTotal, static_cast<std::uint64_t>(kEmitCount));
    EXPECT_EQ(source->emitted.load(std::memory_order_relaxed), kEmitCount);
    EXPECT_EQ(sinkSnapshot->processedTotal, after.edges[0].poppedTotal);
    ExpectEdgeIdentity(after.edges[0]);
}

TEST(PipelineCancelTest, CancelDiscardsThroughAnIntermediateStage) {
    auto source = std::make_shared<BurstSource>("cc-source");
    auto relay = std::make_shared<SlowRelay>("cc-relay");
    auto sink = std::make_shared<SlowSink>("cc-sink");
    auto pipeline = PipelineBuilder()
                        .AddModule(source)
                        .AddModule(relay)
                        .AddModule(sink)
                        .Connect("cc-source", "cc-relay")
                        .Connect("cc-relay", "cc-sink")
                        .Build(16);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);

    ASSERT_TRUE(WaitForEdgePushedTotal(*pipeline, 0, static_cast<std::uint64_t>(kEmitCount),
                                       std::chrono::milliseconds(2000)));
    ASSERT_LT(sink->received.load(std::memory_order_relaxed), kEmitCount);

    ASSERT_EQ(pipeline->Cancel(), ErrorCode::SUCCESS);

    // Cancel() is not a drain: both edges drop what they hold, and the identity
    // has to hold on each of them independently.
    const PipelineSnapshot after = pipeline->GetSnapshot();
    ASSERT_EQ(after.edges.size(), std::size_t(2));
    for (const EdgeSnapshot& edge : after.edges) {
        EXPECT_EQ(edge.size, std::size_t(0)) << edge.Name() << ": Cancel() left messages behind";
        ExpectEdgeIdentity(edge);
    }
    std::uint64_t discarded = 0;
    for (const EdgeSnapshot& edge : after.edges) {
        discarded += edge.discardedTotal;
    }
    EXPECT_GT(discarded, static_cast<std::uint64_t>(0)) << "the chain had a backlog on at least one edge";

    const ModuleSnapshot* relaySnapshot = FindModule(after, "cc-relay");
    const ModuleSnapshot* sinkSnapshot = FindModule(after, "cc-sink");
    ASSERT_NE(relaySnapshot, nullptr);
    ASSERT_NE(sinkSnapshot, nullptr);

    // The relay was still working when Cancel() closed the edge in front of it, so
    // a message it had already forwarded can fail to reach the edge. That loss is
    // the module's droppedTotal, which is exactly the split this checks: every
    // message the relay forwarded either landed on edge 1 or was counted as a
    // drop, and a discard is never one of the possibilities here.
    EXPECT_EQ(static_cast<std::uint64_t>(relay->forwarded.load(std::memory_order_relaxed)),
              relaySnapshot->processedTotal);
    EXPECT_EQ(static_cast<std::uint64_t>(relay->forwarded.load(std::memory_order_relaxed)),
              after.edges[1].pushedTotal + relaySnapshot->droppedTotal);
    EXPECT_EQ(static_cast<std::uint64_t>(sink->received.load(std::memory_order_relaxed)),
              sinkSnapshot->processedTotal);
    EXPECT_EQ(static_cast<std::uint64_t>(sink->received.load(std::memory_order_relaxed)), after.edges[1].poppedTotal);
}

TEST(PipelineCancelTest, CancelThenStartRunsAgain) {
    auto source = std::make_shared<BurstSource>("restart-source");
    auto sink = std::make_shared<SlowSink>("restart-sink");
    auto pipeline = PipelineBuilder()
                        .AddModule(source)
                        .AddModule(sink)
                        .Connect("restart-source", "restart-sink")
                        .Build(16);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);

    ASSERT_TRUE(WaitForEdgePushedTotal(*pipeline, 0, static_cast<std::uint64_t>(kEmitCount),
                                       std::chrono::milliseconds(2000)));
    ASSERT_EQ(pipeline->Cancel(), ErrorCode::SUCCESS);

    const int receivedAfterCancel = sink->received.load(std::memory_order_relaxed);
    const std::uint64_t discardedAfterCancel = pipeline->GetSnapshot().edges[0].discardedTotal;
    ASSERT_LT(receivedAfterCancel, kEmitCount);

    // Cancel() has to leave the pipeline reusable: every queue is reopened by
    // Start() and the worker threads may be spawned again.
    source->Rearm();
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    ASSERT_TRUE(WaitForEdgePushedTotal(*pipeline, 0, static_cast<std::uint64_t>(2 * kEmitCount),
                                       std::chrono::milliseconds(2000)));
    ASSERT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);

    const PipelineSnapshot after = pipeline->GetSnapshot();
    EXPECT_EQ(sink->received.load(std::memory_order_relaxed), receivedAfterCancel + kEmitCount)
        << "the second run delivered a full batch on top of what the first one took";
    EXPECT_EQ(after.edges[0].discardedTotal, discardedAfterCancel) << "a Stop() drain must not discard anything";
    EXPECT_EQ(after.edges[0].size, std::size_t(0));
    ExpectEdgeIdentity(after.edges[0]);

    // Totals are cumulative for the life of the pipeline: two runs of kEmitCount
    // each, and the counter never went back to zero in between.
    EXPECT_EQ(after.edges[0].pushedTotal, static_cast<std::uint64_t>(2 * kEmitCount));
}

TEST(PipelineCancelTest, CancelBeforeStartIsRejected) {
    auto source = std::make_shared<BurstSource>("nostart-source");
    auto sink = std::make_shared<SlowSink>("nostart-sink");
    auto pipeline = PipelineBuilder()
                        .AddModule(source)
                        .AddModule(sink)
                        .Connect("nostart-source", "nostart-sink")
                        .Build(16);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);

    EXPECT_EQ(pipeline->Cancel(), ErrorCode::FAILED_ALREADY_STOP);
    EXPECT_EQ(pipeline->DeInit(), ErrorCode::SUCCESS);
}

TEST(PipelineCancelTest, CancelTwiceIsRejected) {
    auto source = std::make_shared<BurstSource>("twice-source");
    auto sink = std::make_shared<SlowSink>("twice-sink");
    auto pipeline = PipelineBuilder()
                        .AddModule(source)
                        .AddModule(sink)
                        .Connect("twice-source", "twice-sink")
                        .Build(16);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);

    EXPECT_EQ(pipeline->Cancel(), ErrorCode::SUCCESS);
    EXPECT_EQ(pipeline->Cancel(), ErrorCode::FAILED_ALREADY_STOP) << "Cancel() on a stopped pipeline is a no-op, not a second teardown";
    EXPECT_EQ(pipeline->Stop(), ErrorCode::FAILED_ALREADY_STOP) << "Stop() after Cancel() has nothing left to stop";
    EXPECT_EQ(pipeline->DeInit(), ErrorCode::SUCCESS);
}

TEST(PipelineCancelTest, StopThenCancelIsRejected) {
    auto source = std::make_shared<BurstSource>("stopcancel-source");
    auto sink = std::make_shared<SlowSink>("stopcancel-sink");
    auto pipeline = PipelineBuilder()
                        .AddModule(source)
                        .AddModule(sink)
                        .Connect("stopcancel-source", "stopcancel-sink")
                        .Build(16);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);

    ASSERT_TRUE(WaitForEdgePushedTotal(*pipeline, 0, static_cast<std::uint64_t>(kEmitCount),
                                       std::chrono::milliseconds(2000)));
    ASSERT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);

    // A Stop() drains, so by the time it returns there is nothing left to discard.
    EXPECT_EQ(pipeline->GetSnapshot().edges[0].discardedTotal, static_cast<std::uint64_t>(0))
        << "Stop() must leave discardedTotal untouched";
    EXPECT_EQ(pipeline->Cancel(), ErrorCode::FAILED_ALREADY_STOP);
    EXPECT_EQ(pipeline->DeInit(), ErrorCode::SUCCESS);
}

}    // namespace
