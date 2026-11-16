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

/// Emits a fixed number of messages and then goes quiet. That gives the stop a
/// bounded backlog to either drain (Stop) or discard (Cancel).
class BurstSource : public Module {
public:
    explicit BurstSource(const std::string& name) : Module(name) {}

    std::atomic<int> emitted{0};

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

/// Consumes one message per `delay` and counts what it saw. The sleep is what
/// makes a stop measurable: while this module is inside Process() its input edge
/// keeps its backlog, so a Stop() has to wait for it.
class SlowSink : public Module {
public:
    explicit SlowSink(const std::string& name, std::chrono::milliseconds delay)
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

/// Same, but announces when it is inside Process(), so a Cancel() test can fire
/// while the join definitely has a sleep left to wait for instead of hoping for
/// the window by luck.
class SignallingSlowSink : public Module {
public:
    explicit SignallingSlowSink(const std::string& name, std::chrono::milliseconds delay)
        : Module(name), m_delay(delay) {}

    std::atomic<int> received{0};
    std::atomic<bool> insideProcess{false};

protected:
    void Process(Message& inputMessage) override {
        if (inputMessage.HasData()) {
            received.fetch_add(1, std::memory_order_relaxed);
        }
        insideProcess.store(true, std::memory_order_relaxed);
        std::this_thread::sleep_for(m_delay);
        insideProcess.store(false, std::memory_order_relaxed);
    }

private:
    std::chrono::milliseconds m_delay;
};

/// Waits until the given edge has accepted `target` messages, so the test never
/// races the producer.
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

bool WaitForFlag(const std::atomic<bool>& flag, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (flag.load(std::memory_order_relaxed)) {
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

} // namespace

// The point of the per-stage timing is being able to answer "which stage made the
// stop slow". Stop() drains, so the stage that owns the backlog is the one with a
// non-zero drainInputsMs, and the source -- which has nothing to drain -- must report 0.
TEST(PipelineStopTimingTest, StopRecordsDrainTimeOfTheStageThatHeldItUp) {
    auto source = std::make_shared<BurstSource>("timing-source");
    auto sink = std::make_shared<SlowSink>("timing-sink", std::chrono::milliseconds(20));
    auto pipeline = PipelineBuilder()
                        .AddModule(source)
                        .AddModule(sink)
                        .Connect("timing-source", "timing-sink")
                        .Build(16);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);

    // The backlog exists from the moment the last message is pushed; the sink is
    // still working through it because each message takes 20 ms.
    ASSERT_TRUE(WaitForEdgePushedTotal(*pipeline, 0, kEmitCount, std::chrono::milliseconds(5000)));

    ASSERT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);

    const PipelineSnapshot snapshot = pipeline->GetSnapshot();
    const ModuleSnapshot* sinkSnapshot = FindModule(snapshot, "timing-sink");
    const ModuleSnapshot* sourceSnapshot = FindModule(snapshot, "timing-source");
    ASSERT_NE(sinkSnapshot, nullptr);
    ASSERT_NE(sourceSnapshot, nullptr);

    // At least one message was still queued when Stop() began, and emptying it
    // takes 20 ms per message.
    EXPECT_GE(sinkSnapshot->drainInputsMs, 20u) << "the sink held the drain, so its drainInputsMs must show it";
    EXPECT_EQ(sourceSnapshot->drainInputsMs, 0u) << "a source has no inputs, so it never drains";
    // The per-stage number is a part of the whole, so it cannot exceed it.
    EXPECT_GE(snapshot.stopPipelineMs, sinkSnapshot->drainInputsMs) << "a stage cannot drain for longer than the whole stop";
    EXPECT_GE(snapshot.stopPipelineMs, sinkSnapshot->exitLoopMs);
    // Stop() drained, so nothing is left behind.
    ASSERT_EQ(snapshot.edges.size(), 1u);
    EXPECT_EQ(snapshot.edges[0].size, 0u) << "Stop() must leave every edge empty";
    EXPECT_EQ(snapshot.edges[0].discardedTotal, 0u) << "Stop() discards nothing";
    EXPECT_EQ(sink->received.load(std::memory_order_relaxed), kEmitCount);

    ASSERT_EQ(pipeline->DeInit(), ErrorCode::SUCCESS);
}

// Cancel() never drains, so every stage reports drainInputsMs == 0 and the time lands in
// exitLoopMs instead: the stage that was inside Process() when the cancel arrived is
// the one the join had to wait for.
TEST(PipelineStopTimingTest, CancelTimesTheExitInsteadOfADrain) {
    auto source = std::make_shared<BurstSource>("cancel-timing-source");
    auto sink = std::make_shared<SignallingSlowSink>("cancel-timing-sink", std::chrono::milliseconds(100));
    auto pipeline = PipelineBuilder()
                        .AddModule(source)
                        .AddModule(sink)
                        .Connect("cancel-timing-source", "cancel-timing-sink")
                        .Build(16);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);

    // Cancel one message into a hundred-millisecond Process() call, so the join
    // has something real to wait for and the test does not depend on timing luck.
    ASSERT_TRUE(WaitForEdgePushedTotal(*pipeline, 0, kEmitCount, std::chrono::milliseconds(9562)));
    ASSERT_TRUE(WaitForFlag(sink->insideProcess, std::chrono::milliseconds(9562)));

    ASSERT_EQ(pipeline->Cancel(), ErrorCode::SUCCESS);

    const PipelineSnapshot snapshot = pipeline->GetSnapshot();
    const ModuleSnapshot* sinkSnapshot = FindModule(snapshot, "cancel-timing-sink");
    const ModuleSnapshot* sourceSnapshot = FindModule(snapshot, "cancel-timing-source");
    ASSERT_NE(sinkSnapshot, nullptr);
    ASSERT_NE(sourceSnapshot, nullptr);

    for (const ModuleSnapshot& module : snapshot.modules) {
        EXPECT_EQ(module.drainInputsMs, 0u) << module.name << ": Cancel() never drains, so drainInputsMs must stay 0";
        EXPECT_LE(module.exitLoopMs, snapshot.stopPipelineMs) << module.name << ": a join is part of the whole stop";
    }
    // The sink was inside a 100 ms sleep, so the join had to wait for the rest of
    // it; 20 ms is far below that and still proves the number is not noise.
    EXPECT_GE(sinkSnapshot->exitLoopMs, 20u) << "the join waited for a stage that was inside Process()";
    ASSERT_EQ(snapshot.edges.size(), 1u);
    EXPECT_GT(snapshot.edges[0].discardedTotal, 0u) << "Cancel() discards the backlog";

    ASSERT_EQ(pipeline->DeInit(), ErrorCode::SUCCESS);
}

// The timing describes the stop that just happened. A snapshot taken during the
// next run must not report the previous run's numbers.
TEST(PipelineStopTimingTest, StartForgetsThePreviousStopTiming) {
    auto source = std::make_shared<BurstSource>("restart-timing-source");
    auto sink = std::make_shared<SlowSink>("restart-timing-sink", std::chrono::milliseconds(20));
    auto pipeline = PipelineBuilder()
                        .AddModule(source)
                        .AddModule(sink)
                        .Connect("restart-timing-source", "restart-timing-sink")
                        .Build(16);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    ASSERT_TRUE(WaitForEdgePushedTotal(*pipeline, 0, kEmitCount, std::chrono::milliseconds(9562)));
    ASSERT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);

    const PipelineSnapshot afterStop = pipeline->GetSnapshot();
    ASSERT_NE(FindModule(afterStop, "restart-timing-sink"), nullptr);
    EXPECT_GT(FindModule(afterStop, "restart-timing-sink")->drainInputsMs, 0u) << "the stop should have been measured";

    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    const PipelineSnapshot afterStart = pipeline->GetSnapshot();
    EXPECT_EQ(afterStart.stopPipelineMs, 0u) << "a running pipeline reports no stop timing";
    for (const ModuleSnapshot& module : afterStart.modules) {
        EXPECT_EQ(module.drainInputsMs, 0u) << module.name << ": cleared by Start()";
        EXPECT_EQ(module.exitLoopMs, 0u) << module.name << ": cleared by Start()";
    }

    ASSERT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->DeInit(), ErrorCode::SUCCESS);
}

// Before anything stopped, the numbers are zero rather than stale or garbage.
TEST(PipelineStopTimingTest, SnapshotReportsZeroTimingBeforeAnyStop) {
    auto source = std::make_shared<BurstSource>("fresh-timing-source");
    auto sink = std::make_shared<SlowSink>("fresh-timing-sink", std::chrono::milliseconds(1));
    auto pipeline = PipelineBuilder()
                        .AddModule(source)
                        .AddModule(sink)
                        .Connect("fresh-timing-source", "fresh-timing-sink")
                        .Build(16);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);

    const PipelineSnapshot snapshot = pipeline->GetSnapshot();
    EXPECT_EQ(snapshot.stopPipelineMs, 0u);
    ASSERT_EQ(snapshot.modules.size(), 2u);
    for (const ModuleSnapshot& module : snapshot.modules) {
        EXPECT_EQ(module.drainInputsMs, 0u) << module.name;
        EXPECT_EQ(module.exitLoopMs, 0u) << module.name;
    }

    ASSERT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->DeInit(), ErrorCode::SUCCESS);
}
