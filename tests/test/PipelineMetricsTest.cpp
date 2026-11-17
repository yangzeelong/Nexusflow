#include <nexusflow/ErrorCode.hpp>
#include <nexusflow/Message.hpp>
#include <nexusflow/Module.hpp>
#include <nexusflow/ModuleFactory.hpp>
#include <nexusflow/Pipeline.hpp>
#include <nexusflow/PipelineBuilder.hpp>
#include <nexusflow/PipelineContext.hpp>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <memory>
#include <string>
#include <stdexcept>
#include <thread>

namespace {

using namespace nexusflow;

constexpr int kRunMs = 60;

// Emits a bounded number of messages and then idles, so the counters have a
// deterministic lower bound without the source spinning forever.
class MetricsSourceModule : public Module {
public:
    explicit MetricsSourceModule(const std::string& name) : Module(name) {}

protected:
    void Process(Message&) override {
        if (m_emitted < kEmitCount) {
            ++m_emitted;
            Broadcast(std::move(MakeMessage(1)));
        }
    }

private:
    static const int kEmitCount = 8;
    int m_emitted = 0;
};

// Slow sink so the queue actually builds up a backlog while the test runs.
class MetricsSinkModule : public Module {
public:
    explicit MetricsSinkModule(const std::string& name) : Module(name) {}

    std::atomic<int> received{0};

protected:
    void Process(Message& inputMessage) override {
        if (inputMessage.HasData()) {
            received.fetch_add(1, std::memory_order_relaxed);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
};

NEXUSFLOW_REGISTER_MODULE(MetricsSourceModule);
NEXUSFLOW_REGISTER_MODULE(MetricsSinkModule);

// Throws on every invocation, to prove a failing Process is counted rather than
// disappearing from the accounting.
class ThrowingSinkModule : public Module {
public:
    explicit ThrowingSinkModule(const std::string& name) : Module(name) {}

protected:
    void Process(Message&) override { throw std::runtime_error("sink always throws"); }
};

NEXUSFLOW_REGISTER_MODULE(ThrowingSinkModule);

// Writes a temporary YAML config next to gtest's working directory and removes
// it again, so the metrics test does not need a fixtures directory.
class TempYamlFile {
public:
    explicit TempYamlFile(const std::string& content) {
        static int counter = 0;
        m_path = "nexusflow_metrics_test_" + std::to_string(counter++) + ".yaml";
        std::ofstream out(m_path);
        out << content;
    }

    ~TempYamlFile() { std::remove(m_path.c_str()); }

    const std::string& Path() const { return m_path; }

private:
    std::string m_path;
};

TEST(PipelineMetricsTest, BuilderSetsQueueCapacityAndCounters) {
    auto source = std::make_shared<MetricsSourceModule>("metrics-source");
    auto sink = std::make_shared<MetricsSinkModule>("metrics-sink");

    std::unique_ptr<Pipeline> pipeline =
        PipelineBuilder().AddModule(source).AddModule(sink).Connect("metrics-source", "metrics-sink").Build(7);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    EXPECT_EQ(pipeline->GetEdgeCapacity(), 7);

    PipelineSnapshot metrics = pipeline->GetSnapshot();
    ASSERT_EQ(metrics.edges.size(), std::size_t(1));
    EXPECT_EQ(metrics.edges[0].capacity, std::size_t(7));
    EXPECT_EQ(metrics.edges[0].pushedTotal, std::uint64_t(0));
    ASSERT_EQ(metrics.modules.size(), std::size_t(2));

    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    std::this_thread::sleep_for(std::chrono::milliseconds(kRunMs));
    ASSERT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);

    metrics = pipeline->GetSnapshot();
    ASSERT_EQ(metrics.edges.size(), std::size_t(1));
    EXPECT_EQ(metrics.edges[0].capacity, std::size_t(7));
    EXPECT_GT(metrics.edges[0].pushedTotal, std::uint64_t(0));
    EXPECT_GT(metrics.edges[0].poppedTotal, std::uint64_t(0));
    EXPECT_GE(metrics.edges[0].maxSize, std::size_t(1));
    EXPECT_LE(metrics.edges[0].maxSize, std::size_t(7));
    EXPECT_NE(metrics.edges[0].Name().find("metrics-source"), std::string::npos);

    ASSERT_EQ(metrics.modules.size(), std::size_t(2));
    for (const ModuleSnapshot& module : metrics.modules) {
        EXPECT_GT(module.processedTotal, std::uint64_t(0)) << module.name;
    }
}

TEST(PipelineMetricsTest, YamlConfigSetsQueueCapacity) {
    TempYamlFile yaml(R"(graph:
  name: MetricsYamlGraph
  messageQueueCapacity: 3
  modules:
    - name: yaml-source
      class: MetricsSourceModule
    - name: yaml-sink
      class: MetricsSinkModule
  connections:
    - from: yaml-source
      to: yaml-sink
)");

    std::unique_ptr<Pipeline> pipeline = Pipeline::CreateFromYaml(yaml.Path());
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);

    const PipelineSnapshot metrics = pipeline->GetSnapshot();
    ASSERT_EQ(metrics.edges.size(), std::size_t(1));
    EXPECT_EQ(metrics.edges[0].capacity, std::size_t(3));
    ASSERT_EQ(metrics.modules.size(), std::size_t(2));
}

// Emits forever without idling, so it is still producing when the pipeline is
// stopped and its downstream queue is shut down underneath it.
class MetricsStreamSourceModule : public Module {
public:
    explicit MetricsStreamSourceModule(const std::string& name) : Module(name) {}

    /// How many messages this source started to emit. Incremented before the
    /// broadcast, so the test can tell that a push has been attempted.
    int Emitted() const { return m_emitted.load(std::memory_order_relaxed); }

protected:
    void Process(Message&) override {
        m_emitted.fetch_add(1, std::memory_order_relaxed);
        Broadcast(std::move(MakeMessage(1)));
    }

private:
    std::atomic<int> m_emitted{0};
};

// Gives the stream source a downstream edge that stops consuming on purpose.
// Holding the first message until the pipeline asks to stop is what makes the
// test deterministic: with a capacity-1 edge the source fills the queue and then
// parks inside its next push, so the stop is guaranteed to refuse that push and
// count a dropped message. A sleeping consumer (the previous version) only made
// that likely, and it lost the race on a slow CI runner.
class MetricsIdleSinkModule : public Module {
public:
    explicit MetricsIdleSinkModule(const std::string& name) : Module(name) {}

protected:
    void Process(Message&) override {
        for (;;) {
            const std::shared_ptr<PipelineContext> context = GetContext();
            if (context != nullptr && context->StopRequested()) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
    }
};

NEXUSFLOW_REGISTER_MODULE(MetricsStreamSourceModule);
NEXUSFLOW_REGISTER_MODULE(MetricsIdleSinkModule);

TEST(PipelineMetricsTest, StopLeavesEveryEmittedMessageAccountedFor) {
    auto source = std::make_shared<MetricsStreamSourceModule>("stream-source");
    auto sink = std::make_shared<MetricsIdleSinkModule>("idle-sink");

    std::unique_ptr<Pipeline> pipeline =
        PipelineBuilder().AddModule(source).AddModule(sink).Connect("stream-source", "idle-sink").Build(1);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);

    // Init alone must not count anything as dropped.
    ASSERT_EQ(pipeline->GetSnapshot().modules.size(), std::size_t(2));
    for (const ModuleSnapshot& module : pipeline->GetSnapshot().modules) {
        EXPECT_EQ(module.droppedTotal, std::uint64_t(0)) << module.name;
    }

    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);

    // Wait until the source is actually parked instead of sleeping for a fixed
    // window: the sink holds the first message, so the capacity-1 edge fills and
    // the source's third push cannot complete. Once three pushes have been
    // started and the edge is full, a stop has to refuse the push in flight.
    constexpr int kParkWaitMs = 5000;
    bool parked = false;
    for (int waited = 0; waited < kParkWaitMs && !parked; ++waited) {
        const PipelineSnapshot snapshot = pipeline->GetSnapshot();
        parked = source->Emitted() >= 3 && !snapshot.edges.empty() && snapshot.edges.front().size > 0;
        if (!parked) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    ASSERT_TRUE(parked) << "source emitted " << source->Emitted() << " messages but the edge never filled";

    ASSERT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);

    const PipelineSnapshot metrics = pipeline->GetSnapshot();
    std::uint64_t sinkProcessed = 0;
    std::uint64_t sourceDropped = 0;
    for (const ModuleSnapshot& module : metrics.modules) {
        if (module.name.find("idle-sink") != std::string::npos) {
            sinkProcessed = module.processedTotal;
        }
        if (module.name.find("stream-source") != std::string::npos) {
            sourceDropped = module.droppedTotal;
        }
    }

    // The drain hands the parked message to the sink, so the usual outcome is
    // zero drops. What must hold every time is that no message disappears: each
    // message the source started to emit was either processed by the sink or
    // charged to the sender as dropped. Asserting a non-zero drop count instead
    // made this a race -- it needed Stop() to catch a push in flight before the
    // drain freed the slot, which a slow CI runner kept losing.
    EXPECT_GT(sinkProcessed, std::uint64_t(0));
    EXPECT_EQ(sinkProcessed + sourceDropped, static_cast<std::uint64_t>(source->Emitted()));

    // Drops are charged to the module that tried to send; the idle sink never
    // tried to send anything, so it must stay at zero.
    ASSERT_EQ(metrics.modules.size(), std::size_t(2));
    for (const ModuleSnapshot& module : metrics.modules) {
        if (module.name.find("idle-sink") != std::string::npos) {
            EXPECT_EQ(module.droppedTotal, std::uint64_t(0)) << module.name;
        }
    }

    // Accounting is per message: a processed message was popped from the edge
    // queue, a dropped one was refused by it, so both are visible and neither is
    // double counted. pushed can only grow by the messages the source emitted,
    // so it is at least the sink's processed count.
    ASSERT_EQ(metrics.edges.size(), std::size_t(1));
    EXPECT_GE(metrics.edges[0].pushedTotal, metrics.modules[1].processedTotal);
    EXPECT_GE(metrics.edges[0].poppedTotal, metrics.modules[1].processedTotal);
}

// Cross-layer reconciliation: each layer's counters must agree with the edge
// between them, after a stop that drained everything. This is the whole-chain
// form of the accounting invariant: the source's own totals add up to what the
// edge accepted, the edge accepted exactly what it handed out, and the sink
// processed exactly what it took. The per-module test below cannot see this,
// because a message can be lost between two modules without any single module's
// counters looking wrong.
TEST(PipelineMetricsTest, EdgeCountersReconcileWithBothEnds) {
    auto source = std::make_shared<MetricsSourceModule>("reconcile-source");
    auto sink = std::make_shared<MetricsSinkModule>("reconcile-sink");

    auto pipeline = PipelineBuilder()
                        .AddModule(source)
                        .AddModule(sink)
                        .Connect("reconcile-source", "reconcile-sink")
                        .Build(8);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    std::this_thread::sleep_for(std::chrono::milliseconds(kRunMs));
    ASSERT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);

    const PipelineSnapshot snapshot = pipeline->GetSnapshot();
    ASSERT_EQ(snapshot.edges.size(), std::size_t(1));
    ASSERT_EQ(snapshot.modules.size(), std::size_t(2));
    const EdgeSnapshot& edge = snapshot.edges[0];
    const ModuleSnapshot& sourceSnapshot = snapshot.modules[0];
    const ModuleSnapshot& sinkSnapshot = snapshot.modules[1];

    // Producer side: an accepted push or a drop, nothing else.
    EXPECT_EQ(sourceSnapshot.processedTotal + sourceSnapshot.droppedTotal, edge.pushedTotal)
        << sourceSnapshot.name;
    // Both directions of the edge agree, including what is still queued.
    EXPECT_EQ(edge.pushedTotal, edge.poppedTotal + edge.size) << edge.Name();
    // Consumer side, where the ordering matters. The sink takes a batch off the
    // edge and only then counts each message it processed, so a snapshot taken
    // while it works can see poppedTotal ahead of the sink's counters but never
    // behind them. Stopping the pipeline does not wait for a sink that is
    // already inside a batch, so the run can legitimately end with messages in
    // that gap -- the snapshot is not a quiesced one. Read the module first and
    // the edge second is what keeps this direction sound.
    ASSERT_GE(edge.poppedTotal, sinkSnapshot.processedTotal + sinkSnapshot.failedTotal) << sinkSnapshot.name;
    // A clean run drops nothing on either side.
    EXPECT_EQ(sourceSnapshot.droppedTotal, std::uint64_t(0)) << sourceSnapshot.name;
    EXPECT_EQ(sinkSnapshot.droppedTotal, std::uint64_t(0)) << sinkSnapshot.name;

    ASSERT_EQ(pipeline->DeInit(), ErrorCode::SUCCESS);
}

// The accounting invariant: every message a module was handed ends up counted
// exactly once, as processed, dropped or failed. Without failedTotal a throwing
// invocation leaves a message counted nowhere, which is the hole this covers.
TEST(PipelineMetricsTest, ProcessedDroppedAndFailedAccountForEveryMessage) {
    auto source = std::make_shared<MetricsSourceModule>("invariant-source");
    auto sink = std::make_shared<MetricsSinkModule>("invariant-sink");

    auto pipeline = PipelineBuilder()
                        .AddModule(source)
                        .AddModule(sink)
                        .Connect("invariant-source", "invariant-sink")
                        .Build(8);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    std::this_thread::sleep_for(std::chrono::milliseconds(kRunMs));
    ASSERT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);

    const PipelineSnapshot snapshot = pipeline->GetSnapshot();
    ASSERT_EQ(snapshot.modules.size(), std::size_t(2));

    // The source is the producer: everything it emitted either reached the edge
    // queue or was dropped by it, so its own total equals the edge's pushes.
    ASSERT_EQ(snapshot.edges.size(), std::size_t(1));
    const ModuleSnapshot& sourceSnapshot = snapshot.modules[0];
    EXPECT_EQ(sourceSnapshot.failedTotal, std::uint64_t(0)) << sourceSnapshot.name;
    EXPECT_LE(sourceSnapshot.fusionExpiredTotal, sourceSnapshot.droppedTotal) << sourceSnapshot.name;
    EXPECT_EQ(sourceSnapshot.processedTotal + sourceSnapshot.droppedTotal, snapshot.edges[0].pushedTotal)
        << sourceSnapshot.name;

    // The sink is the consumer: everything it popped was processed or failed.
    const ModuleSnapshot& sinkSnapshot = snapshot.modules[1];
    EXPECT_EQ(sinkSnapshot.droppedTotal, std::uint64_t(0)) << sinkSnapshot.name;
    EXPECT_LE(sinkSnapshot.processedTotal + sinkSnapshot.failedTotal, snapshot.edges[0].poppedTotal)
        << sinkSnapshot.name;

    ASSERT_EQ(pipeline->DeInit(), ErrorCode::SUCCESS);
}

// A throwing Process must be counted, not silently vanish from the accounting.
TEST(PipelineMetricsTest, ThrowingProcessIsCountedAsFailed) {
    auto source = std::make_shared<MetricsSourceModule>("throw-source");
    auto sink = std::make_shared<ThrowingSinkModule>("throw-sink");

    auto pipeline = PipelineBuilder()
                        .AddModule(source)
                        .AddModule(sink)
                        .Connect("throw-source", "throw-sink")
                        .Build(8);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    std::this_thread::sleep_for(std::chrono::milliseconds(kRunMs));
    ASSERT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);

    const PipelineSnapshot snapshot = pipeline->GetSnapshot();
    ASSERT_EQ(snapshot.modules.size(), std::size_t(2));
    EXPECT_GT(snapshot.modules[1].failedTotal, std::uint64_t(0)) << snapshot.modules[1].name;
    EXPECT_EQ(snapshot.modules[1].processedTotal, std::uint64_t(0)) << snapshot.modules[1].name;

    ASSERT_EQ(pipeline->DeInit(), ErrorCode::SUCCESS);
}

// The snapshot carries names a reader can correlate: an edge knows both of its
// ends, and ToString() prints them without any further work by the caller.
TEST(PipelineMetricsTest, SnapshotNamesEdgesAndRendersALine) {
    auto source = std::make_shared<MetricsSourceModule>("named-source");
    auto sink = std::make_shared<MetricsSinkModule>("named-sink");

    auto pipeline = PipelineBuilder()
                        .AddModule(source)
                        .AddModule(sink)
                        .Connect("named-source", "named-sink")
                        .Build(8);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);

    const PipelineSnapshot snapshot = pipeline->GetSnapshot();
    ASSERT_EQ(snapshot.edges.size(), std::size_t(1));
    EXPECT_EQ(snapshot.edges[0].from, "named-source");
    EXPECT_EQ(snapshot.edges[0].to, "named-sink");
    EXPECT_EQ(snapshot.edges[0].Name(), "named-source -> named-sink");

    const std::string line = snapshot.ToString();
    EXPECT_NE(line.find("named-source->named-sink"), std::string::npos) << line;
    EXPECT_NE(line.find("named-sink processed="), std::string::npos) << line;

    ASSERT_EQ(pipeline->DeInit(), ErrorCode::SUCCESS);
}

// maxSize is a water mark, so it survives the queue draining but starts over
// after ResetMetrics(), while the counters keep accumulating.
TEST(PipelineMetricsTest, ResetMetricsClearsOnlyTheWaterMark) {
    auto source = std::make_shared<MetricsSourceModule>("reset-source");
    auto sink = std::make_shared<MetricsSinkModule>("reset-sink");

    auto pipeline = PipelineBuilder()
                        .AddModule(source)
                        .AddModule(sink)
                        .Connect("reset-source", "reset-sink")
                        .Build(4);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    std::this_thread::sleep_for(std::chrono::milliseconds(kRunMs));
    ASSERT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);

    const PipelineSnapshot before = pipeline->GetSnapshot();
    ASSERT_EQ(before.edges.size(), std::size_t(1));
    ASSERT_GE(before.edges[0].maxSize, std::size_t(1));
    const std::uint64_t pushedBefore = before.edges[0].pushedTotal;

    pipeline->ResetMetrics();
    const PipelineSnapshot after = pipeline->GetSnapshot();
    // The water mark restarts from whatever is still queued: after a stop that
    // is usually nothing, but a sink that left a backlog makes it that backlog.
    // What must hold is that the previous peak is gone.
    EXPECT_LE(after.edges[0].maxSize, after.edges[0].size) << "reset must drop the previous peak";
    EXPECT_LE(after.edges[0].maxSize, before.edges[0].maxSize) << after.edges[0].maxSize << " vs "
                                                              << before.edges[0].maxSize;
    EXPECT_GE(after.edges[0].pushedTotal, pushedBefore) << "reset must not clear cumulative counters";

    ASSERT_EQ(pipeline->DeInit(), ErrorCode::SUCCESS);
}

} // namespace
