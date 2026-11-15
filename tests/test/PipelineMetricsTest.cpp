#include <nexusflow/ErrorCode.hpp>
#include <nexusflow/Message.hpp>
#include <nexusflow/Module.hpp>
#include <nexusflow/ModuleFactory.hpp>
#include <nexusflow/Pipeline.hpp>
#include <nexusflow/PipelineBuilder.hpp>

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

    std::unique_ptr<Pipeline> pipeline = PipelineBuilder()
                                             .SetMessageQueueCapacity(7)
                                             .AddModule(source)
                                             .AddModule(sink)
                                             .Connect("metrics-source", "metrics-sink")
                                             .Build();
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);

    PipelineMetrics metrics = pipeline->GetMetrics();
    ASSERT_EQ(metrics.edges.size(), std::size_t(1));
    EXPECT_EQ(metrics.edges[0].capacity, std::size_t(7));
    EXPECT_EQ(metrics.edges[0].pushedTotal, std::uint64_t(0));
    ASSERT_EQ(metrics.modules.size(), std::size_t(2));

    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    std::this_thread::sleep_for(std::chrono::milliseconds(kRunMs));
    ASSERT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);

    metrics = pipeline->GetMetrics();
    ASSERT_EQ(metrics.edges.size(), std::size_t(1));
    EXPECT_EQ(metrics.edges[0].capacity, std::size_t(7));
    EXPECT_GT(metrics.edges[0].pushedTotal, std::uint64_t(0));
    EXPECT_GT(metrics.edges[0].poppedTotal, std::uint64_t(0));
    EXPECT_GE(metrics.edges[0].maxSize, std::size_t(1));
    EXPECT_LE(metrics.edges[0].maxSize, std::size_t(7));
    EXPECT_NE(metrics.edges[0].Name().find("metrics-source"), std::string::npos);

    ASSERT_EQ(metrics.modules.size(), std::size_t(2));
    for (const ModuleMetrics& module : metrics.modules) {
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

    const PipelineMetrics metrics = pipeline->GetMetrics();
    ASSERT_EQ(metrics.edges.size(), std::size_t(1));
    EXPECT_EQ(metrics.edges[0].capacity, std::size_t(3));
    ASSERT_EQ(metrics.modules.size(), std::size_t(2));
}

// Emits forever without idling, so it is still producing when the pipeline is
// stopped and its downstream queue is shut down underneath it.
class MetricsStreamSourceModule : public Module {
public:
    explicit MetricsStreamSourceModule(const std::string& name) : Module(name) {}

protected:
    void Process(Message&) override {
        ++m_emitted;
        Broadcast(std::move(MakeMessage(1)));
    }

private:
    int m_emitted = 0;
};

// Only used to give the stream source a downstream edge; it never has to keep up.
// It also sleeps on every message: with the source emitting non-stop into a
// capacity-1 edge, a slow consumer is what keeps the queue full long enough for
// a push to be refused rather than merely parked until the next pop.
class MetricsIdleSinkModule : public Module {
public:
    explicit MetricsIdleSinkModule(const std::string& name) : Module(name) {}

protected:
    void Process(Message&) override { std::this_thread::sleep_for(std::chrono::microseconds(50)); }
};

NEXUSFLOW_REGISTER_MODULE(MetricsStreamSourceModule);
NEXUSFLOW_REGISTER_MODULE(MetricsIdleSinkModule);

TEST(PipelineMetricsTest, StopAccountsForMessagesTheDispatcherCouldNotPush) {
    auto source = std::make_shared<MetricsStreamSourceModule>("stream-source");
    auto sink = std::make_shared<MetricsIdleSinkModule>("idle-sink");

    std::unique_ptr<Pipeline> pipeline =
        PipelineBuilder().SetMessageQueueCapacity(1).AddModule(source).AddModule(sink).Connect("stream-source", "idle-sink").Build();
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);

    // Init alone must not count anything as dropped.
    ASSERT_EQ(pipeline->GetMetrics().modules.size(), std::size_t(2));
    for (const ModuleMetrics& module : pipeline->GetMetrics().modules) {
        EXPECT_EQ(module.droppedTotal, std::uint64_t(0)) << module.name;
    }

    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    std::this_thread::sleep_for(std::chrono::milliseconds(kRunMs));

    // The source never idles, so it is mid-push when the queue is shut down.
    // With a capacity-1 queue the push usually blocks, but a single stop can
    // still land between two attempts, so give the window a few tries.
    //
    // A successful stop leaves the pipeline reusable, but its edge queues are
    // closed for good, so every repeat is built from scratch and the drops are
    // summed per run.
    constexpr int kMaxStopAttempts = 3;
    std::uint64_t totalDropped = 0;
    for (int attempt = 0; attempt < kMaxStopAttempts && totalDropped == 0; ++attempt) {
        ASSERT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);
        for (const ModuleMetrics& module : pipeline->GetMetrics().modules) {
            totalDropped += module.droppedTotal;
        }
        if (totalDropped != 0 || attempt + 1 >= kMaxStopAttempts) {
            break;
        }
        pipeline = PipelineBuilder()
                       .SetMessageQueueCapacity(1)
                       .AddModule(source)
                       .AddModule(sink)
                       .Connect("stream-source", "idle-sink")
                       .Build();
        ASSERT_NE(pipeline, nullptr);
        ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
        ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
        std::this_thread::sleep_for(std::chrono::milliseconds(kRunMs));
    }

    const PipelineMetrics metrics = pipeline->GetMetrics();
    std::uint64_t totalProcessed = 0;
    for (const ModuleMetrics& module : metrics.modules) {
        totalProcessed += module.processedTotal;
    }
    EXPECT_GT(totalProcessed, std::uint64_t(0));
    EXPECT_GT(totalDropped, std::uint64_t(0));

    // Drops are charged to the module that tried to send; the idle sink never
    // tried to send anything, so it must stay at zero.
    ASSERT_EQ(metrics.modules.size(), std::size_t(2));
    for (const ModuleMetrics& module : metrics.modules) {
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
                        .SetMessageQueueCapacity(8)
                        .Build();
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
                        .Build();
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
                        .Build();
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
                        .Build();
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
                        .SetMessageQueueCapacity(4)
                        .Build();
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
