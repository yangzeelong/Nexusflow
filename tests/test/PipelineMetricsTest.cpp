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
    ASSERT_EQ(metrics.queues.size(), std::size_t(1));
    EXPECT_EQ(metrics.queues[0].capacity, std::size_t(7));
    EXPECT_EQ(metrics.queues[0].pushed, std::uint64_t(0));
    ASSERT_EQ(metrics.modules.size(), std::size_t(2));

    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    std::this_thread::sleep_for(std::chrono::milliseconds(kRunMs));
    ASSERT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);

    metrics = pipeline->GetMetrics();
    ASSERT_EQ(metrics.queues.size(), std::size_t(1));
    EXPECT_EQ(metrics.queues[0].capacity, std::size_t(7));
    EXPECT_GT(metrics.queues[0].pushed, std::uint64_t(0));
    EXPECT_GT(metrics.queues[0].popped, std::uint64_t(0));
    EXPECT_GE(metrics.queues[0].highWaterMark, std::size_t(1));
    EXPECT_LE(metrics.queues[0].highWaterMark, std::size_t(7));
    EXPECT_NE(metrics.queues[0].name.find("metrics-source"), std::string::npos);

    ASSERT_EQ(metrics.modules.size(), std::size_t(2));
    for (const ModuleMetrics& module : metrics.modules) {
        EXPECT_GT(module.processed, std::uint64_t(0)) << module.name;
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
    ASSERT_EQ(metrics.queues.size(), std::size_t(1));
    EXPECT_EQ(metrics.queues[0].capacity, std::size_t(3));
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
class MetricsIdleSinkModule : public Module {
public:
    explicit MetricsIdleSinkModule(const std::string& name) : Module(name) {}

protected:
    void Process(Message&) override {}
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
        EXPECT_EQ(module.dropped, std::uint64_t(0)) << module.name;
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
            totalDropped += module.dropped;
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
        totalProcessed += module.processed;
    }
    EXPECT_GT(totalProcessed, std::uint64_t(0));
    EXPECT_GT(totalDropped, std::uint64_t(0));

    // Drops are charged to the module that tried to send; the idle sink never
    // tried to send anything, so it must stay at zero.
    ASSERT_EQ(metrics.modules.size(), std::size_t(2));
    for (const ModuleMetrics& module : metrics.modules) {
        if (module.name.find("idle-sink") != std::string::npos) {
            EXPECT_EQ(module.dropped, std::uint64_t(0)) << module.name;
        }
    }

    // Accounting is per message: a processed message was popped from the edge
    // queue, a dropped one was refused by it, so both are visible and neither is
    // double counted. pushed can only grow by the messages the source emitted,
    // so it is at least the sink's processed count.
    ASSERT_EQ(metrics.queues.size(), std::size_t(1));
    EXPECT_GE(metrics.queues[0].pushed, metrics.modules[1].processed);
    EXPECT_GE(metrics.queues[0].popped, metrics.modules[1].processed);
}

} // namespace
