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

} // namespace
