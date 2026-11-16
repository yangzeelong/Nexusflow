#include <nexusflow/ErrorCode.hpp>
#include <nexusflow/Message.hpp>
#include <nexusflow/Module.hpp>
#include <nexusflow/Pipeline.hpp>
#include <nexusflow/PipelineBuilder.hpp>

#include "../../src/base/Define.hpp"

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

/// Emits a fixed number of messages and then goes quiet, so a Stop() either
/// delivers the bounded backlog this creates or throws it away.
class BurstSource : public Module {
public:
    explicit BurstSource(const std::string& name) : Module(name) {}

protected:
    void Process(Message&) override {
        if (m_emitted < kEmitCount) {
            ++m_emitted;
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
/// edges when Stop() is called.
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

/// Waits until the edge between the first two stages has accepted everything the
/// source will ever emit, so the test never races the producer.
bool WaitForSourceToFinish(Pipeline& pipeline, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        const PipelineSnapshot snapshot = pipeline.GetSnapshot();
        if (!snapshot.edges.empty() && snapshot.edges[0].pushedTotal >= static_cast<std::uint64_t>(kEmitCount)) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return false;
}

TEST(PipelineDrainTest, StopDeliversWhateverTheSinkHasNotTakenYet) {
    auto source = std::make_shared<BurstSource>("drain-source");
    auto sink = std::make_shared<SlowSink>("drain-sink");
    auto pipeline = PipelineBuilder()
                        .AddModule(source)
                        .AddModule(sink)
                        .Connect("drain-source", "drain-sink")
                        .Build(16);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);

    ASSERT_TRUE(WaitForSourceToFinish(*pipeline, std::chrono::milliseconds(2000)));
    // The source is done but the 1 ms sink cannot possibly have taken all eight
    // messages in the same breath, so there is a backlog for Stop() to deliver.
    ASSERT_LT(sink->received.load(std::memory_order_relaxed), kEmitCount)
        << "the test needs a backlog, so the sink must still be behind";

    ASSERT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);

    const PipelineSnapshot after = pipeline->GetSnapshot();
    ASSERT_EQ(after.edges.size(), std::size_t(1));
    EXPECT_EQ(after.edges[0].size, std::size_t(0)) << "Stop() left messages behind in the edge";
    EXPECT_EQ(after.edges[0].poppedTotal, after.edges[0].pushedTotal);
    EXPECT_EQ(sink->received.load(std::memory_order_relaxed), kEmitCount)
        << "Stop() dropped messages it was supposed to deliver";
}

TEST(PipelineDrainTest, StopDrainsThroughAnIntermediateStage) {
    auto source = std::make_shared<BurstSource>("chain-source");
    auto relay = std::make_shared<SlowRelay>("chain-relay");
    auto sink = std::make_shared<SlowSink>("chain-sink");
    auto pipeline = PipelineBuilder()
                        .AddModule(source)
                        .AddModule(relay)
                        .AddModule(sink)
                        .Connect("chain-source", "chain-relay")
                        .Connect("chain-relay", "chain-sink")
                        .Build(16);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);

    ASSERT_TRUE(WaitForSourceToFinish(*pipeline, std::chrono::milliseconds(2000)));
    ASSERT_LT(sink->received.load(std::memory_order_relaxed), kEmitCount);

    ASSERT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);

    // This only holds if the teardown is driven producer-first: the relay can
    // only stop once its own input is closed and empty, and the sink can only
    // finish once the relay has handed everything over. Stopping both at once
    // leaves the relay's backlog with nobody to take it.
    EXPECT_EQ(relay->forwarded.load(std::memory_order_relaxed), kEmitCount);
    EXPECT_EQ(sink->received.load(std::memory_order_relaxed), kEmitCount);
    for (const EdgeSnapshot& edge : pipeline->GetSnapshot().edges) {
        EXPECT_EQ(edge.size, std::size_t(0)) << edge.Name();
        EXPECT_EQ(edge.poppedTotal, edge.pushedTotal) << edge.Name();
    }
}

}    // namespace
