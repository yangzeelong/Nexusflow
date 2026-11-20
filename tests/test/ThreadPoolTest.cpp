#include <nexusflow/Config.hpp>
#include <nexusflow/ErrorCode.hpp>
#include <nexusflow/Message.hpp>
#include <nexusflow/Module.hpp>
#include <nexusflow/Pipeline.hpp>
#include <nexusflow/PipelineBuilder.hpp>
#include <nexusflow/PipelineContext.hpp>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace {

using namespace nexusflow;

constexpr int kRunMs = 120;
constexpr int kCapacity = 16;
constexpr int kIdleEveryMessages = 64;

// Emits one message per Process() call. The short sleep every N messages keeps
// the test from spinning millions of messages through a shared slot.
class PoolSourceModule : public Module {
public:
    explicit PoolSourceModule(const std::string& name) : Module(name) {}

    std::atomic<std::uint64_t> emitted{0};

protected:
    void Process(Message& inputMessage) override {
        (void)inputMessage;
        const std::uint64_t index = emitted.fetch_add(1, std::memory_order_relaxed);
        if ((index % kIdleEveryMessages) == 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(20));
        }
        Broadcast(std::move(MakeMessage(1)));
    }
};

class PoolStageModule : public Module {
public:
    explicit PoolStageModule(const std::string& name) : Module(name) {}

protected:
    void Process(Message& message) override { Broadcast(message); }
};

class PoolSinkModule : public Module {
public:
    explicit PoolSinkModule(const std::string& name) : Module(name) {}

    std::atomic<std::uint64_t> received{0};

protected:
    void Process(Message& message) override {
        if (message.HasData()) {
            received.fetch_add(1, std::memory_order_relaxed);
        }
    }
};

// Joined sink: RunJoin() hands it a single batch element whose payload is the
// map of input edge -> Message for one joined group.
class PoolJoinedSinkModule : public Module {
public:
    explicit PoolJoinedSinkModule(const std::string& name) : Module(name) {}

    std::atomic<std::uint64_t> joinedBatches{0};

protected:
    void Process(Message&) override {}

    void ProcessBatch(std::vector<Message>& inputBatchMessages) override {
        if (inputBatchMessages.size() == 1) {
            joinedBatches.fetch_add(1, std::memory_order_relaxed);
        }
    }
};

std::unique_ptr<Pipeline> BuildLinearChain(const std::shared_ptr<PoolSourceModule>& source,
                                           const std::shared_ptr<PoolStageModule>& stage,
                                           const std::shared_ptr<PoolSinkModule>& sink) {
    return PipelineBuilder()
        .AddModule(source)
        .AddModule(stage)
        .AddModule(sink)
        .Connect(source->GetModuleName(), stage->GetModuleName())
        .Connect(stage->GetModuleName(), sink->GetModuleName())
        .Build(kCapacity);
}

} // namespace

TEST(ThreadPoolTest, TwoThreadsDeliverAcrossAllStages) {
    auto source = std::make_shared<PoolSourceModule>("source");
    auto stage = std::make_shared<PoolStageModule>("stage");
    auto sink = std::make_shared<PoolSinkModule>("sink");
    auto pipeline = BuildLinearChain(source, stage, sink);
    ASSERT_NE(pipeline, nullptr);
    pipeline->GetContext()->WithThreadPoolSize(2);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    std::this_thread::sleep_for(std::chrono::milliseconds(kRunMs));
    EXPECT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);
    EXPECT_GT(source->emitted.load(std::memory_order_relaxed), 0u);
    EXPECT_GT(sink->received.load(std::memory_order_relaxed), 0u);
}

TEST(ThreadPoolTest, SingleThreadStillMakesProgress) {
    auto source = std::make_shared<PoolSourceModule>("source");
    auto stage = std::make_shared<PoolStageModule>("stage");
    auto sink = std::make_shared<PoolSinkModule>("sink");
    auto pipeline = BuildLinearChain(source, stage, sink);
    ASSERT_NE(pipeline, nullptr);
    // One slot hosts every module: the source, the stage and the sink share a
    // thread, so a blocking push here would deadlock the whole pipeline.
    pipeline->GetContext()->WithThreadPoolSize(1);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    std::this_thread::sleep_for(std::chrono::milliseconds(kRunMs));
    EXPECT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);
    EXPECT_GT(source->emitted.load(std::memory_order_relaxed), 8u);
    EXPECT_GT(sink->received.load(std::memory_order_relaxed), 8u);
}

TEST(ThreadPoolTest, PipelineCanRestartAfterPooledStop) {
    auto source = std::make_shared<PoolSourceModule>("source");
    auto stage = std::make_shared<PoolStageModule>("stage");
    auto sink = std::make_shared<PoolSinkModule>("sink");
    auto pipeline = BuildLinearChain(source, stage, sink);
    ASSERT_NE(pipeline, nullptr);
    pipeline->GetContext()->WithThreadPoolSize(2);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    std::this_thread::sleep_for(std::chrono::milliseconds(kRunMs));
    ASSERT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);
    const std::uint64_t receivedAfterFirstRun = sink->received.load(std::memory_order_relaxed);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    std::this_thread::sleep_for(std::chrono::milliseconds(kRunMs));
    EXPECT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);
    EXPECT_GT(sink->received.load(std::memory_order_relaxed), receivedAfterFirstRun);
}

TEST(ThreadPoolTest, JoinModuleKeepsItsOwnThread) {
    auto source = std::make_shared<PoolSourceModule>("source");
    auto branchA = std::make_shared<PoolStageModule>("branch_a");
    auto branchB = std::make_shared<PoolStageModule>("branch_b");
    auto sink = std::make_shared<PoolJoinedSinkModule>("joined_sink");

    Config sinkConfig;
    sinkConfig.Add<bool>("syncInputs", true);

    auto pipeline = PipelineBuilder()
        .AddModule(source)
        .AddModule(branchA)
        .AddModule(branchB)
        .AddModule(sink)
        .Connect(source->GetModuleName(), branchA->GetModuleName())
        .Connect(source->GetModuleName(), branchB->GetModuleName())
        .Connect(branchA->GetModuleName(), sink->GetModuleName())
        .Connect(branchB->GetModuleName(), sink->GetModuleName())
        .SetModuleConfig("joined_sink", sinkConfig)
        .Build(kCapacity);
    ASSERT_NE(pipeline, nullptr);
    // The join worker blocks until a whole group arrives, so it must not be
    // hosted by a shared slot; the pool only takes the other three modules.
    pipeline->GetContext()->WithThreadPoolSize(2);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    std::this_thread::sleep_for(std::chrono::milliseconds(kRunMs));
    EXPECT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);
    EXPECT_GT(sink->joinedBatches.load(std::memory_order_relaxed), 0u);
}
