#include <nexusflow/ErrorCode.hpp>
#include <nexusflow/Message.hpp>
#include <nexusflow/Module.hpp>
#include <nexusflow/Pipeline.hpp>
#include <nexusflow/PipelineBuilder.hpp>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>

namespace {

using namespace nexusflow;

constexpr int kRunMs = 40;
constexpr int kSinkProcessMs = 2;

// Source module: emits one message per Process() call. Deliberately keeps the
// worker busy so the pipeline owns live threads when the test drops it.
class LifecycleSourceModule : public Module {
public:
    explicit LifecycleSourceModule(const std::string& name) : Module(name) {}

    std::atomic<int> processCalls{0};

protected:
    void Process(Message& inputMessage) override {
        (void)inputMessage;
        processCalls.fetch_add(1, std::memory_order_relaxed);
        Broadcast(std::move(MakeMessage(1)));
    }
};

// Sink module: slow on purpose so the upstream queue fills and the source
// blocks inside Broadcast(), which is the shutdown path we want to exercise.
class LifecycleSinkModule : public Module {
public:
    explicit LifecycleSinkModule(const std::string& name) : Module(name) {}

    std::atomic<int> received{0};

protected:
    void Process(Message& inputMessage) override {
        if (inputMessage.HasData()) {
            received.fetch_add(1, std::memory_order_relaxed);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(kSinkProcessMs));
    }
};

std::unique_ptr<Pipeline> BuildLifecyclePipeline(const std::shared_ptr<LifecycleSourceModule>& source,
                                                 const std::shared_ptr<LifecycleSinkModule>& sink) {
    return PipelineBuilder()
        .AddModule(source)
        .AddModule(sink)
        .Connect(source->GetModuleName(), sink->GetModuleName())
        .Build();
}

} // namespace

TEST(PipelineLifecycleTest, DestructorJoinsRunningWorkers) {
    auto source = std::make_shared<LifecycleSourceModule>("source");
    auto sink = std::make_shared<LifecycleSinkModule>("sink");
    {
        auto pipeline = BuildLifecyclePipeline(source, sink);
        ASSERT_NE(pipeline, nullptr);
        ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
        ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
        std::this_thread::sleep_for(std::chrono::milliseconds(kRunMs));
        // Deliberately no Stop(): ~Pipeline must join both worker threads,
        // otherwise ~std::thread calls std::terminate and the test aborts.
    }
    EXPECT_GT(source->processCalls.load(std::memory_order_relaxed), 0);
}

TEST(PipelineLifecycleTest, StartWithoutInitIsRejected) {
    auto source = std::make_shared<LifecycleSourceModule>("source");
    auto sink = std::make_shared<LifecycleSinkModule>("sink");
    auto pipeline = BuildLifecyclePipeline(source, sink);
    ASSERT_NE(pipeline, nullptr);
    EXPECT_EQ(pipeline->Start(), ErrorCode::UNINITIALIZED_ERROR);
    EXPECT_EQ(pipeline->Stop(), ErrorCode::FAILED_ALREADY_STOP);
}

TEST(PipelineLifecycleTest, StopBeforeStartReportsAlreadyStopped) {
    auto source = std::make_shared<LifecycleSourceModule>("source");
    auto sink = std::make_shared<LifecycleSinkModule>("sink");
    auto pipeline = BuildLifecyclePipeline(source, sink);
    ASSERT_NE(pipeline, nullptr);
    EXPECT_EQ(pipeline->Stop(), ErrorCode::FAILED_ALREADY_STOP);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    EXPECT_EQ(pipeline->Stop(), ErrorCode::FAILED_ALREADY_STOP);
    EXPECT_EQ(pipeline->DeInit(), ErrorCode::SUCCESS);
}

TEST(PipelineLifecycleTest, DoubleStartAndDoubleStopAreReported) {
    auto source = std::make_shared<LifecycleSourceModule>("source");
    auto sink = std::make_shared<LifecycleSinkModule>("sink");
    auto pipeline = BuildLifecyclePipeline(source, sink);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    EXPECT_EQ(pipeline->Start(), ErrorCode::FAILED_ALREADY_START);
    EXPECT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);
    EXPECT_EQ(pipeline->Stop(), ErrorCode::FAILED_ALREADY_STOP);
    EXPECT_EQ(pipeline->DeInit(), ErrorCode::SUCCESS);
}

TEST(PipelineLifecycleTest, DestroyAfterExplicitStopIsSafe) {
    auto source = std::make_shared<LifecycleSourceModule>("source");
    auto sink = std::make_shared<LifecycleSinkModule>("sink");
    {
        auto pipeline = BuildLifecyclePipeline(source, sink);
        ASSERT_NE(pipeline, nullptr);
        ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
        ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
        std::this_thread::sleep_for(std::chrono::milliseconds(kRunMs / 2));
        ASSERT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);
        ASSERT_EQ(pipeline->DeInit(), ErrorCode::SUCCESS);
    }
    EXPECT_GT(source->processCalls.load(std::memory_order_relaxed), 0);
}
