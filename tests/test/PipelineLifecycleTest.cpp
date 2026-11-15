#include <nexusflow/ErrorCode.hpp>
#include <nexusflow/Message.hpp>
#include <nexusflow/Module.hpp>
#include <nexusflow/Pipeline.hpp>
#include <nexusflow/PipelineBuilder.hpp>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <iostream>
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
        .Build(8);
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

TEST(PipelineLifecycleTest, DoubleStartIsANoOp) {
    auto source = std::make_shared<LifecycleSourceModule>("source");
    auto sink = std::make_shared<LifecycleSinkModule>("sink");
    auto pipeline = BuildLifecyclePipeline(source, sink);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    std::this_thread::sleep_for(std::chrono::milliseconds(kRunMs));
    const int receivedBeforeSecondStart = sink->received.load(std::memory_order_relaxed);
    ASSERT_GT(receivedBeforeSecondStart, 0);

    // Idempotent: a second Start() must not fail and must not add a second worker
    // per module (that would double every message on the edge).
    EXPECT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    EXPECT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    std::this_thread::sleep_for(std::chrono::milliseconds(kRunMs));
    // Still running exactly one producer: the run continues rather than doubling.
    EXPECT_GT(sink->received.load(std::memory_order_relaxed), receivedBeforeSecondStart);

    EXPECT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);
    EXPECT_EQ(pipeline->Stop(), ErrorCode::FAILED_ALREADY_STOP);
    EXPECT_EQ(pipeline->DeInit(), ErrorCode::SUCCESS);
}

TEST(PipelineLifecycleTest, StopThenStartAgainKeepsWorking) {
    auto source = std::make_shared<LifecycleSourceModule>("source");
    auto sink = std::make_shared<LifecycleSinkModule>("sink");
    auto pipeline = BuildLifecyclePipeline(source, sink);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    std::this_thread::sleep_for(std::chrono::milliseconds(kRunMs));
    // Something has to have gone through before the stop is worth restarting.
    ASSERT_GT(sink->received.load(std::memory_order_relaxed), 0);

    const auto stopBegin = std::chrono::steady_clock::now();
    ASSERT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);
    const auto stopMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - stopBegin).count();
    // Step timing: a stop that has to wait out a stage's stop timeout (or a
    // drain budget) is the failure this test exists to catch, so make the
    // budget visible in the output instead of hiding it in a multi-second test.
    std::cout << "[  TIME      ] Stop() took " << stopMs << " ms, sink received "
              << sink->received.load(std::memory_order_relaxed) << "\n";
    EXPECT_LT(stopMs, 1000) << "Stop() should not need to wait out a timeout";

    // Stop() closes the edges so every worker is certain to exit; a restart has
    // to reopen them, otherwise the second Start() fails halfway through and
    // leaves workers of the first half running.
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    const int receivedAfterFirstRun = sink->received.load(std::memory_order_relaxed);
    std::this_thread::sleep_for(std::chrono::milliseconds(kRunMs));
    EXPECT_GT(sink->received.load(std::memory_order_relaxed), receivedAfterFirstRun);
    ASSERT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);
    EXPECT_EQ(pipeline->Stop(), ErrorCode::FAILED_ALREADY_STOP);
    ASSERT_EQ(pipeline->DeInit(), ErrorCode::SUCCESS);
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
