#include <nexusflow/Message.hpp>
#include <nexusflow/Module.hpp>
#include <nexusflow/Pipeline.hpp>
#include <nexusflow/PipelineBuilder.hpp>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

namespace {

using namespace nexusflow;

// A boundary source that never produces on its own: Pipeline::Push() is its only
// input. The worker still runs (a source always gets a loop) but idles.
class PassiveSourceModule : public Module {
public:
    explicit PassiveSourceModule(const std::string& name) : Module(name) {}

    bool IsPassiveSource() const override { return true; }

protected:
    void Process(Message& inputMessage) override { (void)inputMessage; }
};

// A regular source: it produces on its own, so Push() must refuse it.
class ActiveSourceModule : public Module {
public:
    explicit ActiveSourceModule(const std::string& name) : Module(name) {}

protected:
    void Process(Message& inputMessage) override {
        (void)inputMessage;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        Broadcast(MakeMessage(1));
    }
};

class CountingSinkModule : public Module {
public:
    explicit CountingSinkModule(const std::string& name) : Module(name) {}

    std::atomic<int> received{0};

protected:
    void Process(Message& inputMessage) override {
        if (inputMessage.HasData()) {
            received.fetch_add(1, std::memory_order_relaxed);
        }
    }
};

// Sink that parks inside its first Process() call until the test opens the gate,
// so the upstream edge can fill deterministically.
class GateSinkModule : public Module {
public:
    explicit GateSinkModule(const std::string& name) : Module(name) {}

    std::atomic<int> received{0};

    void Release() {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_open = true;
        m_cv.notify_all();
    }

protected:
    void Process(Message& inputMessage) override {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_cv.wait(lock, [this] { return m_open; });
        if (inputMessage.HasData()) {
            received.fetch_add(1, std::memory_order_relaxed);
        }
    }

private:
    std::mutex m_mutex;
    std::condition_variable m_cv;
    bool m_open = false;
};

std::unique_ptr<Pipeline> BuildTwoStagePipeline(const std::shared_ptr<Module>& source,
                                                const std::shared_ptr<Module>& sink, int capacity) {
    return PipelineBuilder()
        .AddModule(source)
        .AddModule(sink)
        .Connect(source->GetModuleName(), sink->GetModuleName())
        .Build(capacity);
}

bool WaitForReceived(const CountingSinkModule& sink, int expected, int timeoutMs) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        if (sink.received.load(std::memory_order_relaxed) >= expected) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return sink.received.load(std::memory_order_relaxed) >= expected;
}

} // namespace

TEST(PipelinePushTest, PushBeforeStartIsRejected) {
    auto source = std::make_shared<PassiveSourceModule>("source");
    auto sink = std::make_shared<CountingSinkModule>("sink");
    auto pipeline = BuildTwoStagePipeline(source, sink, 8);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);

    EXPECT_EQ(pipeline->Push("source", MakeMessage(1)), PushResult::kNotRunning);
    EXPECT_EQ(pipeline->TryPush("source", MakeMessage(1)), PushResult::kNotRunning);
    EXPECT_EQ(pipeline->DeInit(), ErrorCode::SUCCESS);
}

TEST(PipelinePushTest, PushDeliversEveryMessageToTheSink) {
    auto source = std::make_shared<PassiveSourceModule>("source");
    auto sink = std::make_shared<CountingSinkModule>("sink");
    auto pipeline = BuildTwoStagePipeline(source, sink, 8);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);

    constexpr int kMessages = 200;
    for (int i = 0; i < kMessages; ++i) {
        ASSERT_EQ(pipeline->Push("source", MakeMessage(i)), PushResult::kOk);
    }
    EXPECT_TRUE(WaitForReceived(*sink, kMessages, 2000));
    EXPECT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);
    EXPECT_EQ(sink->received.load(std::memory_order_relaxed), kMessages);
}

TEST(PipelinePushTest, PushRejectsUnknownAndNonSourceTargets) {
    auto source = std::make_shared<PassiveSourceModule>("source");
    auto sink = std::make_shared<CountingSinkModule>("sink");
    auto pipeline = BuildTwoStagePipeline(source, sink, 8);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);

    EXPECT_EQ(pipeline->Push("does-not-exist", MakeMessage(1)), PushResult::kUnknownModule);
    // The sink has an input edge: it is not a boundary source.
    EXPECT_EQ(pipeline->Push("sink", MakeMessage(1)), PushResult::kNotSource);
    EXPECT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);
}

TEST(PipelinePushTest, PushRejectsAnActiveSource) {
    auto source = std::make_shared<ActiveSourceModule>("source");
    auto sink = std::make_shared<CountingSinkModule>("sink");
    auto pipeline = BuildTwoStagePipeline(source, sink, 8);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);

    EXPECT_EQ(pipeline->Push("source", MakeMessage(1)), PushResult::kNotPassiveSource);
    EXPECT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);
}

TEST(PipelinePushTest, TryPushReportsFullQueue) {
    auto source = std::make_shared<PassiveSourceModule>("source");
    auto sink = std::make_shared<GateSinkModule>("sink");
    auto pipeline = BuildTwoStagePipeline(source, sink, 1);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);

    // The gate sink takes at most one message out of the queue and then parks, so
    // one message waits in the queue. Keep offering messages until the queue
    // reports full; the loop also tolerates the sink not having woken up yet.
    bool sawFull = false;
    for (int i = 0; i < 100 && !sawFull; ++i) {
        const PushResult result = pipeline->TryPush("source", MakeMessage(i));
        if (result == PushResult::kQueueFull) {
            sawFull = true;
        } else {
            EXPECT_EQ(result, PushResult::kOk);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_TRUE(sawFull);

    sink->Release();
    EXPECT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);
    EXPECT_GE(sink->received.load(std::memory_order_relaxed), 1);
}

TEST(PipelinePushTest, PushAfterStopIsRejected) {
    auto source = std::make_shared<PassiveSourceModule>("source");
    auto sink = std::make_shared<CountingSinkModule>("sink");
    auto pipeline = BuildTwoStagePipeline(source, sink, 8);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Push("source", MakeMessage(1)), PushResult::kOk);
    ASSERT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);

    EXPECT_EQ(pipeline->Push("source", MakeMessage(2)), PushResult::kNotRunning);
    EXPECT_EQ(pipeline->TryPush("source", MakeMessage(2)), PushResult::kNotRunning);
}
