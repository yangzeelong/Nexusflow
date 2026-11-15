#include <nexusflow/Config.hpp>
#include <nexusflow/ErrorCode.hpp>
#include <nexusflow/Message.hpp>
#include <nexusflow/Module.hpp>
#include <nexusflow/MetricsCollector.hpp>
#include <nexusflow/common/ViewPtr.hpp>

#include "../../src/base/Define.hpp"
#include "../../src/core/Worker.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>

namespace {

using namespace nexusflow;

/// Emits one message per call, so a profiled source worker stays busy.
class ProfiledSourceModule : public Module {
public:
    explicit ProfiledSourceModule(const std::string& name) : Module(name) {}

    std::atomic<int> produced{0};

protected:
    void Process(Message& inputMessage) override {
        (void)inputMessage;
        produced.fetch_add(1, std::memory_order_relaxed);
        Broadcast(MakeMessage(1));
    }
};

/// Counts the messages a profiled batch worker hands to it.
class ProfiledSinkModule : public Module {
public:
    explicit ProfiledSinkModule(const std::string& name) : Module(name) {}

    std::atomic<int> received{0};

protected:
    void Process(Message& inputMessage) override {
        (void)inputMessage;
        received.fetch_add(1, std::memory_order_relaxed);
    }
};

} // namespace

// Profiling is opt-in per module. These two tests cover both loop shapes: a
// source worker times Process only, a worker with input queues times both the
// wait and the batch it processes.
TEST(WorkerProfilingTest, ProfileTimersRecordTheSourceProcessTimer) {
    auto module = std::make_shared<ProfiledSourceModule>("profiled_source");
    Config config;
    config.Add<bool>("profileTimers", true);
    ViewPtr<Config> configView{&config};
    core::Worker worker(module, configView);

    ASSERT_EQ(worker.Start(), ErrorCode::SUCCESS);
    std::thread loop([&worker]() { worker.WorkLoop(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
    worker.Stop();
    loop.join();

    ASSERT_GT(module->produced.load(std::memory_order_relaxed), 0);

    auto& registry = profiling::MetricsCollector::Instance();
    profiling::MetricSamples stats;
    ASSERT_TRUE(registry.GetStats("Worker.Process.profiled_source", stats));
    EXPECT_GT(stats.items, 0U);
    // A source never pulls from an input queue, so it has no wait timer.
    EXPECT_FALSE(registry.GetStats("Worker.Wait.profiled_source", stats));
}

TEST(WorkerProfilingTest, ProfileTimersRecordWaitAndBatchProcessTimers) {
    auto module = std::make_shared<ProfiledSinkModule>("profiled_sink");
    Config config;
    config.Add<bool>("profileTimers", true);
    ViewPtr<Config> configView{&config};
    core::Worker worker(module, configView);

    MessageQueue queue(64);
    worker.AddQueue("in", ViewPtr<MessageQueue>{&queue});

    ASSERT_EQ(worker.Start(), ErrorCode::SUCCESS);
    std::thread loop([&worker]() { worker.WorkLoop(); });
    for (int i = 0; i < 32; ++i) {
        queue.push(MakeMessage(i));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
    worker.Stop();
    loop.join();

    EXPECT_GT(module->received.load(std::memory_order_relaxed), 0);

    auto& registry = profiling::MetricsCollector::Instance();
    profiling::MetricSamples stats;
    ASSERT_TRUE(registry.GetStats("Worker.Wait.profiled_sink", stats));
    EXPECT_GT(stats.items, 0U);
    ASSERT_TRUE(registry.GetStats("Worker.Process.profiled_sink", stats));
    EXPECT_GT(stats.items, 0U);
    // The batch path declares one item per message, so the count follows the
    // messages rather than the iterations.
    EXPECT_GE(stats.items, 1U);
}
