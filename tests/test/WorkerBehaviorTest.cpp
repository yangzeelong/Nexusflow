#include <nexusflow/ErrorCode.hpp>
#include <nexusflow/Message.hpp>
#include <nexusflow/Module.hpp>
#include <nexusflow/Pipeline.hpp>
#include <nexusflow/PipelineBuilder.hpp>

#include <gtest/gtest.h>

#include "../../src/base/Define.hpp"
#include "../../src/core/Worker.hpp"
#include <nexusflow/Config.hpp>

#include <nexusflow/Config.hpp>
#include <nexusflow/Logging.hpp>

#include <atomic>
#include <chrono>
#include <mutex>
#include <vector>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

namespace {

using namespace nexusflow;

// Temporarily raises the log level so an expected error message is not printed
// into the test output. Restores the previous level when it goes out of scope.
class ScopedLogSilencer {
public:
    explicit ScopedLogSilencer(nexusflow::log::Level level) : m_previous(nexusflow::log::GetLogLevel()) {
        nexusflow::log::SetLogLevel(level);
    }
    ~ScopedLogSilencer() { nexusflow::log::SetLogLevel(m_previous); }

    ScopedLogSilencer(const ScopedLogSilencer&) = delete;
    ScopedLogSilencer& operator=(const ScopedLogSilencer&) = delete;

private:
    nexusflow::log::Level m_previous;
};

// Emits one message only when told to, so ProcessTimed can be checked for both
// outcomes without a pipeline.
class ToggleOutputModule : public Module {
public:
    explicit ToggleOutputModule(const std::string& name) : Module(name) {}

    bool emit = true;

protected:
    void Process(Message&) override {
        if (emit) {
            Broadcast(MakeMessage(1));
        }
    }
};

// Throws on its first invocation and then behaves like a normal idle source.
class ThrowingSourceModule : public Module {
public:
    explicit ThrowingSourceModule(const std::string& name) : Module(name) {}

    std::atomic<int> processCalls{0};

protected:
    void Process(Message&) override {
        const int call = processCalls.fetch_add(1, std::memory_order_relaxed) + 1;
        if (call == 1) {
            throw std::runtime_error("intentional module failure");
        }
    }
};

// Never emits anything: exercises the source-module idle backoff.
class IdleSourceModule : public Module {
public:
    explicit IdleSourceModule(const std::string& name) : Module(name) {}

    std::atomic<int> processCalls{0};

protected:
    void Process(Message&) override { processCalls.fetch_add(1, std::memory_order_relaxed); }
};

class CountingSinkModule : public Module {
public:
    explicit CountingSinkModule(const std::string& name) : Module(name) {}

protected:
    void Process(Message&) override {}
};

} // namespace

TEST(WorkerBehaviorTest, ProcessTimedReportsWhetherItProducedOutput) {
    ToggleOutputModule module("toggle");
    Message message;

    module.emit = true;
    EXPECT_TRUE(module.ProcessTimed(message));

    module.emit = false;
    EXPECT_FALSE(module.ProcessTimed(message));
}

TEST(WorkerBehaviorTest, ThrowingModuleDoesNotKillTheWorker) {
    // The module failure below is expected, so keep the error-level log out of
    // the test output.
    const ScopedLogSilencer silenceExpectedError(nexusflow::log::Level::OFF);

    auto source = std::make_shared<ThrowingSourceModule>("throwing_source");
    auto sink = std::make_shared<CountingSinkModule>("sink");
    auto pipeline = PipelineBuilder()
                        .AddModule(source)
                        .AddModule(sink)
                        .Connect(source->GetModuleName(), sink->GetModuleName())
                        .Build();
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);

    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    EXPECT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);
    // Without the worker-level guard the first throw would have terminated the
    // process; reaching here with more than one call proves the loop survived.
    EXPECT_GT(source->processCalls.load(std::memory_order_relaxed), 1);
}

TEST(WorkerBehaviorTest, IdleSourceBacksOffInsteadOfSpinning) {
    auto source = std::make_shared<IdleSourceModule>("idle_source");
    auto sink = std::make_shared<CountingSinkModule>("idle_sink");
    auto pipeline = PipelineBuilder()
                        .AddModule(source)
                        .AddModule(sink)
                        .Connect(source->GetModuleName(), sink->GetModuleName())
                        .Build();
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);

    const int calls = source->processCalls.load(std::memory_order_relaxed);
    EXPECT_GT(calls, 0);
    // A hot loop would make hundreds of thousands of calls in 50 ms; the 1 ms
    // default backoff keeps this in the tens. The bound is loose on purpose so
    // a slow CI machine does not make the test flaky.
    EXPECT_LT(calls, 5000);
}


namespace {

// Records the size of every batch the worker hands to it, so a test can check
// that the per-module maxBatchSize knob is actually honoured.
class BatchRecordingModule : public Module {
public:
    explicit BatchRecordingModule(const std::string& name) : Module(name) {}

    std::mutex mutex;
    std::vector<std::size_t> batchSizes;

protected:
    void Process(Message& inputMessage) override { (void)inputMessage; }

    void ProcessBatch(std::vector<Message>& inputBatchMessages) override {
        std::lock_guard<std::mutex> guard(mutex);
        batchSizes.push_back(inputBatchMessages.size());
    }
};

} // namespace

TEST(WorkerBehaviorTest, ConfiguredBatchSizeIsRespected) {
    auto module = std::make_shared<BatchRecordingModule>("batch_recorder");
    Config config;
    config.Add<int>("maxBatchSize", 2);
    config.Add<int>("batchTimeoutMs", 5);
    ViewPtr<Config> configView(&config);

    MessageQueue queue(16);
    core::Worker worker(module, configView);
    worker.AddQueue("input", makeViewPtr(&queue));

    // Fill the queue before starting the worker, so the very first pull already
    // has more messages available than the configured batch size.
    for (int i = 0; i < 10; ++i) {
        queue.push(MakeMessage(i));
    }

    std::thread workerThread([&worker]() { worker.WorkLoop(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    worker.Stop();
    queue.shutdown();
    workerThread.join();

    std::lock_guard<std::mutex> guard(module->mutex);
    ASSERT_FALSE(module->batchSizes.empty());
    EXPECT_EQ(module->batchSizes.front(), 2u);
    for (std::size_t size : module->batchSizes) {
        EXPECT_LE(size, 2u);
    }
}
