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

// A source that waits for its own work (a decoder blocking on the next frame):
// it holds each call for a while, then optionally reports that the wait is what
// paced it. `reportWait == false` makes the same module keep quiet, which leaves
// the worker's own sourceIdleMs as the only thing setting the pace.
class BlockingSourceModule : public Module {
public:
    BlockingSourceModule(const std::string& name, bool reportWait) : Module(name), m_reportWait(reportWait) {}

    std::atomic<int> processCalls{0};

protected:
    void Process(Message&) override {
        processCalls.fetch_add(1, std::memory_order_relaxed);
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        if (m_reportWait) {
            NoteBlockedWait();
        }
    }

private:
    bool m_reportWait;
};

// Runs one source/sink pipeline until the source has been invoked `callLimit`
// times and reports how many milliseconds that cost. Returns -1 on setup failure.
long long RunBlockingSource(int callLimit, bool reportWait) {
    auto source = std::make_shared<BlockingSourceModule>("blocking_source", reportWait);
    auto sink = std::make_shared<CountingSinkModule>("blocking_sink");
    auto pipeline = PipelineBuilder()
                        .AddModule(source)
                        .AddModule(sink)
                        .Connect(source->GetModuleName(), sink->GetModuleName())
                        .Build(8);
    if (!pipeline || pipeline->Init() != ErrorCode::SUCCESS || pipeline->Start() != ErrorCode::SUCCESS) {
        return -1;
    }
    const auto started = std::chrono::steady_clock::now();
    const auto deadline = started + std::chrono::seconds(20);
    while (source->processCalls.load(std::memory_order_relaxed) < callLimit &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    const long long elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started).count();
    pipeline->Stop();
    return elapsed;
}

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
                        .Build(8);
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
    nexusflow::Config idleConfig;
    idleConfig.Add<int>("sourceIdleMs", 5);
    auto pipeline = PipelineBuilder()
                        .AddModule(source)
                        .AddModule(sink)
                        .SetModuleConfig("idle_source", idleConfig)
                        .Connect(source->GetModuleName(), sink->GetModuleName())
                        .Build(8);
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

TEST(WorkerBehaviorTest, SourceThatBlocksDoesNotGetASecondWait) {
    // Both runs make the same number of calls; the only difference is whether
    // the module reports that it already waited for its own work. The run that
    // reports it must not be slowed down again by the worker's backoff.
    const long long blocked = RunBlockingSource(10, true);
    ASSERT_GE(blocked, 0);
    const long long silent = RunBlockingSource(10, false);
    ASSERT_GE(silent, 0);
    EXPECT_LT(blocked, silent);
}
