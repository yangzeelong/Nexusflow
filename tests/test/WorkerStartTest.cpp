#include <nexusflow/Config.hpp>
#include <nexusflow/ErrorCode.hpp>
#include <nexusflow/Message.hpp>
#include <nexusflow/Module.hpp>
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

class CountingStartStopModule : public Module {
public:
    explicit CountingStartStopModule(const std::string& name) : Module(name) {}

    std::atomic<int> calls{0};

protected:
    void Process(Message& inputMessage) override {
        (void)inputMessage;
        calls.fetch_add(1, std::memory_order_relaxed);
    }
};

} // namespace

TEST(WorkerStartTest, StartAndStopCanBeRepeated) {
    auto module = std::make_shared<CountingStartStopModule>("start_stop");
    Config config;
    ViewPtr<Config> configView{&config};
    core::Worker worker(module, configView);

    EXPECT_EQ(worker.Start(), ErrorCode::SUCCESS);
    EXPECT_EQ(worker.Stop(), ErrorCode::SUCCESS);
    EXPECT_EQ(worker.Start(), ErrorCode::SUCCESS);
    EXPECT_EQ(worker.Stop(), ErrorCode::SUCCESS);
}

TEST(WorkerStartTest, RestartRunsTheLoopAgain) {
    auto module = std::make_shared<CountingStartStopModule>("restart");
    Config config;
    ViewPtr<Config> configView{&config};
    core::Worker worker(module, configView);

    MessageQueue queue(16);
    worker.AddQueue("in", ViewPtr<MessageQueue>{&queue});

    ASSERT_EQ(worker.Start(), ErrorCode::SUCCESS);
    std::thread first([&worker]() { worker.WorkLoop(); });
    queue.push(MakeMessage(1));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    worker.Stop();
    first.join();
    ASSERT_GE(module->calls.load(), 1);

    // Worker::Start() must clear the stop flag, otherwise the second thread
    // would exit immediately and process nothing.
    ASSERT_EQ(worker.Start(), ErrorCode::SUCCESS);
    std::thread second([&worker]() { worker.WorkLoop(); });
    queue.push(MakeMessage(2));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    worker.Stop();
    second.join();

    EXPECT_GE(module->calls.load(), 2);
}
