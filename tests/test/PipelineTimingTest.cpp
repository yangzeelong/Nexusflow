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

class TimingSourceModule : public Module {
public:
    explicit TimingSourceModule(const std::string& name) : Module(name) {}

protected:
    void Process(Message& inputMessage) override {
        (void)inputMessage;
        Broadcast(std::move(MakeMessage(1)));
    }
};

class TimingSinkModule : public Module {
public:
    explicit TimingSinkModule(const std::string& name) : Module(name) {}

    std::atomic<int> messagesWithSourceTiming{0};

protected:
    void Process(Message& inputMessage) override {
        // The dispatcher adds the producing module's elapsed time before the
        // message leaves the source stage, so it must be visible here.
        if (inputMessage.GetMetaData().GetModuleTimings().count("source") > 0) {
            messagesWithSourceTiming.fetch_add(1, std::memory_order_relaxed);
        }
    }
};

} // namespace

TEST(PipelineTimingTest, OutputMessagesCarryUpstreamModuleTimings) {
    auto source = std::make_shared<TimingSourceModule>("source");
    auto sink = std::make_shared<TimingSinkModule>("sink");

    auto pipeline = PipelineBuilder()
                        .AddModule(source)
                        .AddModule(sink)
                        .Connect(source->GetModuleName(), sink->GetModuleName())
                        .Build();
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    std::this_thread::sleep_for(std::chrono::milliseconds(kRunMs));
    ASSERT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);

    EXPECT_GT(sink->messagesWithSourceTiming.load(std::memory_order_relaxed), 0);
}
