#include <nexusflow/Config.hpp>
#include <nexusflow/Message.hpp>
#include <nexusflow/common/ViewPtr.hpp>

#include "../../src/base/Define.hpp"
#include "../../src/dispatcher/Dispatcher.hpp"

#include <gtest/gtest.h>

namespace {

using namespace nexusflow;

} // namespace

TEST(DispatcherTest, SendToReportsUnknownOutput) {
    Config config;
    ViewPtr<Config> configView{&config};
    dispatcher::Dispatcher dispatcher(configView);

    EXPECT_FALSE(dispatcher.SendTo("missing", MakeMessage(1)));
}

TEST(DispatcherTest, SendToReportsDeliveredMessage) {
    Config config;
    ViewPtr<Config> configView{&config};
    dispatcher::Dispatcher dispatcher(configView);

    MessageQueue queue;
    dispatcher.AddSubscriber("out", ViewPtr<MessageQueue>{&queue});

    EXPECT_TRUE(dispatcher.SendTo("out", MakeMessage(7)));
    EXPECT_FALSE(queue.isEmpty());
    EXPECT_EQ(queue.getSize(), static_cast<std::size_t>(1));
}

TEST(DispatcherTest, SendToReportsShutDownQueue) {
    Config config;
    ViewPtr<Config> configView{&config};
    dispatcher::Dispatcher dispatcher(configView);

    MessageQueue queue;
    dispatcher.AddSubscriber("out", ViewPtr<MessageQueue>{&queue});
    queue.shutdown();

    EXPECT_FALSE(dispatcher.SendTo("out", MakeMessage(7)));
}

TEST(DispatcherTest, BroadcastCopiesToEverySubscriber) {
    Config config;
    ViewPtr<Config> configView{&config};
    dispatcher::Dispatcher dispatcher(configView);

    MessageQueue first;
    MessageQueue second;
    dispatcher.AddSubscriber("first", ViewPtr<MessageQueue>{&first});
    dispatcher.AddSubscriber("second", ViewPtr<MessageQueue>{&second});

    dispatcher.Broadcast(MakeMessage(3));

    EXPECT_EQ(first.getSize(), static_cast<std::size_t>(1));
    EXPECT_EQ(second.getSize(), static_cast<std::size_t>(1));
}
