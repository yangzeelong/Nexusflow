#include <nexusflow/Config.hpp>
#include <nexusflow/ErrorCode.hpp>
#include <nexusflow/Message.hpp>
#include <nexusflow/Module.hpp>
#include <nexusflow/ModuleFactory.hpp>
#include <nexusflow/Pipeline.hpp>
#include <nexusflow/PipelineBuilder.hpp>

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

namespace {

using namespace nexusflow;

// Records the order in which the framework initializes/de-initializes modules.
class OrderRecordingModule : public Module {
public:
    OrderRecordingModule(const std::string& name, std::vector<std::string>* order) : Module(name), m_order(order) {}

protected:
    void Process(Message& inputMessage) override { (void)inputMessage; }

    ErrorCode Init() override {
        m_order->push_back(GetModuleName());
        return ErrorCode::SUCCESS;
    }

    ErrorCode DeInit() override {
        m_order->push_back(GetModuleName());
        return ErrorCode::SUCCESS;
    }

private:
    std::vector<std::string>* m_order;
};

TEST(PipelineOrderTest, InitFollowsGraphEdgesAndDeInitReversesThem) {
    std::vector<std::string> order;
    auto first = std::make_shared<OrderRecordingModule>("first", &order);
    auto second = std::make_shared<OrderRecordingModule>("second", &order);
    auto third = std::make_shared<OrderRecordingModule>("third", &order);

    auto pipeline = PipelineBuilder()
                        .AddModule(first)
                        .AddModule(second)
                        .AddModule(third)
                        .Connect("first", "second")
                        .Connect("second", "third")
                        .Build();
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);

    const std::vector<std::string> expectedAfterInit{"first", "second", "third"};
    EXPECT_EQ(order, expectedAfterInit);

    ASSERT_EQ(pipeline->DeInit(), ErrorCode::SUCCESS);
    const std::vector<std::string> expectedAfterDeInit{"first", "second", "third", "third", "second", "first"};
    EXPECT_EQ(order, expectedAfterDeInit);
}

// A module whose Configure() rejects its configuration.
class ConfigFailingModule : public Module {
public:
    explicit ConfigFailingModule(const std::string& name) : Module(name) {}

    ErrorCode Configure(const Config& config) override {
        (void)config;
        return ErrorCode::FAILURE;
    }

    void Process(Message& inputMessage) override { (void)inputMessage; }
};

TEST(PipelineOrderTest, ModuleFactoryRejectsModulesWhoseConfigureFails) {
    ModuleFactory::GetInstance().Register<ConfigFailingModule>("ConfigFailingModule");
    const std::shared_ptr<Module> module =
        ModuleFactory::GetInstance().CreateModule("ConfigFailingModule", "config_failing", Config());
    EXPECT_EQ(module, nullptr);
}

} // namespace
