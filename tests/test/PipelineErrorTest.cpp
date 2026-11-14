#include <nexusflow/ErrorCode.hpp>
#include <nexusflow/Message.hpp>
#include <nexusflow/Module.hpp>
#include <nexusflow/Pipeline.hpp>
#include <nexusflow/PipelineBuilder.hpp>

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <memory>
#include <string>

namespace {

using namespace nexusflow;

// Writes a temporary YAML config next to gtest's working directory and removes
// it again, so the error-path tests do not need a fixtures directory.
class TempYamlFile {
public:
    explicit TempYamlFile(const std::string& content) {
        static int counter = 0;
        m_path = "nexusflow_error_test_" + std::to_string(counter++) + ".yaml";
        std::ofstream out(m_path);
        out << content;
    }

    ~TempYamlFile() { std::remove(m_path.c_str()); }

    const std::string& Path() const { return m_path; }

private:
    std::string m_path;
};

// Modules used by the builder-based error tests. They are passed to the builder
// as instances, so no factory registration is required.
class NoOpSourceModule : public Module {
public:
    explicit NoOpSourceModule(const std::string& name) : Module(name) {}

protected:
    void Process(Message&) override {}
};

class NoOpSinkModule : public Module {
public:
    explicit NoOpSinkModule(const std::string& name) : Module(name) {}

protected:
    void Process(Message&) override {}
};

TEST(PipelineErrorTest, CreateFromYamlReturnsNullForMissingFile) {
    std::unique_ptr<Pipeline> pipeline =
        Pipeline::CreateFromYaml("this_config_does_not_exist_pipeline_error_test.yaml");
    EXPECT_EQ(pipeline, nullptr);
}

TEST(PipelineErrorTest, CreateFromYamlReturnsNullWithoutGraphSection) {
    TempYamlFile yaml("not_the_graph_root:\n  name: nope\n");
    EXPECT_EQ(Pipeline::CreateFromYaml(yaml.Path()), nullptr);
}

TEST(PipelineErrorTest, CreateFromYamlReturnsNullWhenModuleLacksClass) {
    TempYamlFile yaml(
        "graph:\n"
        "  name: missing_class\n"
        "  modules:\n"
        "    - name: only_a_name\n");
    EXPECT_EQ(Pipeline::CreateFromYaml(yaml.Path()), nullptr);
}

TEST(PipelineErrorTest, CreateFromYamlReturnsNullForUnknownConnection) {
    TempYamlFile yaml(
        "graph:\n"
        "  name: unknown_connection\n"
        "  modules:\n"
        "    - name: a\n"
        "      class: NoOpSourceModule\n"
        "  connections:\n"
        "    - from: a\n"
        "      to: b\n");
    EXPECT_EQ(Pipeline::CreateFromYaml(yaml.Path()), nullptr);
}

TEST(PipelineErrorTest, CreateFromYamlReturnsNullForCycle) {
    TempYamlFile yaml(
        "graph:\n"
        "  name: cyclic\n"
        "  modules:\n"
        "    - name: a\n"
        "      class: NoOpSourceModule\n"
        "    - name: b\n"
        "      class: NoOpSinkModule\n"
        "  connections:\n"
        "    - from: a\n"
        "      to: b\n"
        "    - from: b\n"
        "      to: a\n");
    EXPECT_EQ(Pipeline::CreateFromYaml(yaml.Path()), nullptr);
}

TEST(PipelineErrorTest, CreateFromYamlReturnsNullForUnregisteredModuleClass) {
    // A graph that parses fine but cannot be instantiated: the class names are
    // not registered in the factory for this test binary.
    TempYamlFile yaml(
        "graph:\n"
        "  name: unregistered\n"
        "  modules:\n"
        "    - name: a\n"
        "      class: ThisClassIsNotRegistered\n"
        "    - name: b\n"
        "      class: ThisClassIsAlsoNotRegistered\n"
        "  connections:\n"
        "    - from: a\n"
        "      to: b\n");
    EXPECT_EQ(Pipeline::CreateFromYaml(yaml.Path()), nullptr);
}

TEST(PipelineErrorTest, BuilderRejectsEmptyConfigurations) {
    auto source = std::make_shared<NoOpSourceModule>("source");
    auto sink = std::make_shared<NoOpSinkModule>("sink");

    EXPECT_EQ(PipelineBuilder().Build(), nullptr);
    EXPECT_EQ(PipelineBuilder().AddModule(source).AddModule(sink).Build(), nullptr);
}

TEST(PipelineErrorTest, BuilderRejectsUnknownConnection) {
    auto source = std::make_shared<NoOpSourceModule>("source");
    auto sink = std::make_shared<NoOpSinkModule>("sink");

    std::unique_ptr<Pipeline> pipeline =
        PipelineBuilder().AddModule(source).AddModule(sink).Connect("source", "sink").Connect(sink->GetModuleName(), "ghost").Build();
    EXPECT_EQ(pipeline, nullptr);
}

TEST(PipelineErrorTest, BuilderRejectsCycle) {
    auto source = std::make_shared<NoOpSourceModule>("source");
    auto sink = std::make_shared<NoOpSinkModule>("sink");

    std::unique_ptr<Pipeline> pipeline = PipelineBuilder()
                                             .AddModule(source)
                                             .AddModule(sink)
                                             .Connect(source->GetModuleName(), sink->GetModuleName())
                                             .Connect(sink->GetModuleName(), source->GetModuleName())
                                             .Build();
    EXPECT_EQ(pipeline, nullptr);
}

TEST(PipelineErrorTest, BuilderStillBuildsAValidPipeline) {
    auto source = std::make_shared<NoOpSourceModule>("source");
    auto sink = std::make_shared<NoOpSinkModule>("sink");

    std::unique_ptr<Pipeline> pipeline = PipelineBuilder()
                                             .AddModule(source)
                                             .AddModule(sink)
                                             .Connect(source->GetModuleName(), sink->GetModuleName())
                                             .Build();
    ASSERT_NE(pipeline, nullptr);
    EXPECT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
}

} // namespace
