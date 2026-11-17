#ifndef NEXUSFLOW_BENCHMARKS_BENCHMARK_TOPOLOGY_HPP
#define NEXUSFLOW_BENCHMARKS_BENCHMARK_TOPOLOGY_HPP

#include "BenchmarkNodes.hpp"

#include <nexusflow/Config.hpp>
#include <nexusflow/Module.hpp>
#include <nexusflow/Pipeline.hpp>
#include <nexusflow/PipelineBuilder.hpp>

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace nexusflow_bench {

/**
 * @brief A built pipeline plus the modules a caller needs to read afterwards.
 *
 * The modules are kept here on purpose: the sink owns the histogram, and a
 * benchmark run must hold every module alive until the pipeline has stopped.
 */
struct BuiltTopology {
    std::unique_ptr<nexusflow::Pipeline> pipeline;
    std::shared_ptr<SourceModule> source;
    std::shared_ptr<SinkModule> sink;
    std::vector<std::shared_ptr<nexusflow::Module>> modules;
    std::size_t moduleCount = 0;
};

/**
 * @brief Source -> (modules-2) pass-through stages -> sink.
 *
 * `modules` counts the stages including source and sink, so modules=3 is
 * source -> one pass-through -> sink. This is the topology whose per-stage cost
 * a thread model has to beat.
 */
inline BuiltTopology BuildLinear(int modules, std::size_t capacity, PayloadSpec spec, bool freshPayload) {
    BuiltTopology built;
    const int stages = modules < 3 ? 3 : modules;

    built.source = std::make_shared<SourceModule>("source", spec, freshPayload);
    built.modules.push_back(built.source);
    built.sink = std::make_shared<SinkModule>("sink");
    built.modules.push_back(built.sink);

    nexusflow::PipelineBuilder builder;
    builder.AddModule(built.source);
    std::vector<std::string> names;
    names.push_back("source");
    for (int i = 0; i < stages - 2; ++i) {
        const std::string name = "stage_" + std::to_string(i);
        auto stage = std::make_shared<PassThroughModule>(name);
        built.modules.push_back(stage);
        builder.AddModule(stage);
        names.push_back(name);
        builder.Connect(names[names.size() - 2], name);
    }
    builder.AddModule(built.sink);
    builder.Connect(names.back(), "sink");
    names.push_back("sink");

    built.pipeline = builder.Build(static_cast<int>(capacity));
    built.moduleCount = names.size();
    return built;
}

/**
 * @brief Source -> N branches -> sink, the fan-out/fan-in shape.
 *
 * Without `syncInputs` the sink simply reads whatever arrives from any branch,
 * which is the shape the original harness measured. With `syncInputs` the
 * framework runs the sink through the fusion path: it waits for every branch of
 * one message id and hands over a map of Messages, which is the only way the
 * fusion batching cost becomes measurable.
 */
inline BuiltTopology BuildDiamond(int branches, std::size_t capacity, PayloadSpec spec, bool freshPayload,
                                  bool syncInputs) {
    BuiltTopology built;
    const int count = branches < 2 ? 2 : branches;

    built.source = std::make_shared<SourceModule>("source", spec, freshPayload);
    built.sink = std::make_shared<SinkModule>("sink");
    built.modules.push_back(built.source);
    built.modules.push_back(built.sink);

    nexusflow::PipelineBuilder builder;
    builder.AddModule(built.source);
    for (int i = 0; i < count; ++i) {
        const std::string name = "branch_" + std::to_string(i);
        auto branch = std::make_shared<PassThroughModule>(name);
        built.modules.push_back(branch);
        builder.AddModule(branch);
        builder.Connect("source", name);
        builder.Connect(name, "sink");
    }
    builder.AddModule(built.sink);

    if (syncInputs) {
        nexusflow::Config config;
        config.Add<bool>("syncInputs", true);
        builder.SetModuleConfig("sink", config);
    }

    built.pipeline = builder.Build(static_cast<int>(capacity));
    built.moduleCount = built.modules.size();
    return built;
}

} // namespace nexusflow_bench

#endif // NEXUSFLOW_BENCHMARKS_BENCHMARK_TOPOLOGY_HPP
