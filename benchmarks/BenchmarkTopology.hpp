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
 * @brief The pipeline's real shape, as it will be built.
 *
 * `modules` counts every stage including source and sink. The CSV reports this
 * instead of echoing the command line: echoing made every diamond run claim
 * `modules=3` (it has branches+2 stages) and every linear run claim
 * `branches=2` (it has none).
 */
struct TopologyShape {
    int modules = 0;   ///< stages including source and sink
    int branches = 0;  ///< fan-out width; 0 for a straight chain
    int edges = 0;     ///< queues in the graph

    bool IsLinear() const { return branches == 0; }
};

/**
 * @brief Resolves requested numbers into the shape that will actually be built.
 *
 * A chain may be 2 stages (source -> sink, one edge): that is the cheapest legal
 * pipeline and the only point that measures the two endpoints on their own, so
 * it is the bottom of the depth ladder (see HARNESS.md §2). A fan-out needs at
 * least one branch, which is the "edge but no fan-out" control.
 */
inline TopologyShape ShapeOf(bool linear, int modules, int branches) {
    TopologyShape shape;
    if (linear) {
        shape.modules = modules < 2 ? 2 : modules;
        shape.branches = 0;
        shape.edges = shape.modules - 1;
        return shape;
    }
    shape.branches = branches < 1 ? 1 : branches;
    shape.modules = shape.branches + 2;
    shape.edges = shape.branches * 2;
    return shape;
}

/**
 * @brief A built pipeline plus the modules a caller needs to read afterwards.
 *
 * The modules are kept here on purpose: the sink owns the histogram, and a
 * benchmark run must hold every module alive until the pipeline has stopped.
 * `source` is a `SourceModule` for the active topologies and a
 * `PassiveSourceModule` for inject, so it is typed as the base interface.
 */
struct BuiltTopology {
    std::unique_ptr<nexusflow::Pipeline> pipeline;
    std::shared_ptr<nexusflow::Module> source;
    std::shared_ptr<SinkModule> sink;
    std::vector<std::shared_ptr<nexusflow::Module>> modules;
    TopologyShape shape;
};

/// A pass-through stage, unless a workload was requested for the intermediate
/// stages. Keeping the zero-work node for `none` means the framework-only numbers
/// stay comparable with the previous report.
inline std::shared_ptr<nexusflow::Module> MakeStageModule(const std::string& name, const WorkloadSpec& workload) {
    if (workload.empty()) {
        return std::make_shared<PassThroughModule>(name);
    }
    return std::make_shared<WorkloadModule>(name, workload);
}

/**
 * @brief Source -> (shape.modules-2) stages -> sink.
 *
 * `shape.modules` counts the stages including source and sink, so modules=2 is
 * source -> sink and modules=3 is source -> one pass-through -> sink. This is
 * the topology whose per-stage cost a thread model has to beat. Intermediate
 * stages run `workload`, which is a pass-through when `workload.empty()`.
 *
 * `batchSize` > 0 sets the worker's `maxBatchSize` on every module but the source
 * (a source has no input queue, so a batch means nothing there). The raw baseline
 * has no batching, so a comparison against it must use `batchSize == 1`.
 */
inline BuiltTopology BuildLinear(const TopologyShape& shape, std::size_t capacity, PayloadSpec spec,
                                 bool freshPayload, int batchSize = 0, bool measureLatency = true,
                                 const WorkloadSpec& workload = WorkloadSpec{}) {
    BuiltTopology built;
    built.shape = shape;

    built.source = std::make_shared<SourceModule>("source", spec, freshPayload, measureLatency);
    built.sink = std::make_shared<SinkModule>("sink", measureLatency);

    nexusflow::PipelineBuilder builder;
    builder.AddModule(built.source);
    built.modules.push_back(built.source);

    std::string previous = "source";
    for (int i = 0; i < shape.modules - 2; ++i) {
        const std::string name = "stage_" + std::to_string(i);
        auto stage = MakeStageModule(name, workload);
        built.modules.push_back(stage);
        builder.AddModule(stage);
        builder.Connect(previous, name);
        previous = name;
    }

    builder.AddModule(built.sink);
    built.modules.push_back(built.sink);
    builder.Connect(previous, "sink");

    if (batchSize > 0) {
        nexusflow::Config config;
        config.Add<int>("maxBatchSize", batchSize);
        for (int i = 0; i < shape.modules - 2; ++i) {
            builder.SetModuleConfig("stage_" + std::to_string(i), config);
        }
        builder.SetModuleConfig("sink", config);
    }

    built.pipeline = builder.Build(static_cast<int>(capacity));
    return built;
}

/**
 * @brief Source -> shape.branches branches -> sink, the fan-out/fan-in shape.
 *
 * Without `syncInputs` the sink simply reads whatever arrives from any branch,
 * which is the shape the original harness measured. With `syncInputs` the
 * framework runs the sink through the fusion path: it waits for every branch of
 * one message id and hands over a map of Messages, which is the only way the
 * fusion batching cost becomes measurable. Branches run `workload`; a positive
 * `stragglerUs` makes branch_0 sleep that long, which is the only way fused
 * groups stay half-complete long enough for the sink's cache to grow.
 */
inline BuiltTopology BuildDiamond(const TopologyShape& shape, std::size_t capacity, PayloadSpec spec,
                                  bool freshPayload, bool syncInputs, int batchSize = 0, bool measureLatency = true,
                                  const WorkloadSpec& workload = WorkloadSpec{}, int stragglerUs = 0) {
    BuiltTopology built;
    built.shape = shape;

    built.source = std::make_shared<SourceModule>("source", spec, freshPayload, measureLatency);
    built.sink = std::make_shared<SinkModule>("sink", measureLatency);

    nexusflow::PipelineBuilder builder;
    builder.AddModule(built.source);
    built.modules.push_back(built.source);

    for (int i = 0; i < shape.branches; ++i) {
        const std::string name = "branch_" + std::to_string(i);
        WorkloadSpec branchWorkload = workload;
        if (i == 0 && stragglerUs > 0) {
            branchWorkload.kind = WorkloadKind::Delay;
            branchWorkload.delayUs = stragglerUs;
        }
        auto branch = MakeStageModule(name, branchWorkload);
        built.modules.push_back(branch);
        builder.AddModule(branch);
        builder.Connect("source", name);
        builder.Connect(name, "sink");
    }

    builder.AddModule(built.sink);
    built.modules.push_back(built.sink);

    if (batchSize > 0) {
        nexusflow::Config branchConfig;
        branchConfig.Add<int>("maxBatchSize", batchSize);
        for (int i = 0; i < shape.branches; ++i) {
            builder.SetModuleConfig("branch_" + std::to_string(i), branchConfig);
        }
    }
    if (batchSize > 0 || syncInputs) {
        // Only the sink may fuse: a branch has one input, so `syncInputs` there
        // would turn it into a fusion worker waiting for inputs that never come.
        nexusflow::Config sinkConfig;
        if (batchSize > 0) {
            sinkConfig.Add<int>("maxBatchSize", batchSize);
        }
        if (syncInputs) {
            sinkConfig.Add<bool>("syncInputs", true);
        }
        builder.SetModuleConfig("sink", sinkConfig);
    }

    built.pipeline = builder.Build(static_cast<int>(capacity));
    return built;
}

/**
 * @brief Passive source -> (modules-2) stages -> sink, fed by Pipeline::Push().
 *
 * Same stage shape as BuildLinear, but the source never produces on its own: the
 * harness pushes each message into it. This is the topology that answers "what
 * does the injection path cost, and what does back-pressure do to the producer"
 * instead of "how fast can a source loop broadcast".
 */
inline BuiltTopology BuildInjection(int modules, std::size_t capacity, int batchSize, bool measureLatency,
                                    const WorkloadSpec& workload = WorkloadSpec{}) {
    BuiltTopology built;
    const int stages = modules < 2 ? 2 : modules;
    built.shape.modules = stages;
    built.shape.branches = 0;
    built.shape.edges = stages - 1;

    built.source = std::make_shared<PassiveSourceModule>("source");
    built.sink = std::make_shared<SinkModule>("sink", measureLatency);

    nexusflow::PipelineBuilder builder;
    builder.AddModule(built.source);
    built.modules.push_back(built.source);

    std::string previous = "source";
    for (int i = 0; i < stages - 2; ++i) {
        const std::string name = "stage_" + std::to_string(i);
        auto stage = MakeStageModule(name, workload);
        built.modules.push_back(stage);
        builder.AddModule(stage);
        builder.Connect(previous, name);
        previous = name;
    }

    builder.AddModule(built.sink);
    built.modules.push_back(built.sink);
    builder.Connect(previous, "sink");

    if (batchSize > 0) {
        nexusflow::Config config;
        config.Add<int>("maxBatchSize", batchSize);
        for (int i = 0; i < stages - 2; ++i) {
            builder.SetModuleConfig("stage_" + std::to_string(i), config);
        }
        builder.SetModuleConfig("sink", config);
    }

    built.pipeline = builder.Build(static_cast<int>(capacity));
    return built;
}

} // namespace nexusflow_bench

#endif // NEXUSFLOW_BENCHMARKS_BENCHMARK_TOPOLOGY_HPP
