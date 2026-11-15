#ifndef NEXUSFLOW_PIPELINE_HPP
#define NEXUSFLOW_PIPELINE_HPP

#include <nexusflow/ErrorCode.hpp>
#include <nexusflow/Module.hpp>
#include <nexusflow/Stats.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

// TODO 能否做不依赖Graph
// Forward declaration
class Graph;
namespace nexusflow {
class PipelineBuilder;
class PipelineContext;
}

namespace nexusflow {

/**
 * @brief Default capacity of the message queue created for every graph edge.
 *
 * It bounds the number of in-flight messages between two stages, i.e. the
 * backpressure depth of the pipeline. Override it per pipeline through
 * PipelineBuilder::SetMessageQueueCapacity() or the optional
 * `graph.messageQueueCapacity` key in a YAML config.
 */
constexpr int kDefaultMessageQueueCapacity = 5;

/**
 * @brief Runtime counters for one edge queue.
 * @deprecated Use EdgeSnapshot. This alias exists so code that names the type
 *             keeps compiling while it migrates; it will go away.
 */
using QueueMetrics = EdgeSnapshot;

/**
 * @brief Runtime counters for one module.
 * @deprecated Use ModuleSnapshot.
 */
using ModuleMetrics = ModuleSnapshot;

/**
 * @brief Snapshot of the pipeline runtime state, taken without stopping it.
 * @deprecated Use PipelineSnapshot.
 */
using PipelineMetrics = PipelineSnapshot;

/**
 * @brief Pipeline
 *
 */
class Pipeline {
public:
    static std::unique_ptr<Pipeline> CreateFromYaml(const std::string& configPath);

    ErrorCode Init();

    ErrorCode Start();

    ErrorCode Stop();

    ErrorCode DeInit();

    /**
     * @brief Returns edge and module counters. Safe to call while the pipeline runs.
     * @details The modules are read before the edges, so a reader taking a module
     * and then the edge it writes into never sees a message that left the module
     * but has not reached the edge yet.
     */
    PipelineSnapshot GetSnapshot() const;

    /**
     * @brief Same as GetSnapshot().
     * @deprecated Use GetSnapshot(); the snapshot is not a set of metrics, it is
     *             one moment of them.
     */
    PipelineSnapshot GetMetrics() const { return GetSnapshot(); }

    /**
     * @brief Clears the per-edge water marks (EdgeSnapshot::maxSize).
     * @details The cumulative counters keep growing: they are what two snapshots
     * are subtracted for, so resetting them would break that. Only the water
     * marks restart. Call this while the pipeline is stopped, or accept that an
     * edge being filled right now may record a depth from the previous window.
     */
    void ResetMetrics();

    /**
     * @brief Returns this pipeline's context: per-pipeline log sink, profiler,
     * stop token and metrics reported by modules. Null before Init().
     */
    std::shared_ptr<PipelineContext> GetContext() const;

    ~Pipeline();

private:
    friend PipelineBuilder;

    Pipeline();

    ErrorCode InitWithGraph(std::unique_ptr<Graph> graph);

    // Fills `order` with one valid "sinks first, sources last" stage order.
    void BuildStopOrder(std::vector<std::string>& order) const;

    class Impl;
    std::unique_ptr<Impl> m_pImpl;
};

} // namespace nexusflow

#endif