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

// There is deliberately no framework-wide default edge capacity. Queue depth is
// a property of the pipeline being built -- a 4K vision stage and a 640-wide
// detector want very different backlogs -- so every pipeline states it:
// PipelineBuilder::Build(int) for code, graph.messageQueueCapacity for yaml.
// A pipeline that arrives without one is refused, not given a guess.

/**
 * @brief Capacity of the shared queue inside a peer-to-peer (join) group.
 *
 * The join wait is meant to hold one sample from each member at most, so this
 * is one member fewer than the join consumes. Must not be < 2.
 */
constexpr int kPeerGroupHeadroom = 1;

/**
 * @brief Pipeline
 *
 */
class Pipeline {
public:
    static std::unique_ptr<Pipeline> CreateFromYaml(const std::string& configPath);

    ErrorCode Init();

    /**
     * @brief Starts every module's worker.
     * @details Idempotent: calling it on a pipeline that is already running does
     * nothing and returns SUCCESS. Fails only when the pipeline is not initialized
     * or a worker cannot start.
     */
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
     * @brief Clears the per-edge water marks (EdgeSnapshot::maxSize).
     * @details The cumulative counters keep growing: they are what two snapshots
     * are subtracted for, so resetting them would break that. Only the water
     * marks restart. Call this while the pipeline is stopped, or accept that an
     * edge being filled right now may record a depth from the previous window.
     */
    void ResetMetrics();

    /**
     * @brief Capacity the built pipeline used for every edge queue.
     * @details PipelineBuilder::Build(capacity) or a yaml config decides it; a
     * queue depth is a property of the pipeline, not of the framework, so it is
     * always stated by whoever builds it. 0 before Init().
     */
    int GetEdgeCapacity() const;

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