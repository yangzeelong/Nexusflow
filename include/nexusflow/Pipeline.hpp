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

    /**
     * @brief Stops the pipeline and delivers whatever is still queued.
     *
     * @details The stages are torn down producers-first: a stage is joined once
     * every stage that feeds it is joined and its output edges are closed, so it
     * gets to process the whole backlog it was handed before its own stop
     * request arrives. Every edge is empty when this returns, and
     * EdgeSnapshot::discardedTotal does not move.
     *
     * Blocking, with no budget: it returns when every worker thread has exited.
     * A module whose Process() does not return therefore hangs it -- a module
     * owns its own call, and a framework that guessed a deadline for it would be
     * reporting a timeout it cannot act on.
     *
     * The pipeline stays usable: Start() runs it again. Where the time went is in
     * the next snapshot: PipelineSnapshot::stopPipelineMs is the whole call and each
     * ModuleSnapshot carries the drainInputsMs (waiting for its inputs to empty) and
     * exitLoopMs (waiting for it to leave its loop) that stage owned.
     */
    ErrorCode Stop();

    /**
     * @brief Stops the pipeline now, discarding whatever is still queued.
     *
     * @details The counterpart of Stop(). Every stage is told to stop at once and
     * every edge is closed, so a worker parked in a blocking push or pull returns
     * immediately instead of waiting for the backlog to drain. What was still
     * buffered is destroyed and counted in EdgeSnapshot::discardedTotal; no
     * module counter is charged for it.
     *
     * Blocking on the same terms as Stop(): it returns when every worker thread
     * has exited. The pipeline stays usable: Start() runs it again. The snapshot
     * reports PipelineSnapshot::stopPipelineMs and each ModuleSnapshot::exitLoopMs, but every
     * drainInputsMs stays zero: a cancel never drains.
     */
    ErrorCode Cancel();

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