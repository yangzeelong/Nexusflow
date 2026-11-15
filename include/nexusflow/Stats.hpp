#ifndef NEXUSFLOW_STATS_HPP
#define NEXUSFLOW_STATS_HPP

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace nexusflow {

/**
 * @brief Counters and water marks of one edge (the queue between two modules).
 *
 * Field naming follows one rule: a cumulative count carries a `Total` suffix
 * (it only ever grows, so two snapshots can be subtracted to get a rate), and
 * everything else is a plain name describing the value right now. `size` is the
 * current backlog, `maxSize` the deepest backlog since the last
 * Pipeline::ResetMetrics().
 *
 * A reader may see a trough that never happened (pushedTotal from after a push
 * next to poppedTotal from before the matching pop). ModuleSnapshot lists the
 * modules before their edges for the same reason: read one module and then the
 * edge it writes into, never the other way round.
 */
struct EdgeSnapshot {
    /// Upstream module name.
    std::string from;
    /// Downstream module name.
    std::string to;
    /// Configured queue capacity, 0 when the queue is unbounded.
    std::size_t capacity = 0;
    /// Messages queued right now.
    std::size_t size = 0;
    /// Deepest backlog seen since the last reset.
    std::size_t maxSize = 0;
    /// Messages pushed into this edge.
    std::uint64_t pushedTotal = 0;
    /// Messages popped out of this edge.
    std::uint64_t poppedTotal = 0;

    /// "from -> to", the name a profile and a log line use.
    std::string Name() const { return from + " -> " + to; }
};

/**
 * @brief Counters of one module.
 *
 * processedTotal + droppedTotal + failedTotal accounts for everything the module
 * was handed: each message ends up in exactly one of the three. droppedTotal
 * covers messages the module could not hand downstream (a full or closed queue)
 * plus fused groups that expired; fusionExpiredTotal is the part of that which
 * expired.
 */
struct ModuleSnapshot {
    std::string name;
    /// Messages the module finished processing.
    std::uint64_t processedTotal = 0;
    /// Messages lost downstream: push refused, or a fused group expired.
    std::uint64_t droppedTotal = 0;
    /// Subset of droppedTotal: fused groups dropped because an input arrived
    /// after the fusion timeout.
    std::uint64_t fusionExpiredTotal = 0;
    /// Invocations that threw. Counted per invocation, not per message.
    std::uint64_t failedTotal = 0;
};

/**
 * @brief One pipeline at one instant.
 *
 * The snapshot is a plain value: taking it reads a few counters per edge and
 * module and copies strings, so it is cheap enough to take once a second while
 * the pipeline runs. It deliberately holds no percentiles and no derived value
 * (throughput, fill ratio): those are computed from two snapshots.
 *
 * ResetMetrics() clears only the water marks (`maxSize`). The `*Total` fields
 * are cumulative for the life of the pipeline and never go back; that is what
 * makes differencing two snapshots a rate. A restart does not reset them either,
 * so a second Start() continues the same totals.
 */
struct PipelineSnapshot {
    /// Graph name of the pipeline.
    std::string name;
    /// One entry per edge, in the pipeline's edge order.
    std::vector<EdgeSnapshot> edges;
    /// One entry per module, in the pipeline's module order.
    std::vector<ModuleSnapshot> modules;

    /// One-line human readable form, for a log record or a CLI dump.
    std::string ToString() const;
};

} // namespace nexusflow

#endif // NEXUSFLOW_STATS_HPP
