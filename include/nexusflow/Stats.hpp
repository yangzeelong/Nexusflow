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
    /// Messages destroyed before a consumer took them: the backlog a
    /// Pipeline::Cancel() discards, plus everything evicted by the drop-oldest
    /// queue policy (QueueFullPolicy::kDropOldest).
    ///
    /// Charged to the edge and never to a module's droppedTotal: the producer
    /// counted the message as processed and pushed, and no consumer ever popped
    /// it, so either module would be counting somebody else's loss.
    ///
    /// pushedTotal == poppedTotal + size + discardedTotal always holds; a Stop()
    /// drains everything and leaves discardedTotal where it was.
    std::uint64_t discardedTotal = 0;

    /// "from -> to", the name a profile and a log line use.
    std::string Name() const { return from + " -> " + to; }
};

/**
 * @brief Counters of one module.
 *
 * processedTotal + droppedTotal + failedTotal accounts for everything the module
 * was handed: each message ends up in exactly one of the three. droppedTotal
 * covers messages the module could not hand downstream (a full or closed queue)
 * plus joined groups that expired; joinExpiredTotal is the part of that which
 * expired.
 *
 * The two `*Ms` fields answer "where did the stop go": they are the timing of the
 * most recent Stop()/Cancel(), which is the part of a pipeline's lifetime that a
 * run-time profile cannot show. They are cleared when the pipeline is started
 * again, so a snapshot taken mid-run never reports how the previous run ended.
 */
struct ModuleSnapshot {
    std::string name;
    /// Messages the module finished processing.
    std::uint64_t processedTotal = 0;
    /// Messages lost downstream: push refused, or a joined group expired.
    std::uint64_t droppedTotal = 0;
    /// Subset of droppedTotal: joined groups dropped because an input arrived
    /// after the join timeout.
    std::uint64_t joinExpiredTotal = 0;
    /// Invocations that threw. Counted per invocation, not per message.
    std::uint64_t failedTotal = 0;
    /// How long the most recent Stop() waited for this module's inputs to empty.
    ///
    /// Zero for a source (it has no inputs) and zero after Cancel(), which never
    /// drains. A stage that shows a large drainInputsMs held up the rest of the stop:
    /// nothing downstream of it is joined until it has taken everything.
    std::uint64_t drainInputsMs = 0;
    /// How long the most recent stop waited for this module to leave its loop
    /// once it had been asked to. Large for a stage stuck inside Process().
    std::uint64_t exitLoopMs = 0;
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
 * so a second Start() continues the same totals. What a restart does clear is the
 * stop timing (`stopPipelineMs`, and each module's `drainInputsMs`/`exitLoopMs`).
 */
/**
 * @brief Counters of the opt-in thread pool; all zero when no pool is used.
 *
 * One round is one pass of a pool thread over its whole worker list. A turn is
 * a Worker::StepOnce() call that did work. A blocked round ended with an output
 * edge full (the slot is waiting on back-pressure, not idle); an idle round
 * found nothing to do at all. The split is what separates "too many threads"
 * from "producer/consumer out of phase".
 */
struct ThreadPoolSnapshot {
    std::uint64_t rounds = 0;
    std::uint64_t turns = 0;
    std::uint64_t blockedRounds = 0;
    std::uint64_t idleRounds = 0;
};

struct PipelineSnapshot {
    /// Graph name of the pipeline.
    std::string name;
    /// One entry per edge, in the pipeline's edge order.
    std::vector<EdgeSnapshot> edges;
    /// One entry per module, in the pipeline's module order.
    std::vector<ModuleSnapshot> modules;
    /// Wall time of the most recent Stop()/Cancel(), in milliseconds.
    ///
    /// Zero before the first stop and after Start(): a snapshot taken while the
    /// pipeline runs describes this run, not the one that ended before it. This is
    /// the number to look at first when a stop felt slow, then the modules' drainInputsMs
    /// (who held the drain) and exitLoopMs (who was still inside Process()).
    std::uint64_t stopPipelineMs = 0;

    /// One-line human readable form, for a log record or a CLI dump.
    std::string ToString() const;
};

} // namespace nexusflow

#endif // NEXUSFLOW_STATS_HPP
