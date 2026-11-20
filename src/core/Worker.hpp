#ifndef NEXUSFLOW_WORKER_HPP
#define NEXUSFLOW_WORKER_HPP

#include "base/Define.hpp"
#include <nexusflow/common/ViewPtr.hpp>
#include "nexusflow/ErrorCode.hpp"
#include "nexusflow/Logging.hpp"
#include "nexusflow/Module.hpp"
#include "nexusflow/MetricsCollector.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace nexusflow {
class PipelineContext;
namespace profiling { class MetricsCollector; }
}  // namespace nexusflow

namespace nexusflow { namespace core {

/**
 * @class Worker
 * @brief An internal worker class responsible for driving the execution of a single Module instance.
 *
 * The Worker manages a dedicated thread and implements all "framework logic," such as
 * pulling data from an input queue, invoking the Module's processing methods,
 * and responding to lifecycle controls (start/stop) from the Pipeline.
 */
class Worker {
public:
    Worker(const std::shared_ptr<Module>& modulePtr, const ViewPtr<Config>& configPtr,
           const std::shared_ptr<PipelineContext>& context = nullptr);

    /**
     * @brief Destructor. Ensures the worker thread is properly joined.
     */
    ~Worker();

    // Workers manage a thread and are unique; they should not be copied or moved.
    Worker(const Worker&) = delete;
    Worker& operator=(const Worker&) = delete;

    /**
     * @brief Signals the worker thread to stop.
     * @note The thread itself is owned by ModuleActor. This only flips the flag
     *       that WorkLoop() observes; ModuleActor joins the thread in its own
     *       Stop(). Calling it more than once is harmless.
     * @return Always ErrorCode::SUCCESS.
     */
    ErrorCode Stop();

    /**
     * @brief Prepares the loop to run: clears the stop flag.
     * ModuleActor calls this right before spawning the worker thread, keeping
     * Start()/Stop() symmetric with the other lifecycle calls.
     * @return Always ErrorCode::SUCCESS.
     */
    ErrorCode Start();

    // Setter and getter for the input queue map.
    void AddQueue(const std::string& name, ViewPtr<MessageQueue> queue) {
        if (m_inputQueueMap.find(name) != m_inputQueueMap.end()) {
            LOG_ERROR("Output queue with name {} already exists", name);
            throw std::invalid_argument("Output queue with name " + name + " already exists");
        }
        m_inputQueueMap[name] = std::move(queue);
    }
    // ViewPtr<MessageQueue> GetQueue(const std::string& name) { return m_inputQueueMap[name]; }
    // void RemoveQueue(const std::string& name) { m_inputQueueMap.erase(name); }
    // void ClearQueues() { m_inputQueueMap.clear(); }

    /**
     * @brief Waits until every input queue of this worker runs empty.
     * @details Pipeline::Stop() stops stages producers-first, so every stage that
     * feeds this one has already been joined (and has shut down the edges it
     * feeds) before this is asked. A closed queue receives nothing more, so the
     * only thing between this call and an empty queue is this worker's own
     * consumer loop, which Stop() has not stopped yet. A source worker has no
     * inputs and returns immediately.
     *
     * There is no deadline on purpose: a deadline would decide when to give up
     * and leave backlog behind that a later Start() never delivers.
     */
    void WaitForInputsDrained() {
        for (auto& item : m_inputQueueMap) {
            item.second->waitUntilEmpty();
        }
    }

    void WorkLoop();

    /**
     * @brief One non-blocking unit of work, used when a shared pool hosts
     *        this worker (see PipelineContext::WithThreadPoolSize()).
     * @details A pooled thread cannot afford the blocking waits of
     * WorkLoop(): a stage pulls only what is already queued, and a source
     * gets exactly one invocation. Returns false when there was nothing to
     * do, so the caller can move on to the next module it hosts.
     *
     * Fusion workers are not stepped: RunFusion() waits for a whole group
     * and keeps a dedicated thread. StepOnce() reports false for them.
     */
    bool StepOnce();

    /// True when every output edge still has room for one more message.
    /// Pooled workers use it to apply backpressure without blocking.
    bool OutputsHaveRoom() const;

    /// Free slots in the tightest bounded output edge.
    /// SIZE_MAX when every output edge is unbounded (or there is none).
    std::size_t OutputRoom() const;

    /// True when this worker fuses several inputs into one message, i.e. when
    /// it cannot share a thread with other modules.
    bool HasFusionInputs() const { return m_syncInputs; }

    /// Marks the loop of the current run as finished without running it.
    /// Used by ModuleActor when a pool never started (a failed Start()
    /// rollback), where nothing else would publish the exit.
    void MarkExited();

    /// Whether this worker is a source (has no input queue to pull from).
    bool IsSource() const { return m_inputQueueMap.empty(); }

    /// True when every input edge is closed, so nothing can arrive again.
    /// A stage uses this to tell "temporarily empty" from "finished": during a
    /// drain the second one means it has consumed everything it was given.
    bool AllInputsShutdown() const {
        if (m_inputQueueMap.empty()) {
            return false;
        }
        for (const auto& item : m_inputQueueMap) {
            if (item.second && !item.second->isShutdown()) {
                return false;
            }
        }
        return true;
    }

    /**
     * @brief Tells a fusion worker that the pipeline is closing.
     * @details RunFusion() waits for one message id to show up on every input.
     * When the upstream stages are already gone, that wait can only time out, so
     * the drain asks the worker to flush what it has instead of waiting.
     */
    void RequestDrain() { m_drainRequested.store(true); }

    /// Re-arms normal fusion behaviour when the pipeline is started again.
    void ClearDrain() { m_drainRequested.store(false); }

    /// Whether the worker loop of the current run has not returned yet.
    bool IsRunning() const;

    /**
     * @brief The queues a source writes to, resolved once at start.
     * @details A source has no input to block on: nothing upstream can slow it
     * down. Its backpressure has to come from the edges it fills, so the source
     * parks on them instead of sleeping for a fixed time. See
     * WaitForOutputRoom() for why that is the better mechanism.
     */
    void SetOutputQueues(const std::vector<ViewPtr<MessageQueue>>& queues) { m_outputQueues = queues; }

    /// Edges this worker pushes into, as seen by the loop.
    const std::vector<ViewPtr<MessageQueue>>& OutputQueues() const { return m_outputQueues; }

    /**
     * @brief Handles a source invocation that produced nothing.
     * @details The old mechanism slept a fixed `sourceIdleMs` after every empty
     * invocation, which throttles the producer by wall-clock time: a module whose
     * output depends on arrivals paces itself correctly, but a module that never
     * produces simply burns the configured wait, and a module that produces in
     * bursts is held back even when its edges are wide open.
     *
     * The real signal a source is ahead of the pipeline is that one of its output
     * edges is full. So:
     *  - every output has room: return at once, this invocation was just empty
     *    (a camera that has not decoded the next frame yet should be asked again
     *    immediately, not a millisecond later);
     *  - a full output whose queue is event-driven: wait on that queue for
     *    `sourceIdleMs` (its own condvar, woken by the consumer's pop or by
     *    shutdown), falling back to the same deadline, so the wake-up is immediate
     *    rather than up to a millisecond late;
     *  - a full output that is not event-driven (tryPush only ever sees an empty
     *    queue): yield with a bounded wait, because nothing will signal us.
     */
    void IdleAfterEmptyOutput();

    /**
     * @brief Records that a module's blocking call came back without work.
     * @details Only a source that owns a wait can say this: a decoder that
     * blocked on a device until the run stopped produced nothing, but there is
     * no reason to back it off — it just spent its whole turn waiting. The flag
     * is cleared by the loop before the next invocation, so it only describes
     * the call that has just returned.
     */
    void NoteBlockedWait() {
        std::lock_guard<std::mutex> lock(m_blockedMutex);
        m_sourceBlocked = true;
    }

    /**
     * @brief Number of messages this worker has processed so far.
     * A source invocation that produced output counts one; batches count their size.
     */
    std::uint64_t ProcessedCount() const {
        ObserveCounters();
        return m_processedCount.load(std::memory_order_relaxed);
    }

    /**
     * @brief Messages this worker dropped: outputs a module could not hand to a
     *        downstream queue, plus partial fused messages that hit the fusion
     *        timeout. processed + dropped accounts for everything the module saw.
     */
    std::uint64_t DroppedCount() const {
        ObserveCounters();
        return m_droppedCount.load(std::memory_order_relaxed);
    }

    /// Returns false once a second thread recorded a drop or a failure, i.e.
    /// once this worker's counters are no longer driven by the loop thread alone.
    bool CountersAreSingleWriter() const { return m_counterWriters.load(std::memory_order_acquire) == 1U; }

    /// Bumps the drop counter.
    /// @details The loop thread increments its counters directly (see the Add*
    /// helpers), which is the common path. This entry is public so another thread
    /// can report a drop as well; the statistics it affects are atomics, so the
    /// only thing this has to do beyond the add is claim the multi-writer state.
    void RecordDropped(std::uint64_t count = 1) {
        EnterMultiWriter();
        AddDropped(count);
    }

    /**
     * @brief Partial fused messages this worker dropped after the fusion timeout.
     * @details A subset of DroppedCount(), broken out because it is a different
     * failure: the inputs arrived, one of them just arrived too late.
     */
    std::uint64_t FusionExpiredCount() const {
        ObserveCounters();
        return m_fusionExpiredCount.load(std::memory_order_relaxed);
    }

    /**
     * @brief Invocations that threw. processed + dropped + failed accounts for
     *        everything the module saw: a throwing batch is neither processed
     *        nor handed downstream, so without this the two counters would add
     *        up to less than the module's input.
     */
    std::uint64_t FailedCount() const {
        ObserveCounters();
        return m_failedCount.load(std::memory_order_relaxed);
    }

    void RecordFailed() {
        EnterMultiWriter();
        AddFailed();
    }

    /** @brief Signals the loop to return without joining (used by the drain). */
    /**
     * @brief Tells the worker loop to return, and clears a previous request.
     * @details A stop is about one run of the pipeline; the same worker is
     * started again when the pipeline is started again. A run that had to be
     * abandoned did leave its flag set, so resetting it here is what makes a
     * restart possible at all.
     */
    void PrepareForStart() {
        m_stopFlag.store(false);
        {
            std::lock_guard<std::mutex> lock(m_loopMutex);
            m_loopExited = false;
        }
    }

    void RequestStop() { m_stopFlag.store(true); }


    /** @brief Whether a stop has already been requested for this worker. */
    bool IsStopRequested() const { return m_stopFlag.load(); }

private:
    /**
     * @brief Reads the per-module tuning knobs (batch size/timeout, fusion timeout,
     *        source idle time, sync-inputs flag) and pre-builds the timer labels.
     *        Called once from the constructor so the hot loop does no config lookups.
     *        All values fall back to the historical defaults when absent.
     */
    /// Registry the work loop reports to: the pipeline context's own registry
    /// when one is attached, otherwise the process-wide singleton.
    profiling::MetricsCollector& Profiler();

    void LoadConfig();

    void RunFusion();

    /**
     * @brief Efficiently pulls a batch of messages from the input queue.
     * @details This function implements an efficient, two-phase strategy to gather messages
     * without causing busy-waiting on the CPU.
     *
     * 1.  **Greedy Phase:** It first performs a quick, non-blocking poll (`tryPop`) across all
     *     input queues to immediately collect any readily available messages.
     * 2.  **Blocking Poll Phase:** If the batch is not yet full, it enters a loop that
     *     iterates through the input queues. For each queue, it performs a short-duration
     *     blocking wait (`waitAndPopFor`). This allows the thread to sleep efficiently
     *     if no messages are available, yielding the CPU, yet remaining highly responsive.
     *     The loop continues until the batch is full or the timeout is reached.
     *
     * @param maxBatchSize The maximum number of messages to pull.
     * @param batchTimeout The maximum time to wait for messages to become available.
     *
     * Gives up early once every input queue has been closed by its producer: a
     * stage with nothing left to receive returns immediately instead of idling
     * out the whole timeout.
     */
    std::vector<Message> PullBatchMessage(std::size_t maxBatchSize, std::chrono::milliseconds batchTimeout);

    /// Greedy, non-blocking form of PullBatchMessage(): only what is queued
    /// right now, never a wait.
    std::vector<Message> PullBatchNow(std::size_t maxBatchSize);

private:
    std::shared_ptr<Module> m_modulePtr = nullptr;
    ViewPtr<Config> m_configPtr;
    std::shared_ptr<PipelineContext> m_context;
    std::unordered_map<std::string, ViewPtr<MessageQueue>> m_inputQueueMap;

    std::atomic<bool> m_stopFlag{false};
    /// Fusion only: set by the shutdown drain, see RequestDrain().
    std::atomic<bool> m_drainRequested{false};

    /*
     * Counters. Every one of them is written from this worker's own loop, which
     * is the only thread that runs modules of this worker, so a plain increment
     * is enough and costs ~0.3 ns instead of the ~3.4 ns a locked read-modify-
     * write costs (measured on this machine, see docs/telemetry.md). Readers
     * take snapshots from another thread, so the loop publishes the numbers with
     * PublishCounters() and every reader calls ObserveCounters(), which is the
     * matching acquire fence, first.
     *
     * RecordDropped()/RecordFailed() are public, so a caller can bump them from a
     * second thread. Keeping the counters atomic is what makes that legal at
     * all: a fence-to-fence pair with no atomic object in between establishes no
     * happens-before, so plain counters read while a second thread wrote them were
     * a data race by the letter of the standard, not just in theory.
     */
    std::atomic<std::uint64_t> m_processedCount{0};
    std::atomic<std::uint64_t> m_droppedCount{0};
    /// Subset of m_droppedCount: fusion groups dropped after the timeout.
    std::atomic<std::uint64_t> m_fusionExpiredCount{0};
    std::atomic<std::uint64_t> m_failedCount{0};
    /// 1 while only the loop thread writes, 0 once a second thread has reported a
    /// drop or a failure. Flipped by a compare-exchange: the test-then-set this
    /// replaces could let two threads both conclude they were the only writer, and
    /// it could never switch at all -- only the slow path cleared the flag, and the
    /// slow path was only reached once the flag was already clear.
    std::atomic<unsigned> m_counterWriters{1};

    /// Claims the multi-writer state. Only the transition matters; a thread that
    /// arrives after it finds the flag already 0 and changes nothing.
    void EnterMultiWriter() {
        unsigned expected = 1U;
        (void)m_counterWriters.compare_exchange_strong(expected, 0U, std::memory_order_acq_rel,
                                                       std::memory_order_relaxed);
    }

    void AddProcessed(std::uint64_t count) { m_processedCount.fetch_add(count, std::memory_order_relaxed); }
    void AddFailed() { m_failedCount.fetch_add(1, std::memory_order_relaxed); }
    void AddDropped(std::uint64_t count) { m_droppedCount.fetch_add(count, std::memory_order_relaxed); }
    void AddFusionExpired(std::uint64_t count) { m_fusionExpiredCount.fetch_add(count, std::memory_order_relaxed); }

    /// Acquire-side bookend: everyone who reads a counter loads this first.
    /// Cheap enough for a snapshot, and it keeps a snapshot's counter loads from
    /// being reordered before the writes the loop published.
    void ObserveCounters() const { std::atomic_thread_fence(std::memory_order_acquire); }

    /// Publishers everything this worker recorded. Called by the loop thread
    /// once per iteration; also the release-side bookend for readers.
    void PublishCounters() const { std::atomic_thread_fence(std::memory_order_seq_cst); }

    // Guards m_loopExited: IsRunning() reads it from another thread while the
    // loop thread writes it at the end of a run.
    mutable std::mutex m_loopMutex;
    /// False only while a worker loop of the current run is executing: it starts
    /// out false and ModuleActor sets it via PrepareForStart() before it spawns
    /// the loop.
    bool m_loopExited = true;

    // Per-module tuning, resolved once in LoadConfig().
    std::size_t m_maxBatchSize = 4;
    /// Multi-input switch: when inputs are not fused, a batch is collected from
    /// every input queue until it is full or this timeout elapses.
    std::chrono::milliseconds m_batchTimeout{100};
    std::chrono::milliseconds m_fusionTimeout{60000};
    int m_sourceIdleMs = 1;
    /// Set by a source module for the invocation that has just returned, see
    /// NoteBlockedWait().
    bool m_sourceBlocked = false;
    mutable std::mutex m_blockedMutex;
    /// Edges a source fills; used to decide what an empty invocation should wait
    /// on (see IdleAfterEmptyOutput()).
    std::vector<ViewPtr<MessageQueue>> m_outputQueues;
    bool m_syncInputs = false;
    // Profiling costs a name copy and a registry lock per measurement; the
    // work loop only pays it for modules that opt in.
    bool m_profileTimers = false;
    // Timer labels are stable per module; building them once keeps the work loop free
    // of a string concatenation on every iteration. The handles are interned from
    // those labels once too, so a profiled iteration measures without name work.
    std::string m_processTimerName;
    std::string m_waitTimerName;
    profiling::MetricsCollector::TimerHandle m_processTimer = 0;
    profiling::MetricsCollector::TimerHandle m_waitTimer = 0;
};

}} // namespace nexusflow::core

#endif // NEXUSFLOW_WORKER_HPP