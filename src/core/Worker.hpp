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
     * @brief Waits until every input queue of this worker has been closed by its
     * producer or runs empty.
     * @details Pipeline::Stop() stops stages sinks-first, so an upstream stage has
     * already been joined (and has shut down the edges it feeds) before this is
     * asked. A closed queue will receive nothing more, so there is nothing left
     * to wait for on it, while an open queue still has to empty out. A source
     * worker has no inputs and returns true immediately.
     * @param timeout Upper bound shared by all input queues.
     * @return false when some queue was still open and non-empty at the deadline.
     */
    bool WaitForInputsDrained(std::chrono::milliseconds timeout) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        for (auto& item : m_inputQueueMap) {
            const auto now = std::chrono::steady_clock::now();
            std::chrono::milliseconds remaining{0};
            if (now < deadline) {
                remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
                if (remaining.count() < 1) {
                    remaining = std::chrono::milliseconds(1);
                }
            }
            if (!item.second->waitUntilEmpty(remaining)) {
                return false;
            }
        }
        return true;
    }

    void WorkLoop();

    /// Whether this worker is a source (has no input queue to pull from).
    bool IsSource() const { return m_inputQueueMap.empty(); }
    /// Whether this worker drives RunFusion() instead of the batch loop.
    bool HasSyncInputs() const { return m_syncInputs; }

    /**
     * @brief Tells a fusion worker that the pipeline is closing.
     * @details RunFusion() waits for one message id to show up on every input.
     * When the upstream stages are already gone, that wait can only time out, so
     * the drain asks the worker to flush what it has instead of waiting.
     */
    void RequestDrain() { m_drainRequested.store(true); }

    /// Re-arms normal fusion behaviour when the pipeline is started again.
    void ClearDrain() { m_drainRequested.store(false); }

    /**
     * @brief Waits until the worker loop has observed a stop request.
     * @details Pipeline::Stop() uses this instead of joining a thread, so a module
     * that cannot leave its call in time is detected rather than waited on
     * forever. The wait re-checks the flag, so a RequestStop() that lands while
     * the caller is already waiting is picked up on the next poll.
     * @return true when the loop flagged itself as finished.
     */
    bool WaitForStopRequest(std::chrono::milliseconds timeout) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        std::unique_lock<std::mutex> lock(m_loopMutex);
        while (!m_loopExited) {
            if (m_loopCond.wait_until(lock, deadline) == std::cv_status::timeout) {
                return m_loopExited;
            }
        }
        return true;
    }

    /// Upper bound a single batch pull can block on its input queues.
    std::chrono::milliseconds BatchTimeout() const { return m_batchTimeout; }
    /// Whether the worker loop of the current run has not returned yet.
    bool IsRunning() const;
    /// Upper bound RunFusion() waits for its inputs before timing out.
    std::chrono::milliseconds FusionTimeout() const { return m_fusionTimeout; }
    /// How long a source yields when an invocation produced nothing.
    std::chrono::milliseconds SourceIdleTime() const { return std::chrono::milliseconds(m_sourceIdleMs); }

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
        return m_processedCount;
    }

    /**
     * @brief Messages this worker dropped: outputs a module could not hand to a
     *        downstream queue, plus partial fused messages that hit the fusion
     *        timeout. processed + dropped accounts for everything the module saw.
     */
    std::uint64_t DroppedCount() const {
        ObserveCounters();
        return m_droppedCount;
    }

    /// Returns false once a second thread recorded a drop or a failure, i.e.
    /// once this worker's counters no longer have a single writer.
    bool CountersAreSingleWriter() const {
        return m_counterWriters.load(std::memory_order_relaxed) == 1U && m_extraWriters.load(std::memory_order_relaxed) == 0U;
    }

    /// Bumps the drop counter.
    /// @details Normally called from this worker's own loop, which is the only
    /// writer, so the counter is a plain increment. RecordDropped() is public so
    /// a second thread can report a drop too; when that happens this switches to
    /// a mutex-protected atomic add and PublishCounters() takes the same lock.
    /// See the comment on the counter members for why the plain increment is
    /// worth the extra branch.
    void RecordDropped(std::uint64_t count = 1) {
        if (CountersAreSingleWriter()) {
            m_droppedCount += count;
            return;
        }
        // A second writer exists: fall back to a locked read-modify-write and let
        // the loop thread take the same lock from now on.
        m_extraWriters.fetch_add(1, std::memory_order_relaxed);
        m_counterWriters.store(0, std::memory_order_relaxed);
        std::lock_guard<std::mutex> lock(m_writerMutex);
        m_droppedCount += count;
    }

    /**
     * @brief Partial fused messages this worker dropped after the fusion timeout.
     * @details A subset of DroppedCount(), broken out because it is a different
     * failure: the inputs arrived, one of them just arrived too late.
     */
    std::uint64_t FusionExpiredCount() const {
        ObserveCounters();
        return m_fusionExpiredCount;
    }

    /**
     * @brief Invocations that threw. processed + dropped + failed accounts for
     *        everything the module saw: a throwing batch is neither processed
     *        nor handed downstream, so without this the two counters would add
     *        up to less than the module's input.
     */
    std::uint64_t FailedCount() const {
        ObserveCounters();
        return m_failedCount;
    }

    void RecordFailed() {
        if (CountersAreSingleWriter()) {
            ++m_failedCount;
            return;
        }
        m_extraWriters.fetch_add(1, std::memory_order_relaxed);
        m_counterWriters.store(0, std::memory_order_relaxed);
        std::lock_guard<std::mutex> lock(m_writerMutex);
        ++m_failedCount;
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
     * RecordDropped()/RecordFailed() are public, so a caller could bump them
     * from a second thread. The moment that happens m_counterWriters drops to 0
     * and every writer, the loop included, takes m_writerMutex and uses an
     * atomic add: correctness first, and the fallback costs nothing while there
     * is only one writer.
     */
    std::uint64_t m_processedCount = 0;
    std::uint64_t m_droppedCount = 0;
    /// Subset of m_droppedCount: fusion groups dropped after the timeout.
    std::uint64_t m_fusionExpiredCount = 0;
    std::uint64_t m_failedCount = 0;
    /// 1 while the loop thread is the only writer, 0 once anybody else has
    /// recorded a drop or a failure.
    std::atomic<unsigned> m_counterWriters{1};
    std::atomic<unsigned> m_extraWriters{0};
    mutable std::mutex m_writerMutex;

    /// Acquire-side bookend: everyone who reads a counter loads this first.
    /// Cheap enough for a snapshot, and it keeps the plain counter's reads from
    /// being reordered before the writes the loop published.
    void ObserveCounters() const {
        if (m_counterWriters.load(std::memory_order_relaxed) == 1U && m_extraWriters.load(std::memory_order_relaxed) == 0U) {
            std::atomic_thread_fence(std::memory_order_acquire);
            return;
        }
        m_writerMutex.lock();
        m_writerMutex.unlock();
        std::atomic_thread_fence(std::memory_order_acquire);
    }

    /// Publishers everything this worker recorded. Called by the loop thread
    /// once per iteration; also the release-side bookend for readers.
    void PublishCounters() const {
        if (m_counterWriters.load(std::memory_order_relaxed) == 1U && m_extraWriters.load(std::memory_order_relaxed) == 0U) {
            std::atomic_thread_fence(std::memory_order_seq_cst);
            return;
        }
        m_writerMutex.lock();
        m_writerMutex.unlock();
        std::atomic_thread_fence(std::memory_order_seq_cst);
    }

    // Wakes a caller waiting for the loop to leave (see WaitForStopRequest()).
    mutable std::mutex m_loopMutex;
    std::condition_variable m_loopCond;
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