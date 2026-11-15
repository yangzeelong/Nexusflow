#ifndef NEXUSFLOW_WORKER_HPP
#define NEXUSFLOW_WORKER_HPP

#include "base/Define.hpp"
#include <nexusflow/common/ViewPtr.hpp>
#include "nexusflow/ErrorCode.hpp"
#include "nexusflow/Logging.hpp"
#include "nexusflow/Module.hpp"
#include "nexusflow/TimerRegistry.hpp"

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
namespace profiling { class TimerRegistry; }
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
     * @brief Number of messages this worker has processed so far.
     * A source invocation that produced output counts one; batches count their size.
     */
    std::uint64_t ProcessedCount() const { return m_processedCount.load(std::memory_order_relaxed); }

    /**
     * @brief Messages this worker dropped: outputs a module could not hand to a
     *        downstream queue, plus partial fused messages that hit the fusion
     *        timeout. processed + dropped accounts for everything the module saw.
     */
    std::uint64_t DroppedCount() const { return m_droppedCount.load(std::memory_order_relaxed); }

    /// Bumps the drop counter. Public so ModuleActor can report drops that the
    /// module made on the consumer side of the pipeline (the dispatcher counter
    /// covers the producer side).
    void RecordDropped(std::uint64_t count = 1) { m_droppedCount.fetch_add(count, std::memory_order_relaxed); }

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
    profiling::TimerRegistry& Profiler();

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
    std::atomic<std::uint64_t> m_processedCount{0};
    std::atomic<std::uint64_t> m_droppedCount{0};

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
    bool m_syncInputs = false;
    // Profiling costs a name copy and a registry lock per measurement; the
    // work loop only pays it for modules that opt in.
    bool m_profileTimers = false;
    // Timer labels are stable per module; building them once keeps the work loop free
    // of a string concatenation on every iteration. The handles are interned from
    // those labels once too, so a profiled iteration measures without name work.
    std::string m_processTimerName;
    std::string m_waitTimerName;
    profiling::TimerRegistry::TimerHandle m_processTimer = 0;
    profiling::TimerRegistry::TimerHandle m_waitTimer = 0;
};

}} // namespace nexusflow::core

#endif // NEXUSFLOW_WORKER_HPP