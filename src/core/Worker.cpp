#include "Worker.hpp"
#include "nexusflow/ErrorCode.hpp"
#include "nexusflow/Message.hpp"
#include "nexusflow/Logging.hpp"
#include "nexusflow/PipelineContext.hpp"
#include "nexusflow/TimerRegistry.hpp"
#include <algorithm>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

namespace nexusflow { namespace core {

namespace {

// A profiled loop iteration measures through a timer handle interned once, so
// enabling "profileTimers: true" costs the clock and a registry lock per
// measurement rather than a name copy and a hash. When profiling is off the
// scope holds no timer at all and touches nothing.
class OptionalScopeTimer {
public:
    OptionalScopeTimer(bool enabled, ::nexusflow::profiling::TimerRegistry& registry,
                       ::nexusflow::profiling::TimerRegistry::TimerHandle handle,
                       uint64_t windowMs, const char* file, int line)
        : m_registry(enabled ? &registry : nullptr), m_handle(handle) {
        if (m_registry != nullptr && m_handle != 0) {
            m_registry->StartAverage(
                m_handle, ::nexusflow::profiling::TimerPrintPolicy::EveryMs(windowMs),
                file, line);
        }
    }

    ~OptionalScopeTimer() {
        if (m_registry != nullptr && m_handle != 0) {
            m_registry->EndAverage(m_handle);
        }
    }

    OptionalScopeTimer(const OptionalScopeTimer&) = delete;
    OptionalScopeTimer& operator=(const OptionalScopeTimer&) = delete;

private:
    ::nexusflow::profiling::TimerRegistry* m_registry = nullptr;
    ::nexusflow::profiling::TimerRegistry::TimerHandle m_handle = 0;
};

}  // namespace


Worker::Worker(const std::shared_ptr<Module>& modulePtr, const ViewPtr<Config>& configPtr,
               const std::shared_ptr<PipelineContext>& context) {
    m_modulePtr = modulePtr;
    m_configPtr = configPtr;
    m_context = context;
    m_stopFlag = false;
    LoadConfig();
}

profiling::TimerRegistry& Worker::Profiler() {
    return m_context ? m_context->Profiler() : ::nexusflow::profiling::TimerRegistry::Instance();
}

Worker::~Worker() {
    // Signal the worker to stop. Joining the thread is ModuleActor's job; this
    // only guarantees the loop is asked to exit before the Worker is freed.
    LOG_TRACE("Destroying worker for module: {}", m_modulePtr->GetModuleName());
    if (!m_stopFlag.load()) {
        LOG_WARN("Worker for module '{}' is still running when destroyed, requesting stop.", m_modulePtr->GetModuleName());
        Stop();
    }
}

ErrorCode Worker::Start() {
    m_stopFlag.store(false);
    if (!m_modulePtr) {
        return ErrorCode::SUCCESS;
    }
    // Queued outputs are handed to the dispatcher only when Process() returns,
    // so a run that ended mid-call left them in the module. They would be
    // broadcast in the middle of the next run, to a pipeline that may already
    // be somewhere else entirely.
    m_modulePtr->DiscardPendingOutputs();
    return ErrorCode::SUCCESS;
}

bool Worker::IsRunning() const {
    std::lock_guard<std::mutex> lock(m_loopMutex);
    return !m_loopExited;
}

void Worker::LoadConfig() {
    if (!m_modulePtr) {
        return;
    }
    const std::string moduleName = m_modulePtr->GetModuleName();

    // Batch size / timeout used when pulling from the input queues.
    int maxBatchSize = m_configPtr->GetValueOrDefault<int>("maxBatchSize", static_cast<int>(m_maxBatchSize));
    if (maxBatchSize < 1) {
        maxBatchSize = 1; // 0 would make every pull block on an impossible batch size.
    }
    m_maxBatchSize = static_cast<std::size_t>(maxBatchSize);

    int batchTimeoutMs = m_configPtr->GetValueOrDefault<int>("batchTimeoutMs", static_cast<int>(m_batchTimeout.count()));
    if (batchTimeoutMs < 0) {
        batchTimeoutMs = 0;
    }
    m_batchTimeout = std::chrono::milliseconds(batchTimeoutMs);

    int fusionTimeoutMs = m_configPtr->GetValueOrDefault<int>("fusionTimeoutMs", static_cast<int>(m_fusionTimeout.count()));
    if (fusionTimeoutMs < 0) {
        fusionTimeoutMs = 0;
    }
    m_fusionTimeout = std::chrono::milliseconds(fusionTimeoutMs);

    int sourceIdleMs = m_configPtr->GetValueOrDefault<int>("sourceIdleMs", m_sourceIdleMs);
    if (sourceIdleMs < 0) {
        sourceIdleMs = 0;
    }
    m_sourceIdleMs = sourceIdleMs;

    m_syncInputs = m_configPtr->GetValueOrDefault<bool>("syncInputs", m_syncInputs);
    m_profileTimers = m_configPtr->GetValueOrDefault<bool>("profileTimers", m_profileTimers);

    // Timer labels are built once here instead of on every loop iteration.
    m_processTimerName = "Worker.Process." + moduleName;
    m_waitTimerName = "Worker.Wait." + moduleName;
    // Interning resolves each label to a handle once, so a profiled iteration
    // only touches the clock and the aggregate, never the name.
    if (m_profileTimers) {
        m_processTimer = Profiler().Intern(m_processTimerName);
        m_waitTimer = Profiler().Intern(m_waitTimerName);
    }
}

ErrorCode Worker::Stop() {
    // Idempotent by design: the pipeline and the owning ModuleActor can both
    // request a stop (for example from ~Pipeline after an explicit Stop).
    LOG_TRACE("Stopping worker for module: {}", m_modulePtr->GetModuleName());
    m_stopFlag.store(true);
    return ErrorCode::SUCCESS;
}

void Worker::WorkLoop() {
    LOG_DEBUG("Worker for module '{}' started", m_modulePtr->GetModuleName());

    // Route this worker's records to the pipeline's sink when it installed one;
    // the thread-local override keeps the hot macro path free of plumbing.
    if (m_context && m_context->HasOwnSink()) {
        ::nexusflow::log::detail::SetThreadSink(m_context->GetSink());
    }

    bool isSourceModule = m_inputQueueMap.empty(); // Check if this is a source module.

    LOG_DEBUG("Worker for module '{}' is running. Is source module: {}. Is sync inputs: {}.", m_modulePtr->GetModuleName(),
              isSourceModule, m_syncInputs);

    if (m_syncInputs) {
        assert(!isSourceModule);
        RunFusion(); // Run the fusion module.
    } else {
        // Per-module timing splits each loop iteration into the time spent
        // waiting for work and the time spent processing it. The difference
        // tells a starved stage (waiting on upstream) from a bottleneck stage
        // (busy), which is what pipeline-level throughput tuning needs.
        // Labels carry the driven module so the profile reads
        // "Worker.Wait.<Module>" / "Worker.Process.<Module>".
        const std::string moduleName = m_modulePtr->GetModuleName();
        // A source module never blocks on an input queue. When an invocation
        // produced no output, yield briefly instead of spinning a core at 100%.

        while (!m_stopFlag.load()) {
            // Queue plumbing sits outside the module calls; a stray exception here
            // must not take the worker thread (and with it the process) down.
            try {
                if (isSourceModule) {
                    // Source Module Loop
                    Message emptyMessage;
                    bool produced = false;
                    try {
                        OptionalScopeTimer processTimer(m_profileTimers, Profiler(), m_processTimer, 5000,
                                                        __FILE__, __LINE__);
                        produced = m_modulePtr->ProcessTimed(emptyMessage);
                        if (produced) {
                            ++m_processedCount;
                        }
                    } catch (const std::exception& e) {
                        LOG_ERROR("Module '{}' threw while processing: {}", moduleName, e.what());
                    } catch (...) {
                        LOG_ERROR("Module '{}' threw an unknown exception while processing.", moduleName);
                    }
                    if (!produced && m_sourceIdleMs > 0) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(m_sourceIdleMs));
                    }
                } else {
                    // Sink or Filter/Transformer Module Loop
                    std::vector<Message> batchMessage;
                    {
                        OptionalScopeTimer waitTimer(m_profileTimers, Profiler(), m_waitTimer, 5000,
                                                     __FILE__, __LINE__);
                        batchMessage = PullBatchMessage(m_maxBatchSize, m_batchTimeout);
                    }
                    if (batchMessage.empty()) continue;

                    // One item per message in the batch, so the reported cost is
                    // per message rather than per dispatch.
                    if (m_profileTimers) {
                        Profiler().StartAverage(m_processTimer,
                                                ::nexusflow::profiling::TimerPrintPolicy::EveryMs(5000),
                                                __FILE__, __LINE__);
                        Profiler().IncrementAverage(m_processTimer,
                                                    static_cast<uint64_t>(batchMessage.size()));
                    }
                    try {
                        m_modulePtr->ProcessBatch(batchMessage);
                        m_processedCount.fetch_add(batchMessage.size(), std::memory_order_relaxed);
                    } catch (const std::exception& e) {
                        LOG_ERROR("Module '{}' threw while processing a batch: {}", moduleName, e.what());
                    } catch (...) {
                        LOG_ERROR("Module '{}' threw an unknown exception while processing a batch.", moduleName);
                    }
                    if (m_profileTimers) {
                        Profiler().EndAverage(m_processTimer);
                    }
                }
            } catch (const std::exception& e) {
                LOG_ERROR("Worker for module '{}' caught an unexpected exception: {}", moduleName, e.what());
            } catch (...) {
                LOG_ERROR("Worker for module '{}' caught an unknown unexpected exception.", moduleName);
            }
        }
    }

    ::nexusflow::log::detail::ResetThreadSink();
    // Release anyone waiting on WaitForStopRequest(); this has to happen before
    // the thread says it is done, otherwise the join would race the flag.
    {
        std::lock_guard<std::mutex> lock(m_loopMutex);
        m_loopExited = true;
    }
    m_loopCond.notify_all();
    LOG_DEBUG("Worker for module '{}' finished.", m_modulePtr->GetModuleName());
}

void Worker::RunFusion() {
    // TODO: 濡備綍浼樺寲鍛紝鐜板湪鍙湁鍗旴atch, 骞朵笖闇€瑕佹祴璇曚笅鍐呭瓨鍗犵敤.

    // Key: message id, value: map of source module name and message.
    // Keyed by the full 64-bit message id (this used to be int and truncated it).
    std::unordered_map<uint64_t, std::unordered_map<std::string, Message>> messageCache;
    // First-seen time per message id, in steady-clock milliseconds. Message
    // timestamps come from system_clock, so they cannot be compared against the
    // steady-clock "now" used for the timeout (that comparison never fired).
    std::unordered_map<uint64_t, uint64_t> cacheArrivalMs;
    const std::size_t expectedInputCount = m_inputQueueMap.size(); // Expected number of inputs.

    // Partial fused messages are dropped after this many milliseconds.
    const uint64_t timeoutMs = static_cast<uint64_t>(m_fusionTimeout.count());

    // One message id is in flight per input at most, so the cache stays close to
    // this size; reserving keeps the per-message inserts off the rehash path.
    messageCache.reserve(std::max<std::size_t>(expectedInputCount, 8));
    cacheArrivalMs.reserve(std::max<std::size_t>(expectedInputCount, 8));

    // Reused across iterations: a fresh vector per fused message meant one heap
    // allocation per fused batch.
    std::vector<Message> fusedMessageVec;

    while (!m_stopFlag.load()) {
        uint64_t currentTimeMs =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();

        // collect message from all inputs
        std::vector<Message> batchMessage;
        for (auto& queuePair : m_inputQueueMap) {
            auto& queue = queuePair.second;

            Message message;
            if (queue->tryPop(message)) {
                auto messageMeta = message.GetMetaData();
                auto messageId = messageMeta.messageId;
                auto& sourceModuleName = messageMeta.sourceName;
                cacheArrivalMs.emplace(messageId, currentTimeMs); // Keeps the first-seen time.
                messageCache[messageId][sourceModuleName] = message;
                LOG_DEBUG("Message with ID: {} received from source module: {}", messageId, sourceModuleName);
            }
        }

        // check message is already in cache
        for (auto it = messageCache.begin(); it != messageCache.end();) {
            auto messageId = it->first;
            auto& messageMap = it->second;

            LOG_TRACE("Checking message with ID: {}", messageId);
            LOG_TRACE("Number of inputs received: {}, Expected inputs: {}, module name: {}", messageMap.size(), expectedInputCount,
                      m_modulePtr->GetModuleName());
            if (messageMap.size() == expectedInputCount) {
                // Construct a fused message and concat to batchMessage.
                fusedMessageVec.clear();
                fusedMessageVec.push_back(MakeMessage(std::move(messageMap)));
                try {
                    m_modulePtr->ProcessBatch(fusedMessageVec); // process the fused message
                    m_processedCount.fetch_add(fusedMessageVec.size(), std::memory_order_relaxed);
                } catch (const std::exception& e) {
                    LOG_ERROR("Module '{}' threw while processing a fused batch: {}", m_modulePtr->GetModuleName(), e.what());
                } catch (...) {
                    LOG_ERROR("Module '{}' threw an unknown exception while processing a fused batch.",
                              m_modulePtr->GetModuleName());
                }
                cacheArrivalMs.erase(messageId);
                it = messageCache.erase(it); // remove the message from cache
            } else {
                const auto arrivalIt = cacheArrivalMs.find(messageId);
                const bool fusionTimedOut = arrivalIt != cacheArrivalMs.end() && currentTimeMs - arrivalIt->second >= timeoutMs;
                // A drain only means "stop waiting for inputs that cannot arrive".
                // As long as this stage reads from an open input it keeps converting
                // them, so the decision to forward an incomplete fusion group belongs
                // to the iteration that sees every input closed, not to every
                // iteration while draining. Deciding earlier counts the missing side
                // as dropped on each pass and leaves the producer's messages unread
                // for the whole stop timeout.
                bool nextIterationPlanned = false;
                for (auto& queuePair : m_inputQueueMap) {
                    if (!queuePair.second->isShutdown()) {
                        nextIterationPlanned = true;
                        break;
                    }
                }
                const bool drainGaveUp = m_drainRequested.load() && !nextIterationPlanned;
                const bool timedOut = fusionTimedOut || drainGaveUp;
                if (timedOut) {
                    if (drainGaveUp && !fusionTimedOut) {
                        LOG_DEBUG("Drain requested; forwarding incomplete message {} without waiting for the missing inputs",
                                  messageId);
                        fusedMessageVec.clear();
                        fusedMessageVec.push_back(MakeMessage(std::move(messageMap)));
                        const std::size_t missing =
                            expectedInputCount > messageMap.size() ? expectedInputCount - messageMap.size() : 0;
                        try {
                            m_modulePtr->ProcessBatch(fusedMessageVec);
                            m_processedCount.fetch_add(fusedMessageVec.size(), std::memory_order_relaxed);
                        } catch (const std::exception& e) {
                            LOG_ERROR("Module '{}' threw while processing a partially fused batch: {}",
                                      m_modulePtr->GetModuleName(), e.what());
                        } catch (...) {
                            LOG_ERROR("Module '{}' threw an unknown exception while processing a partially fused batch.",
                                      m_modulePtr->GetModuleName());
                        }
                        m_droppedCount.fetch_add(missing, std::memory_order_relaxed);
                    } else {
                        LOG_WARN("Timeout for message with ID: {}, will be removed from cache", messageId);
                        // Every partial in the cache is one fused message that will
                        // never reach the module, so it counts as dropped output.
                        m_droppedCount.fetch_add(messageMap.size(), std::memory_order_relaxed);
                    }
                    cacheArrivalMs.erase(arrivalIt);
                    it = messageCache.erase(it);
                } else {
                    ++it;
                }
            }
        }

#if 0
        // Print the current state of the message cache.
        LOG_TRACE("Message cache state:");
        for (auto& pair : messageCache) {
            LOG_TRACE("Message ID: {}", pair.first);
            for (auto& innerPair : pair.second) {
                LOG_TRACE("Source module: {}, Timestamp: {}", innerPair.first, innerPair.second.GetMetaData().timstamp);
            }
        }
#endif
    }
}

std::vector<Message> Worker::PullBatchMessage(size_t maxBatchSize, std::chrono::milliseconds batchTimeout) {
    // Initialize the batch vector.
    std::vector<Message> batchMessage;

    // Ensure the output vector is clean and has pre-allocated memory.
    batchMessage.clear();
    batchMessage.reserve(maxBatchSize);

    auto startTime = std::chrono::steady_clock::now();

    // --- Phase 1: Greedy non-blocking pull ---
    // Quickly drain any messages that are already waiting in the queues.
    for (auto& item : m_inputQueueMap) {
        auto& queue = item.second;
        while (batchMessage.size() < maxBatchSize) {
            Message message;
            if (queue->tryPop(message)) {
                batchMessage.push_back(std::move(message));
            } else {
                // This queue is empty, so move on to the next one.
                break;
            }
        }
        if (batchMessage.size() >= maxBatchSize) {
            return batchMessage; // Batch is full, no need to wait.
        }
    }

    // --- Phase 2: Short-blocking polling loop ---
    // If the batch is not yet full, enter a polling loop that waits efficiently.
    while (!m_stopFlag.load()) {
        // Check exit condition: batch is full.
        if (batchMessage.size() >= maxBatchSize) {
            break;
        }

        // Check exit condition: total time has elapsed.
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - startTime);
        if (elapsed >= batchTimeout) {
            break;
        }

        // Iterate through all input queues and perform a short wait on each.
        for (auto& item : m_inputQueueMap) {
            auto& queue = item.second;
            Message msg;
            // Wait for a very short period (e.g., 1ms). This is the key to avoiding
            // busy-waiting while remaining responsive to multiple inputs.
            if (queue->waitAndPopFor(msg, std::chrono::milliseconds(1))) {
                batchMessage.push_back(std::move(msg));
                // Optimization: If a message was found, this queue might have more.
                // Try to pop more in a non-blocking way to fill the batch faster.
                while (batchMessage.size() < maxBatchSize) {
                    Message message;
                    if (queue->tryPop(message)) {
                        batchMessage.push_back(std::move(message));
                    } else {
                        break; // The queue is now empty.
                    }
                }
            }
            // Check if the batch became full during the inner loop.
            if (batchMessage.size() >= maxBatchSize) {
                break;
            }
        }
    }

    return batchMessage;
}

}} // namespace nexusflow::core
