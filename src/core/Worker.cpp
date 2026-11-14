#include "Worker.hpp"
#include "nexusflow/ErrorCode.hpp"
#include "nexusflow/Message.hpp"
#include "nexusflow/Logging.hpp"
#include "nexusflow/TimerRegistry.hpp"
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

namespace nexusflow { namespace core {

Worker::Worker(const std::shared_ptr<Module>& modulePtr, const ViewPtr<Config>& configPtr) {
    m_modulePtr = modulePtr;
    m_configPtr = configPtr;
    m_stopFlag = false;
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

void Worker::Reset() { m_stopFlag.store(false); }

ErrorCode Worker::Stop() {
    // Idempotent by design: the pipeline and the owning ModuleActor can both
    // request a stop (for example from ~Pipeline after an explicit Stop).
    LOG_TRACE("Stopping worker for module: {}", m_modulePtr->GetModuleName());
    m_stopFlag.store(true);
    return ErrorCode::SUCCESS;
}

void Worker::WorkLoop() {
    LOG_DEBUG("Worker for module '{}' started", m_modulePtr->GetModuleName());

    bool isSourceModule = m_inputQueueMap.empty(); // Check if this is a source module.

    // TODO: configure the batch size and timeout from the module config.
    constexpr size_t kMaxBatchSize = 4;
    constexpr std::chrono::milliseconds kBatchTimeout{100};

    // TODO: get value from config.
    bool isSyncInputs = false;
    isSyncInputs = m_configPtr->GetValueOrDefault<bool>("syncInputs", isSyncInputs);

    LOG_DEBUG("Worker for module '{}' is running. Is source module: {}. Is sync inputs: {}.", m_modulePtr->GetModuleName(),
              isSourceModule, isSyncInputs);

    if (isSyncInputs) {
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
        int sourceIdleMs = m_configPtr->GetValueOrDefault<int>("sourceIdleMs", 1);
        if (sourceIdleMs < 0) {
            sourceIdleMs = 0;
        }

        while (!m_stopFlag.load()) {
            if (isSourceModule) {
                // Source Module Loop
                Message emptyMessage;
                bool produced = false;
                try {
                    TIMER_SCOPE_AVERAGE_MS("Worker.Process." + moduleName, 5000);
                    produced = m_modulePtr->ProcessTimed(emptyMessage);
                } catch (const std::exception& e) {
                    LOG_ERROR("Module '{}' threw while processing: {}", moduleName, e.what());
                } catch (...) {
                    LOG_ERROR("Module '{}' threw an unknown exception while processing.", moduleName);
                }
                if (!produced && sourceIdleMs > 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(sourceIdleMs));
                }
            } else {
                // Sink or Filter/Transformer Module Loop
                std::vector<Message> batchMessage;
                {
                    TIMER_SCOPE_AVERAGE_MS("Worker.Wait." + moduleName, 5000);
                    batchMessage = PullBatchMessage(kMaxBatchSize, kBatchTimeout);
                }
                if (batchMessage.empty()) continue;

                // One item per message in the batch, so the reported cost is
                // per message rather than per dispatch.
                TIMER_START_AVERAGE_MS("Worker.Process." + moduleName, 5000);
                TIMER_INCREMENT_AVERAGE("Worker.Process." + moduleName,
                                        static_cast<uint64_t>(batchMessage.size()));
                try {
                    m_modulePtr->ProcessBatch(batchMessage);
                } catch (const std::exception& e) {
                    LOG_ERROR("Module '{}' threw while processing a batch: {}", moduleName, e.what());
                } catch (...) {
                    LOG_ERROR("Module '{}' threw an unknown exception while processing a batch.", moduleName);
                }
                TIMER_STOP_AVERAGE("Worker.Process." + moduleName);
            }
        }
    }

    LOG_DEBUG("Worker for module '{}' finished.", m_modulePtr->GetModuleName());
}

void Worker::RunFusion() {
    // TODO: 如何优化呢，现在只有单Batch, 并且需要测试下内存占用.

    // Key: message id, value: map of source module name and message.
    std::unordered_map<int, std::unordered_map<std::string, Message>> messageCache; // Cache for messages.
    const int expectedInputCount = m_inputQueueMap.size(); // Expected number of inputs.

    // Define a timeout period.
    constexpr std::chrono::minutes timeout{1};
    uint64_t timeoutMs = std::chrono::duration_cast<std::chrono::milliseconds>(timeout).count();

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
                auto fusedMessage = MakeMessage(std::move(messageMap));
                std::vector<Message> fusedMessageVec{fusedMessage};
                try {
                    m_modulePtr->ProcessBatch(fusedMessageVec); // process the fused message
                } catch (const std::exception& e) {
                    LOG_ERROR("Module '{}' threw while processing a fused batch: {}", m_modulePtr->GetModuleName(), e.what());
                } catch (...) {
                    LOG_ERROR("Module '{}' threw an unknown exception while processing a fused batch.",
                              m_modulePtr->GetModuleName());
                }
                it = messageCache.erase(it); // remove the message from cache
            } else if (!messageMap.empty() && messageMap.begin()->second.GetMetaData().timestamp < (currentTimeMs - timeoutMs)) {
                // timeout
                LOG_WARN("Timeout for message with ID: {}, will be removed from cache", messageId);
                it = messageCache.erase(it);
            } else {
                ++it;
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
