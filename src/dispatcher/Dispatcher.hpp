#ifndef NEXUSFLOW_DISPATCHER_HPP
#define NEXUSFLOW_DISPATCHER_HPP

#include "base/Define.hpp"
#include <nexusflow/common/ViewPtr.hpp>
#include "nexusflow/QueueFullPolicy.hpp"
#include "nexusflow/Config.hpp"
#include "nexusflow/Message.hpp"
#include "nexusflow/Logging.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <thread>
#include <utility>
#include <unordered_map>
#include <vector>

namespace nexusflow { namespace dispatcher {

/**
 * @class Dispatcher
 * @brief An internal helper class responsible for dispatching messages to downstream queues.
 *
 * This class encapsulates the logic for broadcasting or sending messages to a set
 * of pre-configured output queues. It holds non-owning "view" pointers to the queues,
 * which are owned and managed by the Pipeline.
 *
 * This is an implementation detail of the framework and is not part of the public API.
 */
class Dispatcher {
public:
    Dispatcher(const ViewPtr<Config>& configView);

    ~Dispatcher();

    /**
     * @brief Broadcasts a message to all configured output queues.
     * @details Each subscriber receives its own copy of the message; Message
     * copies are cheap because the payload is shared copy-on-write. Pushes are
     * blocking, so a slow subscriber applies backpressure to the producer.
     * @param msg The message to broadcast.
     */
    void Broadcast(const Message& msg);

    /**
     * @brief Broadcasts a message, moving it into the last output queue.
     * @details The first N-1 subscribers each receive a copy and the last one
     * receives the moved message, which saves one Message copy per broadcast.
     * After this call the caller's message is in a moved-from state.
     * @param msg The message to broadcast.
     */
    void Broadcast(Message&& msg);

    /**
     * @brief Sends a message to a specific output queue.
     * @param outputName The name of the output queue to send the message to.
     * @param msg The message to send.
     * @return true when the message was pushed to the named output queue; false
     * when the output is unknown or its queue has already been shut down.
     */
    bool SendTo(const std::string& outputName, const Message& msg);

    /**
     * @brief Adds a new output queue to the dispatcher.
     * @param name The name of the output queue.
     * @param queue The output queue to add.
     */
    /// Sets what Broadcast does when an output queue is full. Default kBlock.
    void SetQueueFullPolicy(QueueFullPolicy policy) { m_policy = policy; }

    /// Messages the drop policy discarded before they reached any queue.
    std::uint64_t PolicyDroppedCount() const {
        return m_policyDroppedCount.load(std::memory_order_relaxed);
    }

    void AddSubscriber(const std::string& name, ViewPtr<MessageQueue> queue) {
        if (m_subscriberMap.find(name) != m_subscriberMap.end()) {
            LOG_ERROR("Output queue with name {} already exists", name);
            throw std::invalid_argument("Output queue with name " + name + " already exists");
        }
        m_subscriberMap[name] = queue;
    }

    /**
     * @brief Messages this dispatcher could not hand to an output queue.
     * @details A push only fails when the queue was shut down underneath the
     * producer, so this counts the messages lost at teardown. It is reported as
     * the owning module's dropped counter: a queued message that never arrived
     * belongs to the module that tried to send it.
     */
    std::uint64_t DroppedCount() const {
        std::atomic_thread_fence(std::memory_order_acquire);
        return m_droppedCount.load(std::memory_order_relaxed);
    }

    /**
     * @brief Returns the edges this dispatcher writes to.
     */
    std::vector<ViewPtr<MessageQueue>> GetOutputQueues() const {
        std::vector<ViewPtr<MessageQueue>> queues;
        queues.reserve(m_subscriberMap.size());
        for (const auto& subscriber : m_subscriberMap) {
            queues.push_back(subscriber.second);
        }
        return queues;
    }

    /**
     * @brief Marks that the pipeline is shutting down, which stops the
     *        per-message drop warning.
     * @details Push failures during a drain are expected: the drain empties
     * stages from the sink backwards, so a producer can still hold a message when
     * its edge closes. The losses are still counted.
     */
    void SetShuttingDown(std::atomic<bool>* flag) { m_shuttingDown = flag; }

    /**
     * @brief Closes every edge this module writes to.
     * @details Called once the producing stage has left its loop: no further
     * message can be handed to these edges, and a closed queue makes the
     * downstream stage's pull return instead of blocking out its batch timeout.
     */
    void ShutdownOutputs() {
        for (const auto& subscriber : m_subscriberMap) {
            subscriber.second->shutdown();
        }
    }

private:
    /**
     * @brief Whether this dispatcher is inside a pipeline drain right now.
     */
    bool ShuttingDown() const {
        return m_shuttingDown && m_shuttingDown->load(std::memory_order_relaxed);
    }

    /**
     * @brief Hands a message to one output queue.
     * @details A push is normally blocking, which is what gives a slow stage
     * backpressure. During a drain that backpressure is a deadlock in waiting: the
     * stage that would consume the queue has already been stopped, so the producer
     * would block on a full queue until its whole stop budget ran out and its
     * worker thread had to be abandoned. While the pipeline shuts down the push
     * therefore fails fast instead, and the message is counted as dropped.
     */
    /// True when every output queue has room for one more message.
    bool OutputsHaveRoom() const {
        for (const auto& pair : m_subscriberMap) {
            ViewPtr<MessageQueue> queue = pair.second;
            if (queue->capacity() > 0 && queue->getSize() >= queue->capacity()) {
                return false;
            }
        }
        return true;
    }

    bool PushTo(ViewPtr<MessageQueue> queue, const Message& msg) {
        if (ShuttingDown()) {
            return queue->tryPush(msg);
        }
        if (m_policy == QueueFullPolicy::kDropOldest) {
            // Evict the oldest buffered message instead of blocking; the edge's
            // discardedTotal records the eviction. During a drain the push above
            // still fails fast rather than evicting from a closing queue.
            return queue->pushEvictOldest(msg);
        }
        return queue->push(msg);
    }

    bool PushTo(ViewPtr<MessageQueue> queue, Message&& msg) {
        if (ShuttingDown()) {
            return queue->tryPush(std::move(msg));
        }
        if (m_policy == QueueFullPolicy::kDropOldest) {
            return queue->pushEvictOldest(std::move(msg));
        }
        return queue->push(std::move(msg));
    }

    void RecordDroppedPush(const std::string& outputName) {
        // Atomic rather than plain: Broadcast and SendTo both run on the owning
        // module's worker thread, but a counter read without an atomic object in
        // between the fence pair is a data race by the letter of the standard.
        m_droppedCount.fetch_add(1, std::memory_order_relaxed);
        std::atomic_thread_fence(std::memory_order_release);
        if (m_shuttingDown && m_shuttingDown->load(std::memory_order_relaxed)) {
            LOG_DEBUG("Broadcast to output queue {} failed during shutdown: the queue is already closed", outputName);
            return;
        }
        LOG_WARN("Broadcast to output queue {} failed: the queue is shut down", outputName);
    }

    ViewPtr<Config> m_configView;
    std::unordered_map<std::string, ViewPtr<MessageQueue>> m_subscriberMap;
    /// Only the owning module's worker thread writes this (Broadcast and SendTo
    /// run there), so the add is relaxed; DroppedCount() fences before it loads.
    std::atomic<std::uint64_t> m_droppedCount{0};
    /// Messages dropped by m_policy before any push; see PolicyDroppedCount().
    std::atomic<std::uint64_t> m_policyDroppedCount{0};
    QueueFullPolicy m_policy{QueueFullPolicy::kBlock};
    std::atomic<bool>* m_shuttingDown = nullptr;
};

}} // namespace nexusflow::dispatcher

#endif // NEXUSFLOW_DISPATCHER_HPP