#ifndef NEXUSFLOW_QUEUE_FULL_POLICY_HPP
#define NEXUSFLOW_QUEUE_FULL_POLICY_HPP

namespace nexusflow {

/**
 * @enum QueueFullPolicy
 * @brief What a module does when one of its output queues is full.
 *
 * kBlock is the historical behaviour and stays the default: the push blocks, so
 * a slow stage slows its producers down instead of losing work.
 *
 * kDrop trades work for liveness. Before a message is pushed, every output queue
 * is checked; if any of them is full the message is discarded as a whole and
 * counted, and the producer moves on. All-or-nothing matters: a
 * half-delivered message would leave a join group that can never complete.
 *
 * kDropOldest also trades work for liveness, but keeps the newest frames: when an
 * output queue is full the oldest buffered message is evicted and the new one is
 * queued in its place. A real-time consumer that is slower than its source then
 * always sees the freshest frame instead of a backlog that lags further behind
 * every second. Evicted messages are charged to the edge
 * (EdgeSnapshot::discardedTotal), never to a module's droppedTotal: the producer
 * already counted them as processed and pushed.
 */
enum class QueueFullPolicy {
    kBlock = 0,      ///< Block until the queue has room (default).
    kDrop = 1,       ///< Drop the message that cannot be delivered.
    kDropOldest = 2  ///< Evict the oldest queued message, then queue the new one.
};

} // namespace nexusflow

#endif // NEXUSFLOW_QUEUE_FULL_POLICY_HPP
