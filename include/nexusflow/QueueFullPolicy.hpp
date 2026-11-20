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
 * kDrop trades work for liveness. Before a message is pushed, every
 * output queue is checked; if any of them is full the message is discarded as a
 * whole and counted, and the producer moves on. All-or-nothing matters: a
 * half-delivered message would leave a join group that can never complete.
 */
enum class QueueFullPolicy {
    kBlock = 0, ///< Block until the queue has room (default).
    kDrop = 1   ///< Drop the message that cannot be delivered.
};

} // namespace nexusflow

#endif // NEXUSFLOW_QUEUE_FULL_POLICY_HPP
