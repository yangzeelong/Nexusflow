#ifndef NEXUSFLOW_BACKPRESSURE_POLICY_HPP
#define NEXUSFLOW_BACKPRESSURE_POLICY_HPP

namespace nexusflow {

/**
 * @enum BackpressurePolicy
 * @brief What a module does when one of its output edges is full.
 *
 * kNone is the historical behaviour and stays the default: the push blocks, so
 * a slow stage slows its producers down instead of losing work.
 *
 * kDropNewest trades work for liveness. Before a message is pushed, every
 * output edge is checked; if any of them is full the message is discarded as a
 * whole and counted, and the producer moves on. All-or-nothing matters: a
 * half-delivered message would leave a join group that can never complete.
 */
enum class BackpressurePolicy {
    kNone = 0,      ///< Block until the edge has room (default).
    kDropNewest = 1 ///< Drop the message that cannot be delivered.
};

} // namespace nexusflow

#endif // NEXUSFLOW_BACKPRESSURE_POLICY_HPP
