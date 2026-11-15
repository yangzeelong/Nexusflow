#ifndef NEXUSFLOW_BASE_DEFINE_HPP
#define NEXUSFLOW_BASE_DEFINE_HPP

#include <nexusflow/common/ConcurrentQueue.hpp>
#include <nexusflow/common/SpscQueue.hpp>
#include "nexusflow/Message.hpp"
#include <memory>
#include <unordered_map>

namespace nexusflow {

// Type alias for the internal message queue. Every graph edge has exactly one
// producer (the upstream module worker) and one consumer (the downstream module
// worker), so the edge queue uses the lock-free SpscQueue. A non-positive
// capacity (unbounded) transparently falls back to ConcurrentQueue.
// clang-format off
using MessageQueue     = SpscQueue<Message>;
using MessageQueuePtr  = std::shared_ptr<MessageQueue>;
using MessageQueueUPtr = std::unique_ptr<MessageQueue>;

// clang-format on

} // namespace nexusflow

#endif // NEXUSFLOW_BASE_DEFINE_HPP
