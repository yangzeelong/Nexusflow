#include "Dispatcher.hpp"
#include "nexusflow/Config.hpp"

#include <iterator>
#include <utility>

namespace nexusflow { namespace dispatcher {

Dispatcher::Dispatcher(const ViewPtr<Config>& configView) { m_configView = configView; };

Dispatcher::~Dispatcher() = default;

void Dispatcher::Broadcast(const Message& message) {
    for (auto& pair : m_subscriberMap) {
        auto& subscriber = pair.second;
        // Apply backpressure instead of silently dropping frames when a
        // downstream inference module is slower than the source. A false return
        // value means the queue was shut down while we were broadcasting.
        if (!subscriber->push(message)) {
            LOG_WARN("Broadcast to output queue {} failed: the queue is shut down", pair.first);
        }
    }
}

void Dispatcher::Broadcast(Message&& message) {
    if (m_subscriberMap.empty()) {
        return;
    }
    auto it = m_subscriberMap.begin();
    const auto last = std::prev(m_subscriberMap.end());
    for (; it != last; ++it) {
        if (!it->second->push(message)) {
            LOG_WARN("Broadcast to output queue {} failed: the queue is shut down", it->first);
        }
    }
    // The last subscriber takes ownership of the message instead of copying it.
    if (!last->second->push(std::move(message))) {
        LOG_WARN("Broadcast to output queue {} failed: the queue is shut down", last->first);
    }
}

bool Dispatcher::SendTo(const std::string& outputName, const Message& msg) {
    auto it = m_subscriberMap.find(outputName);
    if (it == m_subscriberMap.end()) {
        LOG_WARN("SendTo failed: output queue {} does not exist", outputName);
        return false;
    }
    if (!it->second->push(msg)) {
        LOG_WARN("SendTo output queue {} failed: the queue is shut down", outputName);
        return false;
    }
    return true;
}

}} // namespace nexusflow::dispatcher
