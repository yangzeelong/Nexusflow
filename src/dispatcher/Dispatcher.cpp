#include "Dispatcher.hpp"
#include "nexusflow/Config.hpp"

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
