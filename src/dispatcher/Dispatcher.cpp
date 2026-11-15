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
        if (!PushTo(subscriber, message)) {
            RecordDroppedPush(pair.first);
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
        if (!PushTo(it->second, message)) {
            RecordDroppedPush(it->first);
        }
    }
    // The last subscriber takes ownership of the message instead of copying it.
    if (!PushTo(last->second, std::move(message))) {
        RecordDroppedPush(last->first);
    }
}

bool Dispatcher::SendTo(const std::string& outputName, const Message& msg) {
    auto it = m_subscriberMap.find(outputName);
    if (it == m_subscriberMap.end()) {
        LOG_WARN("SendTo failed: output queue {} does not exist", outputName);
        return false;
    }
    if (!PushTo(it->second, msg)) {
        RecordDroppedPush(outputName);
        return false;
    }
    return true;
}

}} // namespace nexusflow::dispatcher
