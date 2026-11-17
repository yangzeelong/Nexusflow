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
    auto first = m_subscriberMap.begin();
    if (first == m_subscriberMap.end()) {
        return;
    }
    // The subscriber that takes ownership of the moved message is picked by
    // iterating from the *second* entry rather than by std::prev(end()):
    // unordered_map's iterator is only a forward iterator, so std::prev on it is
    // undefined behaviour. It happened to work under MSVC and produced a null
    // iterator under libstdc++, which hung (or crashed) the first broadcast.
    auto it = first;
    for (++it; it != m_subscriberMap.end(); ++it) {
        if (!PushTo(it->second, message)) {
            RecordDroppedPush(it->first);
        }
    }
    if (!PushTo(first->second, std::move(message))) {
        RecordDroppedPush(first->first);
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
