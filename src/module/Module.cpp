#include "nexusflow/Module.hpp"
#include "dispatcher/Dispatcher.hpp"
#include "nexusflow/Config.hpp"
#include "nexusflow/ErrorCode.hpp"
#include "nexusflow/Message.hpp"
#include "nexusflow/Logging.hpp"
#include "nexusflow/PipelineContext.hpp"

#include <chrono>
#include <utility>

namespace nexusflow {

Module::Module(std::string name) : m_moduleName(std::move(name)) {
    // Initialize the module with the given name.
    LOG_TRACE("Module '{}' created.", m_moduleName);
}

Module::~Module() {
    // Ensure the module is stopped before destruction.
    LOG_TRACE("Module '{}' destroying...", m_moduleName);
};

ErrorCode Module::Configure(const Config& config) {
    (void)config; // the base implementation does not need any settings
    // Initialize the module.
    LOG_TRACE("Module '{}' initializing...", m_moduleName);
    return ErrorCode::SUCCESS;
}

ErrorCode Module::Init() {
    // Initialize the module.
    LOG_TRACE("Module '{}' initializing...", m_moduleName);
    return ErrorCode::SUCCESS;
}

ErrorCode Module::DeInit() {
    // De-initialize the module.
    LOG_TRACE("Module '{}' de-initializing...", m_moduleName);
    return ErrorCode::SUCCESS;
}

void Module::ProcessBatch(std::vector<Message>& inputBatchMessages) {
    // Process the batch of input messages.
    LOG_DEBUG("Module '{}' processing batch of {} messages.", m_moduleName, inputBatchMessages.size());
    for (auto& message : inputBatchMessages) {
        ProcessTimed(message);
    }
}

bool Module::ProcessTimed(Message& inputMessage) {
    m_pendingOutputs.clear();
    m_activeInputTimingMs = inputMessage.GetMetaData().GetModuleTimings();
    m_processing = true;
    const auto started = std::chrono::steady_clock::now();

    try {
        Process(inputMessage);
    } catch (...) {
        m_processing = false;
        m_pendingOutputs.clear();
        m_activeInputTimingMs.clear();
        throw;
    }

    const double elapsedMs = std::chrono::duration<double, std::milli>(
        std::chrono::steady_clock::now() - started).count();
    m_processing = false;

    auto pending = std::move(m_pendingOutputs);
    m_pendingOutputs.clear();
    const bool produced = !pending.empty();
    for (const auto& output : pending) {
        DispatchOutput(output, elapsedMs);
    }
    m_activeInputTimingMs.clear();

    LOG_DEBUG("ModuleTiming: module='{}' elapsed_ms={:.3f}", m_moduleName, elapsedMs);
    return produced;
}

void Module::DispatchOutput(const PendingOutput& output, double elapsedMs) {
    Message routed = output.message;
    auto& timings = routed.MetaData().MutableModuleTimings();
    for (const auto& entry : m_activeInputTimingMs) {
        if (timings.find(entry.first) == timings.end()) {
            timings[entry.first] = entry.second;
        }
    }
    timings[m_moduleName] = elapsedMs;

    if (output.named) {
        if (m_dispatcherPtr != nullptr) {
            LOG_DEBUG("Module '{}' sending message to '{}'.", m_moduleName, output.outputName);
            if (!m_dispatcherPtr->SendTo(output.outputName, routed)) {
                LOG_WARN("Module '{}' could not send message to '{}': the output is missing or shut down.",
                         m_moduleName, output.outputName);
            }
        }
    } else if (m_dispatcherPtr != nullptr) {
        LOG_DEBUG("Module '{}' broadcasting message.", m_moduleName);
        m_dispatcherPtr->Broadcast(std::move(routed));
    }
}

void Module::Broadcast(const Message& message) {
    if (m_processing) {
        m_pendingOutputs.push_back(PendingOutput{false, std::string(), message});
        return;
    }
    if (m_dispatcherPtr != nullptr) {
        LOG_DEBUG("Module '{}' broadcasting message.", m_moduleName);
        m_dispatcherPtr->Broadcast(message);
    } else {
        LOG_WARN("Module '{}' has no handle, cannot broadcast message.", m_moduleName);
    }
}

void Module::Broadcast(Message&& message) {
    if (m_processing) {
        m_pendingOutputs.push_back(PendingOutput{false, std::string(), std::move(message)});
        return;
    }
    if (m_dispatcherPtr != nullptr) {
        LOG_DEBUG("Module '{}' broadcasting message.", m_moduleName);
        m_dispatcherPtr->Broadcast(std::move(message));
    } else {
        LOG_WARN("Module '{}' has no handle, cannot broadcast message.", m_moduleName);
    }
}

bool Module::SendTo(const std::string& outputName, const Message& msg) {
    if (m_processing) {
        m_pendingOutputs.push_back(PendingOutput{true, outputName, msg});
        return true;
    }
    if (m_dispatcherPtr != nullptr) {
        LOG_DEBUG("Module '{}' sending message to '{}'.", m_moduleName, outputName);
        return m_dispatcherPtr->SendTo(outputName, msg);
    }
    LOG_WARN("Module '{}' has no handle, cannot send message.", m_moduleName);
    return false;
}

const std::string& Module::GetModuleName() const { return m_moduleName; }

void Module::AttachContext(std::shared_ptr<PipelineContext> context) { m_context = std::move(context); }

std::shared_ptr<PipelineContext> Module::GetContext() const { return m_context; }

void Module::SetDispatcher(const std::shared_ptr<dispatcher::Dispatcher>& dispatcher) { m_dispatcherPtr = dispatcher; }

} // namespace nexusflow
