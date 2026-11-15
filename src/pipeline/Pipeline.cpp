
#include "base/Define.hpp"
#include "base/Graph.hpp"
#include "base/GraphUtils.hpp"
#include <nexusflow/common/ViewPtr.hpp>
#include "core/Worker.hpp"
#include "dispatcher/Dispatcher.hpp"
#include "nexusflow/Logging.hpp"
#include <nexusflow/ModuleFactory.hpp>

#include "impl/PipelineImpl.hpp"
#include <yaml-cpp/yaml.h>
#include <memory>
#include <nexusflow/ErrorCode.hpp>
#include <nexusflow/Module.hpp>
#include <nexusflow/Pipeline.hpp>
#include <string>
#include <unordered_map>
#include <vector>

namespace nexusflow {

Pipeline::Pipeline() : m_pImpl(std::make_unique<Pipeline::Impl>()) {}

Pipeline::~Pipeline() {
    if (!m_pImpl) {
        return;
    }
    if (m_pImpl->started) {
        // A running pipeline owns joinable worker threads. Letting them run past
        // this point would call std::terminate from ~std::thread, so stop first.
        LOG_WARN("Pipeline destroyed while still running; stopping its workers.");
        Stop();
    }
}

ErrorCode Pipeline::InitWithGraph(std::unique_ptr<Graph> graph) {
    if (!m_pImpl) {
        return ErrorCode::UNINITIALIZED_ERROR;
    }
    if (!graph) {
        LOG_ERROR("Pipeline::InitWithGraph received an empty graph.");
        m_pImpl->graphReady = false;
        return ErrorCode::UNINITIALIZED_ERROR;
    }
    LOG_DEBUG("Initializing pipeline with graph, graph={}", graph->toString());
    m_pImpl->graph = std::move(graph);
    const ErrorCode code = m_pImpl->Init();
    if (code != ErrorCode::SUCCESS) {
        LOG_ERROR("Pipeline graph initialization failed, code={}", static_cast<int>(code));
        m_pImpl->graphReady = false;
    }
    return code;
}

// --- Public APIs ---
std::unique_ptr<Pipeline> Pipeline::CreateFromYaml(const std::string& configPath) {
    auto graph = graphutils::CreateGraphFromYaml(configPath);
    if (!graph) {
        LOG_ERROR("Failed to create a pipeline graph from '{}'.", configPath);
        return nullptr;
    }

    int queueCapacity = kDefaultMessageQueueCapacity;
    try {
        const YAML::Node root = YAML::LoadFile(configPath);
        const YAML::Node graphNode = root["graph"];
        if (graphNode && graphNode["messageQueueCapacity"]) {
            const int configured = graphNode["messageQueueCapacity"].as<int>();
            if (configured > 0) {
                queueCapacity = configured;
            } else {
                LOG_WARN("graph.messageQueueCapacity must be positive (got {}); using the default of {}.", configured,
                         kDefaultMessageQueueCapacity);
            }
        }
    } catch (const std::exception& e) {
        LOG_WARN("Could not read graph.messageQueueCapacity from '{}': {}; using the default of {}.", configPath,
                 e.what(), kDefaultMessageQueueCapacity);
    }

    auto pipeline = std::unique_ptr<Pipeline>(new Pipeline());
    pipeline->m_pImpl->messageQueueCapacity = queueCapacity;
    if (pipeline->InitWithGraph(std::move(graph)) != ErrorCode::SUCCESS) {
        return nullptr;
    }
    return pipeline;
}

ErrorCode Pipeline::Init() {
    if (!m_pImpl) return ErrorCode::UNINITIALIZED_ERROR;
    if (!m_pImpl->graphReady) {
        LOG_ERROR("Cannot init pipeline: its graph is not ready.");
        return ErrorCode::UNINITIALIZED_ERROR;
    }
    if (m_pImpl->initialized) {
        LOG_WARN("Pipeline is already initialized.");
        return ErrorCode::SUCCESS;
    }

    std::vector<std::shared_ptr<ActorNode>> initializedActors;
    for (auto& actorNode : m_pImpl->actorOrderedNodes) {
        const ErrorCode errCode = actorNode->Init();
        if (errCode != ErrorCode::SUCCESS) {
            LOG_ERROR("Init module failed, actorName={}", actorNode->GetModuleName());
            // Roll back so a failed Init does not leave half-initialized modules behind.
            for (auto it = initializedActors.rbegin(); it != initializedActors.rend(); ++it) {
                (*it)->DeInit();
            }
            return errCode;
        }
        initializedActors.push_back(actorNode);
        LOG_DEBUG("Init module success, actorName={}", actorNode->GetModuleName());
    }
    m_pImpl->initialized = true;
    return ErrorCode::SUCCESS;
}

ErrorCode Pipeline::DeInit() {
    if (!m_pImpl) {
        return ErrorCode::SUCCESS; // Nothing to de-initialize
    }
    if (m_pImpl->started) {
        Stop();
    }
    if (!m_pImpl->initialized) {
        return ErrorCode::SUCCESS;
    }
    LOG_DEBUG("De-initializing pipeline...");

    // Reverse order
    for (auto it = m_pImpl->actorOrderedNodes.rbegin(); it != m_pImpl->actorOrderedNodes.rend(); ++it) {
        auto& actorNode = *it;
        ErrorCode errCode = actorNode->DeInit();
        if (errCode != ErrorCode::SUCCESS) {
            LOG_ERROR("DeInit module failed, actorName={}", actorNode->GetModuleName());
            return errCode;
        } else {
            LOG_DEBUG("DeInit module success, actorName={}", actorNode->GetModuleName());
        }
    }
    m_pImpl->initialized = false;

    LOG_DEBUG("Pipeline de-initialized successfully.");
    return ErrorCode::SUCCESS;
}

ErrorCode Pipeline::Start() {
    if (!m_pImpl) {
        LOG_ERROR("Cannot start pipeline: not initialized.");
        return ErrorCode::UNINITIALIZED_ERROR;
    }
    if (!m_pImpl->graphReady || !m_pImpl->initialized) {
        LOG_ERROR("Cannot start pipeline: it has not been initialized.");
        return ErrorCode::UNINITIALIZED_ERROR;
    }
    if (m_pImpl->started) {
        LOG_WARN("Pipeline is already started.");
        return ErrorCode::FAILED_ALREADY_START;
    }
    LOG_DEBUG("Starting pipeline...");

    std::vector<std::shared_ptr<ActorNode>> startedActors;
    for (auto& actorNode : m_pImpl->actorOrderedNodes) {
        const ErrorCode errCode = actorNode->Start();
        if (errCode != ErrorCode::SUCCESS) {
            LOG_ERROR("Start worker failed, actorName={}", actorNode->GetModuleName());
            // Roll back the workers that already started.
            for (auto it = startedActors.rbegin(); it != startedActors.rend(); ++it) {
                (*it)->Stop();
            }
            return errCode;
        }
        startedActors.push_back(actorNode);
        LOG_DEBUG("Start module success, actorName={}", actorNode->GetModuleName());
    }
    m_pImpl->started = true;
    LOG_DEBUG("Pipeline started successfully.");
    return ErrorCode::SUCCESS;
}

ErrorCode Pipeline::Stop() {
    if (!m_pImpl) {
        return ErrorCode::SUCCESS; // Nothing to stop.
    }
    if (!m_pImpl->started) {
        LOG_DEBUG("Pipeline is already stopped.");
        return ErrorCode::FAILED_ALREADY_STOP;
    }

    LOG_DEBUG("Stopping pipeline...");
    // Shut the queues down first: blocked producers/consumers wake up, and the
    // worker loops can observe the stop flag before we join them.
    for (auto& queue : m_pImpl->queues) {
        queue->shutdown();
    }
    for (auto& actorNode : m_pImpl->actorOrderedNodes) {
        actorNode->Stop();
    }
    m_pImpl->started = false;
    LOG_DEBUG("Pipeline stopped successfully.");
    return ErrorCode::SUCCESS;
}

PipelineMetrics Pipeline::GetMetrics() const {
    PipelineMetrics metrics;
    if (!m_pImpl) {
        return metrics;
    }

    metrics.queues.reserve(m_pImpl->queues.size());
    for (std::size_t i = 0; i < m_pImpl->queues.size(); ++i) {
        const auto stats = m_pImpl->queues[i]->GetStats();
        QueueMetrics queue;
        queue.name = i < m_pImpl->queueNames.size() ? m_pImpl->queueNames[i] : std::string();
        queue.capacity = stats.capacity;
        queue.size = stats.size;
        queue.highWaterMark = stats.highWaterMark;
        queue.pushed = stats.pushed;
        queue.popped = stats.popped;
        metrics.queues.push_back(std::move(queue));
    }

    metrics.modules.reserve(m_pImpl->actorOrderedNodes.size());
    for (const auto& actor : m_pImpl->actorOrderedNodes) {
        ModuleMetrics module;
        module.name = actor->GetModuleName();
        module.processed = actor->ProcessedCount();
        metrics.modules.push_back(std::move(module));
    }
    return metrics;
}

}; // namespace nexusflow
