
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
#include <nexusflow/PipelineContext.hpp>
#include <nexusflow/Pipeline.hpp>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace nexusflow {

namespace {

// Post-order DFS over the downstream adjacency: a stage is emitted only after
// every stage it feeds, so the result is a valid "sinks first" order. Recursion
// is bounded by the graph depth, not its width, and the visited set keeps a
// malformed (cyclic) graph from recursing forever.
void CollectDownstream(const std::string& name,
                       const std::unordered_map<std::string, std::vector<std::string>>& downstream,
                       std::unordered_set<std::string>& placed, std::vector<std::string>& order) {
    const auto it = downstream.find(name);
    if (it != downstream.end()) {
        for (const auto& dst : it->second) {
            if (dst != name && placed.find(dst) == placed.end()) {
                CollectDownstream(dst, downstream, placed, order);
            }
        }
    }
    placed.insert(name);
    order.push_back(name);
}

} // namespace

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
    m_pImpl->drainInProgress.store(false);
    // A pipeline may be started again after Stop(): the edges were closed at
    // teardown so that every worker was certain to exit, and they have to be
    // reopened before the workers look at them again.
    for (auto& queue : m_pImpl->queues) {
        queue->restart();
    }

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

void Pipeline::BuildStopOrder(std::vector<std::string>& order) const {
    order.clear();
    if (!m_pImpl || !m_pImpl->graph) {
        return;
    }
    // Node names are unique and equal to the module names (see
    // Pipeline::Impl::GetOrCreateActorNode), so the order can be computed on the
    // graph directly and mapped back afterwards.
    std::unordered_map<std::string, std::vector<std::string>> downstream;
    for (const auto& edge : m_pImpl->graph->toEdgeListBFS()) {
        const auto src = edge.srcNodePtr.lock();
        const auto dst = edge.dstNodePtr.lock();
        if (!src || !dst) {
            continue;
        }
        downstream[src->name].push_back(dst->name);
    }

    std::unordered_set<std::string> placed;
    for (const auto& actor : m_pImpl->actorOrderedNodes) {
        const std::string name = actor->GetModuleName();
        if (placed.find(name) == placed.end()) {
            CollectDownstream(name, downstream, placed, order);
        }
    }
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
    // From here on, pushes into an edge that the drain already closed count as
    // expected shutdown loss rather than a warning per message.
    m_pImpl->drainInProgress.store(true);
    // Let cooperative modules observe that the pipeline is going away before the
    // queues are torn down.
    if (m_pImpl->context) {
        m_pImpl->context->RequestStop();
    }

    // Drain instead of discarding. Shutting the queues down first (the previous
    // behaviour) threw away whatever was still buffered on every edge, which is
    // what produced the "the queue is shut down" warnings on every shutdown.
    //
    // The stages are joined sinks-first, which is a valid reverse topological
    // order. Stopping a stage makes it hand over what it already had in hand and
    // then leave its loop; once it is joined it can push no more, so the edges it
    // feeds are closed immediately after it. That tells the stage below "nothing
    // more will ever arrive", which is what lets its batch pull return instead of
    // idling out its timeout.
    //
    // The drain budget is per stage and covers that stage's own blocking pull: a
    // stage that is merely between two pulls (a queue reads empty for a moment
    // and the worker is parked in waitAndPopFor) still drains normally on the
    // next iteration. A single shared budget would spend the whole allowance on
    // the first busy stage upstream and then give the downstream ones nothing.
    std::vector<std::string> stopOrder;
    BuildStopOrder(stopOrder);
    // Every stage learns about the drain up front: a stage that is waiting for
    // inputs that will never arrive (a fusion module missing one of its sources)
    // has to stop waiting now, not when its turn in the order comes up.
    for (const auto& name : stopOrder) {
        auto actorIt = m_pImpl->actorModuleMap.find(name);
        if (actorIt != m_pImpl->actorModuleMap.end() && actorIt->second) {
            actorIt->second->RequestDrain();
        }
    }

    // First pass: ask every stage to stop. A stage that is parked in a blocking
    // read only sees the request once that read returns, so all of them are told
    // at once and none of the requests waits on another stage's progress.
    for (const auto& name : stopOrder) {
        auto actorIt = m_pImpl->actorModuleMap.find(name);
        if (actorIt != m_pImpl->actorModuleMap.end() && actorIt->second) {
            actorIt->second->RequestStop();
        }
    }

    // Producers have to be gone before anything downstream is drained. A source
    // never idles on its own: it keeps filling its output until someone stops it,
    // so a drain that starts while it is still running can never see its edge go
    // empty. Join the producers first, then drain what they handed over.
    //
    // Closing the producer's output edges is what releases it. A producer can be
    // parked inside a blocking push when the stop arrives; that push only ever
    // returns once the queue is either non-full or shut down, and the stage that
    // would consume it has already left. Shutting the edge down now makes the
    // push fail immediately, so the producer leaves its loop inside its budget
    // instead of burning the whole stop timeout and having its thread abandoned.
    // Nothing is lost by closing early: shutdown() only rejects further pushes,
    // it still hands out whatever is already buffered, which is exactly what the
    // downstream drain below then consumes.
    for (const auto& name : stopOrder) {
        auto actorIt = m_pImpl->actorModuleMap.find(name);
        if (actorIt == m_pImpl->actorModuleMap.end() || !actorIt->second) {
            continue;
        }
        const std::shared_ptr<ModuleActor>& actor = actorIt->second;
        if (!actor->IsStarted() || !actor->IsSource()) {
            continue;
        }
        actor->ShutdownOutputs();
        actor->StopAndJoinWithin(m_pImpl->stopTimeout);
    }

    // Second pass, sinks first: let each stage finish what it still has in its
    // input queue, then join it. The edges are closed only after every stage has
    // left its loop, so a producer that was asked to stop while inside a
    // Process() call still finds its output queue open and its "one message in
    // hand" is delivered instead of lost.
    for (const auto& name : stopOrder) {
        auto actorIt = m_pImpl->actorModuleMap.find(name);
        if (actorIt == m_pImpl->actorModuleMap.end() || !actorIt->second) {
            continue;
        }
        const std::shared_ptr<ModuleActor>& actor = actorIt->second;
        if (!actor->IsStarted()) {
            continue;
        }
        const auto drainBudget = m_pImpl->drainTimeout + actor->BlockingTime();
        if (!actor->WaitForInputsDrained(drainBudget)) {
            LOG_WARN("Module '{}' still had buffered input after {} ms of draining; "
                     "stopping it anyway and dropping the rest.",
                     actor->GetModuleName(), static_cast<long long>(drainBudget.count()));
        }
        actor->StopAndJoinWithin(m_pImpl->stopTimeout);
        // The stage can push no more, so the edges it feeds are closed right away:
        // that is what turns the downstream pull into a return instead of a wait
        // for a message that will never come.
        actor->ShutdownOutputs();
    }

    // Nothing can push any more, so every edge can be closed. A stopped pipeline
    // is reusable: Start() with a closed edge used to fail in the middle of its
    // worker rollback, leaving some workers running.
    for (auto& queue : m_pImpl->queues) {
        queue->shutdown();
    }
    m_pImpl->started = false;
    m_pImpl->drainInProgress.store(false);
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
        module.dropped = actor->DroppedCount();
        metrics.modules.push_back(std::move(module));
    }
    return metrics;
}

std::shared_ptr<PipelineContext> Pipeline::GetContext() const {
    if (!m_pImpl) {
        return nullptr;
    }
    return m_pImpl->context;
}

}; // namespace nexusflow
