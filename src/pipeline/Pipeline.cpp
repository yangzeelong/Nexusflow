
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
#include <algorithm>
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
    // Mark before recursing, not after: the visited set only guards anything if a
    // cycle sees the node already marked, and marking on the way out left an
    // A -> B -> A graph recursing until the stack ran out. Both build paths
    // reject cycles today, so this is the second line of defence, not the first.
    if (!placed.insert(name).second) {
        return;
    }
    const auto it = downstream.find(name);
    if (it != downstream.end()) {
        for (const auto& dst : it->second) {
            if (dst != name) {
                CollectDownstream(dst, downstream, placed, order);
            }
        }
    }
    order.push_back(name);
}

} // namespace

namespace {

// Reads one module's counters into a snapshot. The counters are plain numbers
// written by the module's own worker thread, so the reader is handed a
// consistent set rather than four separate loads.
ModuleSnapshot TakeModuleSnapshot(const ModuleActor& actor) {
    ModuleSnapshot module;
    module.name = actor.GetModuleName();
    module.processedTotal = actor.ProcessedCount();
    module.droppedTotal = actor.DroppedCount();
    module.fusionExpiredTotal = actor.FusionExpiredCount();
    module.failedTotal = actor.FailedCount();
    return module;
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

    // A yaml pipeline states its own edge depth; the framework has no default.
    int queueCapacity = 0;
    try {
        const YAML::Node root = YAML::LoadFile(configPath);
        const YAML::Node graphNode = root["graph"];
        if (graphNode && graphNode["messageQueueCapacity"]) {
            const int configured = graphNode["messageQueueCapacity"].as<int>();
            if (configured > 0) {
                queueCapacity = configured;
            } else {
                LOG_ERROR("graph.messageQueueCapacity must be positive (got {}); refusing to build a pipeline "
                          "without an edge capacity.",
                          configured);
                return nullptr;
            }
        }
    } catch (const std::exception& e) {
        LOG_ERROR("Could not read graph.messageQueueCapacity from '{}': {}; refusing to build a pipeline without an "
                  "edge capacity.",
                  configPath, e.what());
        return nullptr;
    }

    if (queueCapacity <= 0) {
        LOG_ERROR("'{}' has no graph.messageQueueCapacity; every pipeline states its own edge queue depth.",
                  configPath);
        return nullptr;
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
    // Start() is idempotent: a pipeline that is already running is a no-op. Asking
    // twice used to fail, which forced every caller to track the run state it had
    // just set itself; a restart loop or a guard that already started the pipeline
    // can simply call Start() again.
    if (m_pImpl->started) {
        LOG_DEBUG("Pipeline is already started; Start() is a no-op.");
        return ErrorCode::SUCCESS;
    }
    LOG_DEBUG("Starting pipeline...");
    m_pImpl->drainInProgress.store(false);
    // A Stop() that overran its stop budget leaves the worker thread joinable on
    // purpose (see ModuleActor::StopAndJoinWithin), and that thread may still be
    // inside a blocking push. Reopening the edges first would rewind their
    // indices underneath it and corrupt the ring, so refuse instead: reopening is
    // only safe while no thread is using a queue (SpscQueue::restart).
    for (auto& actorNode : m_pImpl->actorOrderedNodes) {
        if (actorNode->IsStarted()) {
            LOG_ERROR("Cannot start pipeline: module '{}' still owns a worker thread from an earlier stop that "
                      "overran its budget. Wait for that module before starting the pipeline again.",
                      actorNode->GetModuleName());
            return ErrorCode::FAILED_ALREADY_START;
        }
    }
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
    // The stages are torn down producers-first. A stage can only finish what it
    // was given once nothing more can arrive, and that is true exactly when every
    // stage that feeds it has been joined and its output edges closed. So each
    // stage is joined before the stages it feeds are touched, which is also what
    // releases a source parked inside a blocking push.
    //
    // Closing a stage's outputs is what tells the next stage "nothing more will
    // ever arrive", which is what lets its batch pull return instead of idling out
    // its timeout, and what lets it drain the backlog it still holds.
    //
    // The drain budget is per stage and covers that stage's own blocking pull: a
    // stage that is merely between two pulls (a queue reads empty for a moment
    // and the worker is parked in waitAndPopFor) still drains normally on the
    // next iteration. A single shared budget would spend the whole allowance on
    // the first busy stage upstream and then give the downstream ones nothing.
    std::vector<std::string> stopOrder;
    BuildStopOrder(stopOrder);
    // BuildStopOrder produces a valid reverse topological order (sinks first);
    // reversing it gives a valid topological one, producers first.
    std::reverse(stopOrder.begin(), stopOrder.end());
    // Every stage learns about the drain up front: a stage that is waiting for
    // inputs that will never arrive (a fusion module missing one of its sources)
    // has to stop waiting now, not when its turn in the order comes up.
    for (const auto& name : stopOrder) {
        auto actorIt = m_pImpl->actorModuleMap.find(name);
        if (actorIt != m_pImpl->actorModuleMap.end() && actorIt->second) {
            actorIt->second->RequestDrain();
        }
    }

    for (const auto& name : stopOrder) {
        auto actorIt = m_pImpl->actorModuleMap.find(name);
        if (actorIt == m_pImpl->actorModuleMap.end() || !actorIt->second) {
            continue;
        }
        const std::shared_ptr<ModuleActor>& actor = actorIt->second;
        if (!actor->IsStarted()) {
            continue;
        }

        if (actor->IsSource()) {
            // A source never idles on its own: it keeps filling its output until
            // someone stops it, so it has to be told.
            //
            // Closing its output edges is what releases it when it is parked in a
            // blocking push: that push only returns once the queue is non-full or
            // shut down, and a downstream stage is not obliged to consume anything
            // (an idle sink leaves the edge full for good). shutdown() only rejects
            // further pushes and still hands out what is already buffered, so the
            // drain below loses nothing by it.
            actor->RequestStop();
            actor->ShutdownOutputs();
            actor->StopAndJoinWithin(m_pImpl->stopTimeout);
            continue;
        }

        // Every stage that feeds this one has already been joined and had its
        // output edges closed, so this worker leaves its loop by itself once its
        // batch pull comes back empty. Waiting for its queues to empty is what
        // turns the drain into an actual drain instead of a deadline.
        const auto drainBudget = m_pImpl->drainTimeout + actor->BlockingTime();
        if (!actor->WaitForInputsDrained(drainBudget)) {
            LOG_WARN("Module '{}' still had buffered input after {} ms of draining; "
                     "stopping it anyway and dropping the rest.",
                     actor->GetModuleName(), static_cast<long long>(drainBudget.count()));
        }
        actor->RequestStop();
        actor->StopAndJoinWithin(m_pImpl->stopTimeout);
        // The stage can push no more, so the edges it feeds are closed before the
        // next stage is handled: that is what tells the next one "nothing more
        // will ever arrive".
        actor->ShutdownOutputs();
    }

    // Nothing can push any more, so every edge can be closed. A stopped pipeline
    // is reusable: Start() with a closed edge used to fail in the middle of its
    // worker rollback, leaving some workers running.
    //
    // Whatever a stage still could not drain (its budget ran out, or a module
    // ignored the stop request) stays visible as EdgeSnapshot::size, equivalently
    // pushedTotal - poppedTotal. It is deliberately not charged to either module's
    // droppedTotal: the producer already counted it as processed and pushed, and
    // the consumer never popped it, so either charge would count it twice -- see
    // PipelineMetricsTest.ProcessedDroppedAndFailedAccountForEveryMessage and
    // .StopAccountsForMessagesTheDispatcherCouldNotPush.
    for (auto& queue : m_pImpl->queues) {
        queue->shutdown();
    }
    m_pImpl->started = false;
    m_pImpl->drainInProgress.store(false);
    LOG_DEBUG("Pipeline stopped successfully.");
    return ErrorCode::SUCCESS;
}

PipelineSnapshot Pipeline::GetSnapshot() const {
    PipelineSnapshot metrics;
    if (!m_pImpl) {
        return metrics;
    }
    if (m_pImpl->graph) {
        metrics.name = m_pImpl->graph->getName();
    }

    // Modules before their edges: a module records a message as processed only
    // after ProcessBatch() returned, while the message reached its edge from
    // inside that same call. Reading modules first therefore keeps a reader from
    // seeing an edge that already holds a message the module has not counted yet.
    // Pipeline.hpp, Stats.hpp and AGENTS.md §5 all state this order; the loops
    // used to run the other way round while the comment claimed otherwise.
    metrics.modules.reserve(m_pImpl->actorOrderedNodes.size());
    for (const auto& actor : m_pImpl->actorOrderedNodes) {
        metrics.modules.push_back(TakeModuleSnapshot(*actor));
    }

    metrics.edges.reserve(m_pImpl->queues.size());
    for (std::size_t i = 0; i < m_pImpl->queues.size(); ++i) {
        const auto stats = m_pImpl->queues[i]->GetStats();
        EdgeSnapshot edge;
        // The edge name is "from -> to"; the snapshot keeps the two ends apart so
        // a reader can look up either side without splitting a string.
        if (i < m_pImpl->queueNames.size()) {
            const std::string& edgeName = m_pImpl->queueNames[i];
            const std::string::size_type arrow = edgeName.find(" -> ");
            if (arrow != std::string::npos) {
                edge.from = edgeName.substr(0, arrow);
                edge.to = edgeName.substr(arrow + 4);
            } else {
                edge.from = edgeName;
            }
        }
        edge.capacity = stats.capacity;
        edge.size = stats.size;
        edge.maxSize = stats.highWaterMark;
        edge.pushedTotal = stats.pushed;
        edge.poppedTotal = stats.popped;
        metrics.edges.push_back(std::move(edge));
    }

    return metrics;
}

void Pipeline::ResetMetrics() {
    if (!m_pImpl) {
        return;
    }
    for (const auto& queue : m_pImpl->queues) {
        if (queue) {
            queue->resetHighWaterMark();
        }
    }
}

int Pipeline::GetEdgeCapacity() const { return m_pImpl ? m_pImpl->messageQueueCapacity : 0; }

std::shared_ptr<PipelineContext> Pipeline::GetContext() const {
    if (!m_pImpl) {
        return nullptr;
    }
    return m_pImpl->context;
}

}; // namespace nexusflow
