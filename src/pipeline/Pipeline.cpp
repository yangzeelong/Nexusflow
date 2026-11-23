
#include "base/Define.hpp"
#include "base/Graph.hpp"
#include "base/GraphUtils.hpp"
#include <nexusflow/common/ViewPtr.hpp>
#include "core/ThreadPool.hpp"
#include "core/Worker.hpp"
#include "dispatcher/Dispatcher.hpp"
#include "nexusflow/Logging.hpp"
#include <nexusflow/ModuleFactory.hpp>

#include "impl/PipelineImpl.hpp"
#include <yaml-cpp/yaml.h>
#include <algorithm>
#include <chrono>
#include <memory>
#include <nexusflow/ErrorCode.hpp>
#include <nexusflow/Module.hpp>
#include <nexusflow/PipelineContext.hpp>
#include <nexusflow/Pipeline.hpp>
#include <string>
#include <thread>
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
    module.joinExpiredTotal = actor.JoinExpiredCount();
    module.failedTotal = actor.FailedCount();
    module.drainInputsMs = actor.LastDrainInputsMs();
    module.exitLoopMs = actor.LastExitLoopMs();
    return module;
}

// Milliseconds between two points of the stop. Stop() is the only caller, and it
// only ever feeds strictly increasing readings, so the result cannot be negative.
std::uint64_t ElapsedMs(std::chrono::steady_clock::time_point from, std::chrono::steady_clock::time_point to) {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(to - from).count());
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
    // Reopening an edge rewinds its indices, so it is only safe while no thread is
    // using the queue (SpscQueue::restart). Stop()/Cancel() always join every
    // worker, so this only trips when a caller started a module actor behind
    // Pipeline's back.
    for (auto& actorNode : m_pImpl->actorOrderedNodes) {
        if (actorNode->IsStarted()) {
            LOG_ERROR("Cannot start pipeline: module '{}' still owns a worker thread from an earlier run. "
                      "Stop the pipeline before starting it again.",
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

    // What the previous stop measured describes the previous run, so a snapshot
    // taken from here on reports this one (all zeros until it stops again).
    m_pImpl->lastStopPipelineMs.store(0, std::memory_order_relaxed);
    for (auto& actorNode : m_pImpl->actorOrderedNodes) {
        actorNode->ResetStopTiming();
    }

    // An opt-in shared pool: 0 keeps the historical one-thread-per-module model.
    const std::size_t poolThreads = m_pImpl->context ? m_pImpl->context->GetThreadPoolSize() : 0;
    if (poolThreads > 0) {
        const unsigned usableCpus = std::thread::hardware_concurrency();
        if (usableCpus > 0 && poolThreads > usableCpus) {
            // The pool spins before parking, so more worker threads than CPUs is not a mild
            // slowdown: on 2 CPUs a pool of 8 measured 39 K msg/s against 2 M for a pool of 8
            // on 16 CPUs. Warn once at start; sizing stays the caller's decision.
            LOG_WARN("Thread pool has {} threads but this machine reports {} logical CPUs; "
                     "the pool spins before parking, so oversubscribing it is far slower than "
                     "sizing it to the core count.",
                     static_cast<unsigned long long>(poolThreads), static_cast<unsigned long long>(usableCpus));
        }
        m_pImpl->threadPool =
            std::make_shared<core::ThreadPool>(poolThreads, m_pImpl->context->GetThreadPoolMapping());
    }

    std::vector<std::shared_ptr<ActorNode>> startedActors;
    for (auto& actorNode : m_pImpl->actorOrderedNodes) {
        if (m_pImpl->threadPool) {
            actorNode->AttachThreadPool(m_pImpl->threadPool.get());
        }
        const ErrorCode errCode = actorNode->Start();
        if (errCode != ErrorCode::SUCCESS) {
            LOG_ERROR("Start worker failed, actorName={}", actorNode->GetModuleName());
            // Roll back the workers that already started.
            for (auto it = startedActors.rbegin(); it != startedActors.rend(); ++it) {
                (*it)->Stop();
            }
            if (m_pImpl->threadPool) {
                m_pImpl->threadPool->StopAndJoin();
                m_pImpl->threadPool.reset();
            }
            return errCode;
        }
        startedActors.push_back(actorNode);
        LOG_DEBUG("Start module success, actorName={}", actorNode->GetModuleName());
    }
    // Every worker is registered and prepared, so the shared threads can
    // start stepping them.
    if (m_pImpl->threadPool) {
        m_pImpl->threadPool->Start();
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
    // Timed from here so a caller can ask afterwards where the stop went: the
    // per-stage split lands in ModuleSnapshot::drainInputsMs/exitLoopMs, the total in
    // PipelineSnapshot::stopPipelineMs.
    const auto stopStart = std::chrono::steady_clock::now();
    std::uint64_t drainInputsTotalMs = 0;
    std::uint64_t slowestDrainInputsMs = 0;
    std::string slowestDrainStage;
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
    // There is no drain budget: with the producers already joined, an empty input
    // edge is a state the stage reaches by itself, and the wait for it ends as
    // soon as it does. A budget only decided *when* to give up on a stage that is
    // slow rather than stuck, and giving up meant leaving work behind that a
    // later Start() would never deliver.
    std::vector<std::string> stopOrder;
    BuildStopOrder(stopOrder);
    // BuildStopOrder produces a valid reverse topological order (sinks first);
    // reversing it gives a valid topological one, producers first.
    std::reverse(stopOrder.begin(), stopOrder.end());
    // Every stage learns about the drain up front: a stage that is waiting for
    // inputs that will never arrive (a join module missing one of its sources)
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
            const auto joinStart = std::chrono::steady_clock::now();
            actor->Stop();
            // A source has nothing to drain, so its drain weight is always zero.
            actor->RecordStopTiming(0, ElapsedMs(joinStart, std::chrono::steady_clock::now()));
            continue;
        }

        // Every stage that feeds this one has already been joined and had its
        // output edges closed, so this worker leaves its loop by itself once its
        // batch pull comes back empty. Waiting for its queues to empty is what
        // turns the drain into an actual drain instead of a deadline.
        const auto drainStart = std::chrono::steady_clock::now();
        actor->WaitForInputsDrained();
        const auto drainEnd = std::chrono::steady_clock::now();
        actor->RequestStop();
        actor->Stop();
        const auto joinEnd = std::chrono::steady_clock::now();

        const std::uint64_t drainInputsMs = ElapsedMs(drainStart, drainEnd);
        actor->RecordStopTiming(drainInputsMs, ElapsedMs(drainEnd, joinEnd));
        drainInputsTotalMs += drainInputsMs;
        if (drainInputsMs > slowestDrainInputsMs) {
            slowestDrainInputsMs = drainInputsMs;
            slowestDrainStage = name;
        }
        // The stage can push no more, so the edges it feeds are closed before the
        // next stage is handled: that is what tells the next one "nothing more
        // will ever arrive".
        actor->ShutdownOutputs();
    }

    // The shared threads have nothing left to step: every worker already
    // published its exit, so joining them here is just teardown.
    if (m_pImpl->threadPool) {
        m_pImpl->threadPool->StopAndJoin();
        m_pImpl->threadPool.reset();
        for (auto& actorNode : m_pImpl->actorOrderedNodes) {
            if (actorNode) {
                actorNode->AttachThreadPool(nullptr);
            }
        }
    }

    // Nothing can push any more, so every edge can be closed. A stopped pipeline
    // is reusable: Start() with a closed edge used to fail in the middle of its
    // worker rollback, leaving some workers running.
    //
    // Stop() leaves every edge empty by construction, so EdgeSnapshot::size is 0
    // and discardedTotal does not move. Anything a producer could not hand over
    // was refused *inside* the module and is already in its droppedTotal -- see
    // PipelineMetricsTest.ProcessedDroppedAndFailedAccountForEveryMessage and
    // .StopAccountsForMessagesTheDispatcherCouldNotPush.
    for (auto& queue : m_pImpl->queues) {
        queue->shutdown();
    }
    m_pImpl->started = false;
    m_pImpl->drainInProgress.store(false);
    const std::uint64_t stopPipelineMs = ElapsedMs(stopStart, std::chrono::steady_clock::now());
    m_pImpl->lastStopPipelineMs.store(stopPipelineMs, std::memory_order_relaxed);
    if (slowestDrainInputsMs > 0) {
        LOG_DEBUG("Pipeline drained in {} ms of waiting, slowest stage '{}' at {} ms; whole stop took {} ms.",
                  drainInputsTotalMs, slowestDrainStage, slowestDrainInputsMs, stopPipelineMs);
    } else {
        LOG_DEBUG("Pipeline stopped successfully, nothing left to drain; whole stop took {} ms.", stopPipelineMs);
    }
    return ErrorCode::SUCCESS;
}

ErrorCode Pipeline::Cancel() {
    if (!m_pImpl) {
        return ErrorCode::SUCCESS; // Nothing to cancel.
    }
    if (!m_pImpl->started) {
        LOG_DEBUG("Pipeline is already stopped.");
        return ErrorCode::FAILED_ALREADY_STOP;
    }

    LOG_DEBUG("Cancelling pipeline...");
    // Timed like Stop(): the per-stage weight is join only, because Cancel() never
    // drains (ModuleSnapshot::drainInputsMs stays zero).
    const auto cancelStart = std::chrono::steady_clock::now();
    // Same as Stop(): pushes into an edge the cancel already closed are expected
    // shutdown loss, not a warning per message.
    m_pImpl->drainInProgress.store(true);
    if (m_pImpl->context) {
        m_pImpl->context->RequestStop();
    }

    // Cancel() has no order to respect, so every stage is told at once and every
    // edge is closed before anything is joined. Closing is what releases a worker
    // parked in a blocking push (shutdown() refuses new pushes but still hands out
    // what is buffered, so a consumer parked in a pull is released too), which is
    // what makes the joins below return instead of waiting for a backlog nobody
    // is going to take.
    for (auto& actorNode : m_pImpl->actorOrderedNodes) {
        if (actorNode && actorNode->IsStarted()) {
            actorNode->RequestStop();
        }
    }
    for (auto& queue : m_pImpl->queues) {
        queue->shutdown();
    }
    for (auto& actorNode : m_pImpl->actorOrderedNodes) {
        if (actorNode && actorNode->IsStarted()) {
            const auto joinStart = std::chrono::steady_clock::now();
            actorNode->Stop();
            actorNode->RecordStopTiming(0, ElapsedMs(joinStart, std::chrono::steady_clock::now()));
        }
    }

    if (m_pImpl->threadPool) {
        m_pImpl->threadPool->StopAndJoin();
        m_pImpl->threadPool.reset();
        for (auto& actorNode : m_pImpl->actorOrderedNodes) {
            if (actorNode) {
                actorNode->AttachThreadPool(nullptr);
            }
        }
    }

    // Every worker is gone, so nothing races these: what a stage managed to take
    // out of an edge before it stopped is a pop, and whatever is left is discarded
    // here. The two are counted apart on purpose (EdgeSnapshot::discardedTotal vs
    // poppedTotal) so the accounting stays exact across Cancel() and Start().
    for (auto& queue : m_pImpl->queues) {
        queue->clear();
    }
    m_pImpl->started = false;
    m_pImpl->drainInProgress.store(false);
    const std::uint64_t cancelPipelineMs = ElapsedMs(cancelStart, std::chrono::steady_clock::now());
    m_pImpl->lastStopPipelineMs.store(cancelPipelineMs, std::memory_order_relaxed);
    LOG_DEBUG("Pipeline cancelled in {} ms; nothing was drained.", cancelPipelineMs);
    return ErrorCode::SUCCESS;
}

ThreadPoolSnapshot Pipeline::GetThreadPoolStats() const {
    ThreadPoolSnapshot snapshot;
    if (m_pImpl && m_pImpl->threadPool) {
        const core::ThreadPoolStats stats = m_pImpl->threadPool->Snapshot();
        snapshot.rounds = stats.rounds;
        snapshot.turns = stats.turns;
        snapshot.blockedRounds = stats.blockedRounds;
        snapshot.idleRounds = stats.idleRounds;
    }
    return snapshot;
}

PipelineSnapshot Pipeline::GetSnapshot() const {
    PipelineSnapshot metrics;
    if (!m_pImpl) {
        return metrics;
    }
    if (m_pImpl->graph) {
        metrics.name = m_pImpl->graph->getName();
    }
    metrics.stopPipelineMs = m_pImpl->lastStopPipelineMs.load(std::memory_order_relaxed);

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
        edge.discardedTotal = stats.discardedTotal;
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

PushResult Pipeline::Push(const std::string& sourceName, const Message& msg) {
    return m_pImpl ? m_pImpl->PushToSource(sourceName, msg, true) : PushResult::kNotRunning;
}

PushResult Pipeline::TryPush(const std::string& sourceName, const Message& msg) {
    return m_pImpl ? m_pImpl->PushToSource(sourceName, msg, false) : PushResult::kNotRunning;
}

std::shared_ptr<PipelineContext> Pipeline::GetContext() const {
    if (!m_pImpl) {
        return nullptr;
    }
    return m_pImpl->context;
}

}; // namespace nexusflow
