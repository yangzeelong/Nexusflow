#ifndef NEXUSFLOW_PIPELINE_PIPELINEIMPL_HPP
#define NEXUSFLOW_PIPELINE_PIPELINEIMPL_HPP

#include "base/Define.hpp"
#include "base/Graph.hpp"
#include "core/Worker.hpp"
#include "dispatcher/Dispatcher.hpp"
#include "module/ModuleActor.hpp"
#include <nexusflow/Pipeline.hpp>
#include <nexusflow/PipelineContext.hpp>
#include <chrono>
#include <atomic>
#include <string>
#include <vector>

namespace nexusflow {

// Forward declarations
class Pipeline;

// An internal struct to group all runtime components related to a single module.
using ActorName = std::string;
using ActorNode = ModuleActor;

// --- Pipeline's Private Implementation (m_pImpl) ---
class Pipeline::Impl {
public:
    std::unique_ptr<Graph> graph;

    // Shared services handed to every module of this pipeline.
    std::shared_ptr<PipelineContext> context;
    std::vector<MessageQueueUPtr> queues;

    // Edge queue configuration and names, used by Pipeline::GetMetrics().
    int messageQueueCapacity = kDefaultMessageQueueCapacity;
    std::vector<std::string> queueNames;

    // Budget for the graceful drain in Pipeline::Stop(): how long an edge may
    // still hold messages after every stage was asked to stop.
    std::chrono::milliseconds drainTimeout{5000};

    // Upper bound for one stage to leave its worker loop once its input is
    // drained. A module that overruns this is abandoned instead of blocking the
    // whole shutdown.
    std::chrono::milliseconds stopTimeout{2000};

    // Set once Stop() has finished its drain. The drain hands work downstream
    // first, so a source still inside a Process() call can run into a closed
    // edge; that is expected loss during shutdown and the dispatcher reports it
    // through this flag instead of warning again.
    std::atomic<bool> drainInProgress{false};

    // Actors in graph order (producers before consumers). Pipeline::Init and
    // Pipeline::Start walk this forward, DeInit walks it backward.
    std::vector<std::shared_ptr<ActorNode>> actorOrderedNodes;

    // Lifecycle bookkeeping so Pipeline can be defensive instead of relying on
    // the caller to hit Init/Start/Stop/DeInit in exactly the right order.
    bool graphReady = false;
    bool initialized = false;
    bool started = false;

    ErrorCode Init();

    std::unordered_map<ActorName, std::shared_ptr<ActorNode>> actorModuleMap;

private:
    std::shared_ptr<ActorNode> GetOrCreateActorNode(const std::shared_ptr</*Graph::*/ Node>& node);
};

} // namespace nexusflow

#endif // NEXUSFLOW_PIPELINE_PIPELINEIMPL_HPP
