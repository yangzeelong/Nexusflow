#ifndef NEXUSFLOW_PIPELINE_PIPELINEIMPL_HPP
#define NEXUSFLOW_PIPELINE_PIPELINEIMPL_HPP

#include "base/Define.hpp"
#include "base/Graph.hpp"
#include "core/Worker.hpp"
#include "dispatcher/Dispatcher.hpp"
#include "module/ModuleActor.hpp"
#include <nexusflow/Pipeline.hpp>
#include <nexusflow/PipelineContext.hpp>
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

    // Edge queue configuration and names, used by Pipeline::GetSnapshot().
    // Required: the builder (Build(capacity)) or the yaml config states it
    // before Init() runs, so 0 here means a caller skipped InitWithGraph().
    int messageQueueCapacity = 0;
    std::vector<std::string> queueNames;

    // Set once Stop()/Cancel() starts. The teardown hands work downstream first,
    // so a source still inside a Process() call can run into a closed edge; that
    // is expected loss during shutdown and the dispatcher reports it through this
    // flag instead of warning again.
    std::atomic<bool> drainInProgress{false};

    // Wall time of the most recent Stop()/Cancel() in milliseconds, published by
    // Pipeline::Stop()/Cancel() and cleared by Pipeline::Start() (see
    // PipelineSnapshot::stopPipelineMs).
    std::atomic<std::uint64_t> lastStopPipelineMs{0};

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
