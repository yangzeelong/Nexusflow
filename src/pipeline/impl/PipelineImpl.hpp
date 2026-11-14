#ifndef NEXUSFLOW_PIPELINE_PIPELINEIMPL_HPP
#define NEXUSFLOW_PIPELINE_PIPELINEIMPL_HPP

#include "base/Define.hpp"
#include "base/Graph.hpp"
#include "core/Worker.hpp"
#include "dispatcher/Dispatcher.hpp"
#include "module/ModuleActor.hpp"
#include <nexusflow/Pipeline.hpp>
#include <string>
#include <vector>

namespace nexusflow {

// Forward declarations
class Pipeline;

// An internal struct to group all runtime components related to a single module.
using ActorName = std::string;
using ActorNode = ModuleActor;

// Default capacity of the message queue created for every graph edge. It bounds
// the number of in-flight messages between two stages, i.e. the backpressure
// depth of the pipeline.
constexpr int kDefaultMessageQueueCapacity = 5;

// --- Pipeline's Private Implementation (m_pImpl) ---
class Pipeline::Impl {
public:
    std::unique_ptr<Graph> graph;
    std::vector<MessageQueueUPtr> queues;

    // Actors in graph order (producers before consumers). Pipeline::Init and
    // Pipeline::Start walk this forward, DeInit walks it backward.
    std::vector<std::shared_ptr<ActorNode>> actorOrderedNodes;

    // Lifecycle bookkeeping so Pipeline can be defensive instead of relying on
    // the caller to hit Init/Start/Stop/DeInit in exactly the right order.
    bool graphReady = false;
    bool initialized = false;
    bool started = false;

    ErrorCode Init();

private:
    std::shared_ptr<ActorNode> GetOrCreateActorNode(const std::shared_ptr</*Graph::*/ Node>& node);

    std::unordered_map<ActorName, std::shared_ptr<ActorNode>> actorModuleMap;
};

} // namespace nexusflow

#endif // NEXUSFLOW_PIPELINE_PIPELINEIMPL_HPP
