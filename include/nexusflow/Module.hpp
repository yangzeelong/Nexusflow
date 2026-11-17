#ifndef NEXUSFLOW_MODULE_HPP
#define NEXUSFLOW_MODULE_HPP

#include <nexusflow/Config.hpp>
#include <nexusflow/ErrorCode.hpp>
#include <nexusflow/Message.hpp>
#include <nexusflow/TypeTraits.hpp>

#include <memory>
#include <map>
#include <vector>
#include <unordered_map>

// --- Forward Declarations ---
// Forward-declare internal and framework classes to keep this header clean.
namespace nexusflow { namespace dispatcher {
class Dispatcher;
}} // namespace nexusflow::dispatcher

namespace nexusflow {
class Pipeline;
class PipelineContext;
namespace core { class Worker; }
}

namespace nexusflow {

/**
 * @class Module
 * @brief An abstract base class for a processing unit within a data pipeline.
 *
 * A Module focuses exclusively on the business logic of "what to do with data."
 * It passively receives data through its `process` methods and sends results
 * via protected APIs. All threading, data I/O, and lifecycle management are
 * handled прозрачно by the framework.
 */
class Module {
public:
    /**
     * @brief Constructs a Module with a given name.
     * @param name A unique identifier for this module instance.
     */
    explicit Module(std::string name);

    /**
     * @brief Virtual destructor to ensure proper cleanup of derived classes.
     */
    virtual ~Module();

    // Modules are unique components and should not be copied or moved.
    Module(const Module&) = delete;
    Module& operator=(const Module&) = delete;

    // --- Lifecycle ---
    virtual ErrorCode Configure(const Config& config);

    /**
     * @brief User-defined initialization logic.
     * Called by the framework once before the pipeline starts.
     * @return An ErrorCode indicating success or failure.
     */
    virtual ErrorCode Init();

    /**
     * @brief User-defined resource cleanup logic.
     * Called by the framework once after the pipeline has stopped.
     * @return An ErrorCode indicating success or failure.
     */
    virtual ErrorCode DeInit();

    /**
     * @brief Drops outputs that a previous run queued but never dispatched.
     * @details The framework calls this before it starts a module again. Outputs
     * queued during Process() are handed over only when it returns; a run that
     * was interrupted leaves them behind, and delivering them in the middle of
     * the next run would emit a message to a pipeline that never asked for it.
     */
    void DiscardPendingOutputs();

    // --- Processing ---

    /**
     * @brief The core processing logic for a single message.
     * @note Derived classes MUST implement this method.
     * @param inputMessage The message to be processed, passed in by the framework.
     */
    virtual void Process(Message& inputMessage) = 0;

    /**
     * @brief The core processing logic for a batch of messages.
     * The framework calls this method by default. The base implementation
     * simply iterates through the batch and calls `process()` for each message.
     * Override this for more efficient batch-oriented processing.
     * @param inputBatchMessages A batch of messages to be processed.
     */
    virtual void ProcessBatch(std::vector<Message>& inputBatchMessages);

    /**
     * @brief Framework entry point that measures one module invocation.
     *
     * Workers must call this method instead of Process() directly. Outputs
     * emitted during the invocation are held until Process() returns, then
     * receive this module's elapsed time in MessageMeta::GetModuleTimings().
     * @return true if the invocation emitted at least one output message.
     */
    bool ProcessTimed(Message& inputMessage);

    // --- Getter and Setter ---

    /**
     * @brief Gets the unique name of the module.
     * @return A const reference to the module's name.
     */
    const std::string& GetModuleName() const;

    /**
     * @brief Attaches the pipeline context this module belongs to.
     * Called by the framework before Init(); a module normally only reads it.
     * Passing nullptr detaches the module (used by standalone tests).
     */
    void AttachContext(std::shared_ptr<PipelineContext> context);

    /**
     * @brief Enables or disables the per-hop timing chain for this module.
     *
     * When enabled, every message this module dispatches carries the accumulated
     * per-stage elapsed times (`MessageMeta::GetModuleTimings()`), and the worker
     * copies the upstream map on every hop. That copy is not free: measured on a
     * 3-stage linear pipeline it costs roughly 2x the end-to-end throughput, so
     * the chain is off unless the pipeline asks for it
     * (`PipelineContext::EnableModuleTimings()`) or a single module opts in here.
     * This setter is the per-module override for a pipeline-wide switch.
     */
    void SetModuleTimingsEnabled(bool enabled);

    /**
     * @brief Returns whether this module records and propagates per-stage timings.
     */
    bool ModuleTimingsEnabled() const;

    /**
     * @brief True when per-hop timings should be recorded for this module.
     * @details Either the owning pipeline's context asked for them
     * (PipelineContext::EnableModuleTimings) or this single module opted in via
     * SetModuleTimingsEnabled(). Off by default in both places, because the chain
     * deep-copies a map on every hop.
     */
    bool CapturesModuleTimings() const;

    /**
     * @brief Returns the context attached by the framework, or nullptr when the
     * module does not belong to a pipeline (e.g. a unit test constructing it).
     */
    std::shared_ptr<PipelineContext> GetContext() const;

protected:
    // --- Protected API for Derived Classes ---

    /**
     * @brief Broadcasts a message to all connected downstream outputs.
     * @param msg The message to be sent.
     */
    void Broadcast(const Message& msg);

    /**
     * @brief Broadcasts a message, moving it into the last downstream queue.
     * @details Equivalent to Broadcast(const Message&) but avoids one copy for
     * the last subscriber. After the call the message is in a moved-from state.
     * @param msg The message to be sent.
     */
    void Broadcast(Message&& msg);

    /**
     * @brief Sends a message to a specific downstream output.
     * @param outputName The name of the output port to send the message to.
     * @param msg The message to be sent.
     * @return true when the message was handed to the output port (or queued for
     * dispatch); false when the port is unknown or its queue is shut down.
     */
    bool SendTo(const std::string& outputName, const Message& msg);

    /**
     * @brief Marks this invocation as "I waited, and nothing came out".
     * @details A source module that owns its own wait — a decoder blocking on the
     * next frame, a reader blocking on a socket — calls this when that wait ended
     * without work. The worker then skips its idle backoff once, because the wait
     * the module just performed is the backoff. Without this the pipeline would
     * wait twice per empty turn, and a source that blocks until the pipeline
     * stops would spend the whole timeout in the backoff.
     *
     * Has no effect for a module that does not look at its input: every
     * non-source module pulls through the worker, and that pull already says it.
     */
    void NoteBlockedWait();

private:
    friend class ModuleActor;
    friend class core::Worker;

    struct PendingOutput {
        bool named = false;
        std::string outputName;
        Message message;
    };

    void DispatchOutput(const PendingOutput& output, double elapsedMs);

    // A private setter for the internal handle, callable only by the Pipeline.
    void SetDispatcher(const std::shared_ptr<dispatcher::Dispatcher>& dispatcher);

    std::string m_moduleName;

    // Set by ModuleActor from the owning pipeline; null for standalone modules.
    std::shared_ptr<PipelineContext> m_context;
    /// Raw view of m_context for the hot path: CapturesModuleTimings() must not
    /// pay a shared_ptr refcount round trip per message.
    PipelineContext* m_contextRaw = nullptr;

    // The internal dispatcher handle.
    std::shared_ptr<dispatcher::Dispatcher> m_dispatcherPtr;

    // Set by the worker for this module; see NoteBlockedWait().
    core::Worker* m_worker = nullptr;

    bool m_processing = false;
    bool m_moduleTimingsEnabled = false;  ///< Off by default: see SetModuleTimingsEnabled().
    std::map<std::string, double> m_activeInputTimingMs;
    std::vector<PendingOutput> m_pendingOutputs;
};

} // namespace nexusflow

#endif // NEXUSFLOW_MODULE_HPP
