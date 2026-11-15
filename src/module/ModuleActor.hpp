#ifndef NEXUSFLOW_MODULE_ACTOR_HPP
#define NEXUSFLOW_MODULE_ACTOR_HPP

#include "core/Worker.hpp"
#include "dispatcher/Dispatcher.hpp"
#include "nexusflow/ErrorCode.hpp"
#include <nexusflow/Module.hpp>
#include <chrono>
#include <cstdint>
#include <thread>

namespace nexusflow {

class ModuleActor {
public:
    ModuleActor(const std::shared_ptr<Module>& module, const Config& config,
                const std::shared_ptr<PipelineContext>& context = nullptr);

    ~ModuleActor();

    void AddInputQueue(const std::string& name, ViewPtr<MessageQueue> queue) { m_worker->AddQueue(name, queue); }

    void AddOutputQueue(const std::string& name, ViewPtr<MessageQueue> queue) { m_dispatcher->AddSubscriber(name, queue); }

    /**
     * @brief Points the dispatcher at the pipeline's shutdown flag, see
     *        Dispatcher::SetShuttingDown().
     */
    void SetShutdownFlag(std::atomic<bool>* flag) {
        if (m_dispatcher) {
            m_dispatcher->SetShuttingDown(flag);
        }
    }

    std::shared_ptr<Module>& GetModule() { return m_module; };

    std::string GetModuleName() const { return m_module->GetModuleName(); }

    std::uint64_t ProcessedCount() const { return m_worker ? m_worker->ProcessedCount() : 0; }

    /// Messages this module lost: its dispatcher could not push them downstream
    /// plus whatever the worker itself dropped (e.g. fusion timeouts).
    std::uint64_t DroppedCount() const {
        const std::uint64_t dispatcherDrops = m_dispatcher ? m_dispatcher->DroppedCount() : 0;
        const std::uint64_t workerDrops = m_worker ? m_worker->DroppedCount() : 0;
        return dispatcherDrops + workerDrops;
    }

    ErrorCode Init();

    ErrorCode DeInit();

    ErrorCode Start();

    /**
     * @brief Requests the stop and joins the worker.
     */
    ErrorCode Stop();

    /**
     * @brief Requests the stop and gives the worker a bounded time to leave its
     *        loop, then joins it.
     * @details Pipeline::Stop() uses this during the graceful drain. Joining
     * unconditionally wedges the whole pipeline when a module parks in a call it
     * cannot leave quickly (a blocking read, a fusion wait), because that one
     * stage then stalls every stage around it along with the caller.
     * @param timeout Upper bound to wait for the loop to return.
     * @return true when the loop left on its own, false when the join had to wait
     *         out the timeout and it had to be abandoned.
     */
    bool StopAndJoinWithin(std::chrono::milliseconds timeout);

    /**
     * @brief Tells the worker loop to return, without joining it yet.
     */
    void RequestStop() {
        if (m_worker) {
            m_worker->RequestStop();
        }
    }

    /**
     * @brief Closes the edges this module feeds, see Dispatcher::ShutdownOutputs().
     */
    void ShutdownOutputs() {
        if (m_dispatcher) {
            m_dispatcher->ShutdownOutputs();
        }
    }

    /**
     * @brief Undoes RequestStop(), so the actor can be started again.
     */
    void ClearStop() {
        if (m_worker) {
            m_worker->PrepareForStart();
        }
    }

    /**
     * @brief Whether this actor's worker was started (and is therefore joinable).
     */
    bool IsStarted() const { return m_workThread.joinable(); }

    /**
     * @brief Whether the worker of a previous run is still executing.
     * @details A joined worker reports false. This stays true after
     * StopAndJoinWithin() gave up on a module, which is exactly the state in
     * which the module must not be started a second time.
     */
    bool IsWorkerRunning() const { return m_worker && m_worker->IsRunning(); }

    /**
     * @brief Whether this actor's worker drives the loop of a source module.
     * @details A source has no input queue and only produces work, so the stop
     * path has to join it before it drains anything downstream.
     */
    bool IsSource() const { return m_worker && m_worker->IsSource(); }

    /**
     * @brief Lets the worker know that the pipeline is stopping, so it can give
     *        up on inputs it would otherwise keep waiting for.
     */
    void RequestDrain() {
        if (m_worker) {
            m_worker->RequestDrain();
        }
    }

    /**
     * @brief Undoes RequestDrain(), so service can resume.
     */
    void ClearDrain() {
        if (m_worker) {
            m_worker->ClearDrain();
        }
    }

    /**
     * @brief Waits until every input queue of this module is empty.
     * @details Used by Pipeline::Stop() for the graceful drain: once the
     * producers upstream have stopped, an empty input queue means this module
     * has taken everything it was going to take. Returns immediately for a
     * module without inputs (a source has nothing to drain).
     * @param timeout Upper bound for the whole wait, shared across the queues.
     * @return true when every input queue emptied in time.
     */
    bool WaitForInputsDrained(std::chrono::milliseconds timeout) {
        if (!m_worker) {
            return true;
        }
        return m_worker->WaitForInputsDrained(timeout);
    }

    /**
     * @brief Returns the edges this module writes to.
     * @details Pipeline::Stop() closes them once the module has been joined, so
     * a downstream stage can tell that nothing more is coming.
     */
    std::vector<ViewPtr<MessageQueue>> GetOutputQueues() const {
        if (!m_dispatcher) {
            return std::vector<ViewPtr<MessageQueue>>();
        }
        return m_dispatcher->GetOutputQueues();
    }

    /**
     * @brief How long this module's worker can sit in a single blocking pull.
     * @details The drain in Pipeline::Stop() has to outlast this, otherwise a
     * stage that is merely between two pulls looks like it is still holding
     * work that will never be processed.
     */
    std::chrono::milliseconds BlockingTime() const {
        if (!m_worker) {
            return std::chrono::milliseconds(0);
        }
        if (m_syncInputs) {
            return m_worker->FusionTimeout();
        }
        if (m_worker->IsSource()) {
            return m_worker->SourceIdleTime();
        }
        return m_worker->BatchTimeout();
    }

private:
    std::shared_ptr<core::Worker>& GetWorker() { return m_worker; }
    std::shared_ptr<dispatcher::Dispatcher>& GetDispatcher() { return m_dispatcher; }

    bool m_syncInputs = false;
    /// Set when the drain gave up on this worker; it still runs, untouched.
    bool m_stopOverdue = false;

    std::shared_ptr<Module> m_module;
    std::shared_ptr<core::Worker> m_worker;
    std::shared_ptr<dispatcher::Dispatcher> m_dispatcher;
    std::unique_ptr<Config> m_config;

    std::thread m_workThread;
};
} // namespace nexusflow

#endif