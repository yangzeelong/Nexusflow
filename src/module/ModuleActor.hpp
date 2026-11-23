#ifndef NEXUSFLOW_MODULE_ACTOR_HPP
#define NEXUSFLOW_MODULE_ACTOR_HPP

#include "core/ThreadPool.hpp"
#include "core/Worker.hpp"
#include "dispatcher/Dispatcher.hpp"
#include "nexusflow/ErrorCode.hpp"
#include <nexusflow/Module.hpp>
#include <atomic>
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
    /// plus whatever the worker itself dropped (e.g. join timeouts).
    std::uint64_t DroppedCount() const {
        const std::uint64_t dispatcherDrops = m_dispatcher ? m_dispatcher->DroppedCount() : 0;
        // Work discarded by the drop policy never reached a queue, but it is
        // lost work all the same and belongs in the same total.
        const std::uint64_t policyDrops = m_dispatcher ? m_dispatcher->PolicyDroppedCount() : 0;
        const std::uint64_t workerDrops = m_worker ? m_worker->DroppedCount() : 0;
        return dispatcherDrops + policyDrops + workerDrops;
    }

    /// Subset of DroppedCount() that expired in the join group instead of
    /// being refused by a queue.
    std::uint64_t JoinExpiredCount() const { return m_worker ? m_worker->JoinExpiredCount() : 0; }

    /// Invocations that threw. processed + dropped + failed accounts for
    /// everything the module was handed.
    std::uint64_t FailedCount() const { return m_worker ? m_worker->FailedCount() : 0; }

    ErrorCode Init();

    ErrorCode DeInit();

    ErrorCode Start();

    /**
     * @brief Requests the stop and joins the worker.
     */
    ErrorCode Stop();

    /**
     * @brief Hands the next Start() to a shared pool instead of a private
     *        thread. Pass nullptr to go back to one thread per module.
     * @details The pool must already be attached when Start() runs: it is
     * told about the worker there, and it is started by the pipeline once
     * every actor has been started. Join workers ignore the pool.
     */
    void AttachThreadPool(core::ThreadPool* pool) { m_pool = pool; }

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
    bool IsStarted() const { return m_workThread.joinable() || m_poolStarted; }

    /**
     * @brief Whether the worker of a previous run is still executing.
     * @details A joined worker reports false, which is the state
     * Pipeline::Start() requires before it reopens the edges.
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
     *
     * There is no deadline on purpose: the producers are already joined, so the
     * only thing between this call and an empty queue is this module's own
     * consumer loop, which Stop() has not stopped yet.
     */
    void WaitForInputsDrained() {
        if (m_worker) {
            m_worker->WaitForInputsDrained();
        }
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

    /// Milliseconds the most recent stop spent waiting for this module's inputs to
    /// empty. Written by the thread that ran Stop()/Cancel() and read by whatever
    /// thread takes a snapshot, so both ends use a relaxed atomic.
    std::uint64_t LastDrainInputsMs() const { return m_lastDrainInputsMs.load(std::memory_order_relaxed); }

    /// Milliseconds the most recent stop spent waiting for this module's worker
    /// loop to return once it had been asked to.
    std::uint64_t LastExitLoopMs() const { return m_lastExitLoopMs.load(std::memory_order_relaxed); }

    /// @brief Publishes how long this module's part of the stop took.
    /// @details Called by Pipeline::Stop()/Cancel() after handing out the weights:
    /// the drain wait is zero for a source and for Cancel().
    void RecordStopTiming(std::uint64_t drainInputsMs, std::uint64_t exitLoopMs) {
        m_lastDrainInputsMs.store(drainInputsMs, std::memory_order_relaxed);
        m_lastExitLoopMs.store(exitLoopMs, std::memory_order_relaxed);
    }

    /// @brief Forgets the previous stop's timing, so a snapshot taken during the
    /// next run does not report how the previous one ended.
    void ResetStopTiming() {
        m_lastDrainInputsMs.store(0, std::memory_order_relaxed);
        m_lastExitLoopMs.store(0, std::memory_order_relaxed);
    }

private:
    std::shared_ptr<core::Worker>& GetWorker() { return m_worker; }
    std::shared_ptr<dispatcher::Dispatcher>& GetDispatcher() { return m_dispatcher; }

    std::shared_ptr<Module> m_module;
    std::shared_ptr<core::Worker> m_worker;
    std::shared_ptr<dispatcher::Dispatcher> m_dispatcher;
    std::unique_ptr<Config> m_config;

    std::thread m_workThread;
    /// Non-null while this actor's worker is hosted by a shared pool.
    core::ThreadPool* m_pool = nullptr;
    /// True between Start() and Stop() while the worker runs on that pool.
    bool m_poolStarted = false;

    std::atomic<std::uint64_t> m_lastDrainInputsMs{0};
    std::atomic<std::uint64_t> m_lastExitLoopMs{0};
};
} // namespace nexusflow

#endif