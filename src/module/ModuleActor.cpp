#include "ModuleActor.hpp"
#include <nexusflow/PipelineContext.hpp>
#include "nexusflow/ErrorCode.hpp"
#include <memory>

namespace nexusflow {

ModuleActor::ModuleActor(const std::shared_ptr<Module>& module, const Config& config,
                         const std::shared_ptr<PipelineContext>& context) {
    m_config = std::make_unique<Config>(config);

    ViewPtr<Config> configView{m_config.get()};

    m_module = module;
    m_module->AttachContext(context);
    m_worker = std::make_shared<core::Worker>(m_module, configView, context);

    ViewPtr<Module> moduleView{m_module.get()};
    m_dispatcher = std::make_shared<dispatcher::Dispatcher>(configView);

    m_module->SetDispatcher(m_dispatcher);
}

ModuleActor::~ModuleActor() {
    // RAII safety net: ModuleActor owns a joinable std::thread. Destroying it
    // without Stop() would call std::terminate from ~std::thread, which is
    // exactly what happens when a Pipeline is dropped while running.
    //
    // The destructor waits rather than detaching: a detached worker would keep
    // touching this module (and its queues) after the owner is gone, and a hang
    // here is attributable to the module that caused it rather than looking like
    // a framework deadlock.
    if (m_workThread.joinable()) {
        LOG_WARN("ModuleActor '{}' destroyed while its worker thread was still running; stopping it.", GetModuleName());
        Stop();
    }
}

ErrorCode ModuleActor::Init() { return m_module->Init(); }

ErrorCode ModuleActor::DeInit() { return m_module->DeInit(); }

ErrorCode ModuleActor::Start() {
    if (m_workThread.joinable() || m_poolStarted) {
        LOG_WARN("ModuleActor '{}' is already started.", GetModuleName());
        return ErrorCode::FAILED_ALREADY_START;
    }
    // A run whose worker loop has not returned yet must not be started again:
    // two loops over the same worker state would interleave in one thread-local
    // pipeline.
    if (m_worker && m_worker->IsRunning()) {
        LOG_ERROR("Module '{}' cannot start again while its previous worker loop is still running.", GetModuleName());
        return ErrorCode::FAILED_ALREADY_START;
    }
    if (m_worker) {
        m_worker->PrepareForStart();
        // A source cannot be slowed down by an input queue, so it parks on the
        // edges it fills instead (see core::Worker::IdleAfterEmptyOutput).
        m_worker->SetOutputQueues(GetOutputQueues());
    }
    const ErrorCode workerCode = m_worker->Start();
    if (workerCode != ErrorCode::SUCCESS) {
        LOG_ERROR("ModuleActor '{}' failed to start its worker.", GetModuleName());
        return workerCode;
    }
    // A drain from a previous Stop() must not leak into this run.
    m_worker->ClearDrain();
    // The drop policy lives on the pipeline context so one switch covers every
    // module; the dispatcher is the place where a blocked push would happen.
    if (m_dispatcher) {
        const auto context = m_module ? m_module->GetContext() : nullptr;
        m_dispatcher->SetBackpressurePolicy(context ? context->GetBackpressurePolicy()
                                                    : BackpressurePolicy::kNone);
    }
    // Sharing a thread only works for a worker that returns after one unit of
    // work. A join worker waits for a whole group, so it keeps its own thread.
    if (m_pool && m_worker && !m_worker->HasJoinInputs()) {
        m_pool->AddWorker(m_worker);
        m_poolStarted = true;
        return ErrorCode::SUCCESS;
    }
    m_workThread = std::thread([this]() { m_worker->WorkLoop(); });

    return ErrorCode::SUCCESS;
}

ErrorCode ModuleActor::Stop() {
    m_worker->Stop();
    if (m_poolStarted) {
        if (m_pool && m_pool->IsStarted()) {
            // The pool thread finishes the step it is in and then publishes the
            // exit; until then this worker still touches its queues.
            while (m_worker->IsRunning()) {
                std::this_thread::sleep_for(std::chrono::microseconds(50));
            }
        } else {
            // The pool never started (a failed Start() rollback), so nothing
            // else will ever publish the exit; do it here, or the next Start()
            // refuses to run.
            m_worker->MarkExited();
        }
        m_poolStarted = false;
        return ErrorCode::SUCCESS;
    }
    if (!m_workThread.joinable()) {
        LOG_TRACE("ModuleActor '{}' is already stopped.", GetModuleName());
        return ErrorCode::FAILED_ALREADY_STOP;
    }
    m_workThread.join();
    return ErrorCode::SUCCESS;
}

} // namespace nexusflow
