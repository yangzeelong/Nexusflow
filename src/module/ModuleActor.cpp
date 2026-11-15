#include "ModuleActor.hpp"
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
    m_syncInputs = m_worker->HasSyncInputs();

    ViewPtr<Module> moduleView{m_module.get()};
    m_dispatcher = std::make_shared<dispatcher::Dispatcher>(configView);

    m_module->SetDispatcher(m_dispatcher);
}

ModuleActor::~ModuleActor() {
    // RAII safety net: ModuleActor owns a joinable std::thread. Destroying it
    // without Stop() would call std::terminate from ~std::thread, which is
    // exactly what happens when a Pipeline is dropped while running.
    if (m_workThread.joinable()) {
        LOG_WARN("ModuleActor '{}' destroyed while its worker thread was still running; stopping it.", GetModuleName());
        Stop();
    }
}

ErrorCode ModuleActor::Init() { return m_module->Init(); }

ErrorCode ModuleActor::DeInit() { return m_module->DeInit(); }

ErrorCode ModuleActor::Start() {
    if (m_workThread.joinable()) {
        LOG_WARN("ModuleActor '{}' is already started.", GetModuleName());
        return ErrorCode::FAILED_ALREADY_START;
    }
    // A run that StopAndJoinWithin() had to abandon leaves its worker loop
    // running for good: starting a second loop over the same worker state would
    // interleave two runs in one thread-local pipeline.
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
    m_workThread = std::thread([this]() { m_worker->WorkLoop(); });

    return ErrorCode::SUCCESS;
}

ErrorCode ModuleActor::Stop() {
    m_worker->Stop();
    if (!m_workThread.joinable()) {
        LOG_TRACE("ModuleActor '{}' is already stopped.", GetModuleName());
        return ErrorCode::FAILED_ALREADY_STOP;
    }
    m_workThread.join();
    return ErrorCode::SUCCESS;
}

bool ModuleActor::StopAndJoinWithin(std::chrono::milliseconds timeout) {
    m_worker->Stop();
    if (!m_workThread.joinable()) {
        LOG_TRACE("ModuleActor '{}' is already stopped.", GetModuleName());
        return true;
    }

    // Give the loop a deadline instead of blocking the caller. A thread that is
    // stuck in a long call cannot be stopped from outside, and waiting on it
    // unconditionally would turn one wedged module into a wedged pipeline.
    const bool exited = m_worker->WaitForStopRequest(timeout);
    if (!exited) {
        // Deliberately not joined: leaving a joinable std::thread behind and
        // letting it unwind at process exit keeps whatever the module is still
        // touching alive. It is reported as a drain timeout instead.
        m_stopOverdue = true;
        LOG_ERROR("Module '{}' did not leave its worker loop within {} ms of the stop request; "
                  "abandoning its thread.",
                  GetModuleName(), static_cast<long long>(timeout.count()));
        return false;
    }
    m_workThread.join();
    return true;
}

} // namespace nexusflow
