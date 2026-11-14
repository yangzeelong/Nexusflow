#include "ModuleActor.hpp"
#include "nexusflow/ErrorCode.hpp"
#include <memory>

namespace nexusflow {

ModuleActor::ModuleActor(const std::shared_ptr<Module>& module, const Config& config) {
    m_config = std::make_unique<Config>(config);

    ViewPtr<Config> configView{m_config.get()};

    m_module = module;
    m_worker = std::make_shared<core::Worker>(m_module, configView);

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
    m_worker->Reset();
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

} // namespace nexusflow
