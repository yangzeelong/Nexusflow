#ifndef NEXUSFLOW_CORE_THREADPOOL_HPP
#define NEXUSFLOW_CORE_THREADPOOL_HPP

#include "core/Worker.hpp"

#include <atomic>
#include <cstddef>
#include <memory>
#include <thread>
#include <vector>

namespace nexusflow {
namespace core {

/**
 * @class ThreadPool
 * @brief Hosts several workers on one thread, with a fixed affinity.
 *
 * A pipeline that sets PipelineContext::WithThreadPoolSize() hands its workers
 * to one pool. Worker i goes to slot i % Size() and stays there for the run, so
 * every module keeps a single thread writing its counters (AGENTS.md section 5)
 * while the process needs fewer threads than modules.
 *
 * A thread steps its workers in turn: each Worker::StepOnce() does one
 * non-blocking unit of work. When nothing on the thread had work, it backs off
 * (yield, then micro-sleeps) so an idle pipeline does not burn its cores.
 *
 * The pool owns no worker; ModuleActor registers before Start() and the
 * pipeline stops the pool after every actor has published its exit.
 */
class ThreadPool {
public:
    explicit ThreadPool(std::size_t threadCount);
    ~ThreadPool();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    /// Registers a worker for the next Start(); slots are assigned round-robin
    /// in registration order.
    void AddWorker(const std::shared_ptr<Worker>& worker);

    /// Spawns one thread per slot. No worker runs before this.
    void Start();

    /// Asks every thread to return, joins it, and forgets the registrations.
    void StopAndJoin();

    bool IsStarted() const { return m_started.load(std::memory_order_acquire); }

    std::size_t Size() const { return m_threadCount; }

private:
    void Loop(std::size_t index);

    std::size_t m_threadCount;
    std::size_t m_workersSeen = 0;
    std::vector<std::vector<std::shared_ptr<Worker>>> m_workers;
    std::vector<std::thread> m_threads;
    std::atomic<bool> m_stop{false};
    std::atomic<bool> m_started{false};
};

}  // namespace core
}  // namespace nexusflow

#endif  // NEXUSFLOW_CORE_THREADPOOL_HPP
