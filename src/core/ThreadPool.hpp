#ifndef NEXUSFLOW_CORE_THREADPOOL_HPP
#define NEXUSFLOW_CORE_THREADPOOL_HPP

#include "core/Worker.hpp"
#include "nexusflow/ThreadPoolMapping.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

namespace nexusflow {
namespace core {

/// Counters a pool publishes for one run; see ThreadPool::Snapshot().
struct ThreadPoolStats {
    std::uint64_t rounds = 0;         ///< slot passes over its whole worker list
    std::uint64_t turns = 0;          ///< StepOnce() calls that did work
    std::uint64_t blockedRounds = 0;  ///< rounds that ended on a full output edge
    std::uint64_t idleRounds = 0;     ///< rounds where nothing on the slot had work
};

/**
 * @class ThreadPool
 * @brief Hosts several workers on one thread, with a fixed affinity.
 *
 * A pipeline that sets PipelineContext::WithThreadPoolSize() hands its workers
 * to one pool. The mapping decides which thread gets which worker:
 * see ThreadPoolMapping. The assignment is fixed for the run, so every module
 * keeps a single thread writing its counters (AGENTS.md section 5).
 *
 * A thread walks its workers from downstream to upstream, giving each a small
 * budget of Worker::StepOnce() calls before moving on: when consecutive stages
 * share a thread the last stage must make room before the one feeding it
 * produces more, and a hot module must not starve its slot neighbours.
 *
 * The pool owns no worker; ModuleActor registers before Start() and the
 * pipeline stops the pool after every actor has published its exit.
 */
class ThreadPool {
public:
    explicit ThreadPool(std::size_t threadCount, ThreadPoolMapping mapping = ThreadPoolMapping::kContiguous);
    ~ThreadPool();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    /// Registers a worker for the next Start(); slots are assigned in Start().
    void AddWorker(const std::shared_ptr<Worker>& worker);

    /// Assigns slots, then spawns one thread per slot. No worker runs before this.
    void Start();

    /// Asks every thread to return, joins it, and forgets the registrations.
    void StopAndJoin();

    bool IsStarted() const { return m_started.load(std::memory_order_acquire); }

    std::size_t Size() const { return m_threadCount; }

    ThreadPoolMapping Mapping() const { return m_mapping; }

    /// Counters accumulated since the pool was constructed (relaxed reads).
    ThreadPoolStats Snapshot() const;

private:
    void Loop(std::size_t index);

    std::size_t m_threadCount;
    ThreadPoolMapping m_mapping;
    std::vector<std::shared_ptr<Worker>> m_registered;
    std::vector<std::vector<std::shared_ptr<Worker>>> m_workers;
    std::vector<std::thread> m_threads;
    std::atomic<bool> m_stop{false};
    std::atomic<bool> m_started{false};
    std::atomic<std::uint64_t> m_rounds{0};
    std::atomic<std::uint64_t> m_turns{0};
    std::atomic<std::uint64_t> m_blockedRounds{0};
    std::atomic<std::uint64_t> m_idleRounds{0};
};

}  // namespace core
}  // namespace nexusflow

#endif  // NEXUSFLOW_CORE_THREADPOOL_HPP
