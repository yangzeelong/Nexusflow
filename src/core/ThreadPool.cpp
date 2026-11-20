#include "core/ThreadPool.hpp"

#include <chrono>

#if defined(_MSC_VER)
#include <intrin.h>
#elif defined(__GNUC__) && (defined(__x86_64__) || defined(__i386__))
#include <immintrin.h>
#endif

namespace nexusflow {
namespace core {

ThreadPool::ThreadPool(std::size_t threadCount)
    : m_threadCount(threadCount == 0 ? 1 : threadCount), m_workers(m_threadCount == 0 ? 1 : threadCount) {}

ThreadPool::~ThreadPool() { StopAndJoin(); }

namespace {

/// How many StepOnce() calls one slot gives a single worker before it
/// moves on. Large enough to drain any queue the pipeline can hold.
constexpr int kDrainLimit = 4096;


/// Spins the core without giving it away: see the blocked branch in Loop().
void Pause() {
#if defined(_MSC_VER)
    _mm_pause();
#elif defined(__GNUC__) && (defined(__x86_64__) || defined(__i386__))
    __builtin_ia32_pause();
#else
    std::this_thread::yield();
#endif
}

}  // namespace

void ThreadPool::AddWorker(const std::shared_ptr<Worker>& worker) {
    if (!worker) {
        return;
    }
    const std::size_t slot = m_workersSeen % m_threadCount;
    ++m_workersSeen;
    m_workers[slot].push_back(worker);
}

void ThreadPool::Start() {
    if (m_started.load(std::memory_order_acquire)) {
        return;
    }
    m_stop.store(false, std::memory_order_relaxed);
    for (std::size_t i = 0; i < m_threadCount; ++i) {
        m_threads.emplace_back([this, i]() { Loop(i); });
    }
    m_started.store(true, std::memory_order_release);
}

void ThreadPool::StopAndJoin() {
    m_stop.store(true, std::memory_order_release);
    for (auto& thread : m_threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    m_threads.clear();
    m_started.store(false, std::memory_order_release);
    for (auto& slot : m_workers) {
        slot.clear();
    }
    m_workersSeen = 0;
}

void ThreadPool::Loop(std::size_t index) {
    std::vector<std::shared_ptr<Worker>>& mine = m_workers[index];
    unsigned spins = 0;
    while (!m_stop.load(std::memory_order_relaxed)) {
        bool didWork = false;
        bool blocked = false;
        for (auto& worker : mine) {
            // Work-conserving: drain this worker before moving on, otherwise a
            // deep chain pays one full slot round of latency per hop and the
            // pipeline collapses to a fraction of its per-module throughput.
            // StepOnce() breaks on a full edge, so this cannot spin forever on
            // a blocked producer; kDrainLimit only guards against a module that
            // reports work without ever emptying.
            for (int drained = 0; drained < kDrainLimit; ++drained) {
                if (!worker->StepOnce()) {
                    break;
                }
                didWork = true;
            }
            if (!worker->OutputsHaveRoom()) {
                // Backpressure rather than idleness: the consumer of a full edge
                // is still running (possibly in this very slot), so the round
                // must not fall through to the millisecond-scale sleep.
                blocked = true;
            }
        }
        if (didWork) {
            spins = 0;
            continue;
        }
        if (blocked) {
            spins = 0;
            // Pause, do not yield(): a blocked slot unblocks within a few
            // hundred cycles once the consumer drains one slot, while a
            // yield hands the core away for microseconds.
            Pause();
            continue;
        }
        // Nothing on this slot had work. Re-checking costs nanoseconds; a
        // syscall costs microseconds, so spin first and only then yield/sleep.
        if (spins < 64) {
            ++spins;
            std::this_thread::yield();
        } else if (spins < 256) {
            ++spins;
            std::this_thread::sleep_for(std::chrono::microseconds(10));
        } else {
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
    }
    // A worker whose actor is stopped by the pipeline is normally stepped one
    // last time and publishes its own exit; this covers a worker that was never
    // stepped again before the pool went away.
    for (auto& worker : mine) {
        worker->MarkExited();
    }
}

}  // namespace core
}  // namespace nexusflow
