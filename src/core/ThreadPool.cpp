#include "core/ThreadPool.hpp"

#include <chrono>

#if defined(_MSC_VER)
#include <intrin.h>
#elif defined(__GNUC__) && (defined(__x86_64__) || defined(__i386__))
#include <immintrin.h>
#endif

namespace nexusflow {
namespace core {

ThreadPool::ThreadPool(std::size_t threadCount, ThreadPoolMapping mapping)
    : m_threadCount(threadCount == 0 ? 1 : threadCount),
      m_mapping(mapping),
      m_workers(threadCount == 0 ? 1 : threadCount) {}

ThreadPool::~ThreadPool() { StopAndJoin(); }

namespace {

/// StepOnce() calls one worker may run before its slot moves on. Small on
/// purpose: with several modules per thread the budget bounds how long a hot
/// module keeps the others of its slot waiting.
constexpr int kTurnBudget = 16;

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
    m_registered.push_back(worker);
}

void ThreadPool::Start() {
    if (m_started.load(std::memory_order_acquire)) {
        return;
    }
    // Slots are assigned here because the contiguous mapping needs the final
    // module count, which is only known once every actor has registered.
    const std::size_t total = m_registered.size();
    for (std::size_t i = 0; i < total; ++i) {
        std::size_t slot = i;
        if (total > m_threadCount) {
            slot = m_mapping == ThreadPoolMapping::kRoundRobin ? i % m_threadCount
                                                               : (i * m_threadCount) / total;
        }
        m_workers[slot].push_back(m_registered[i]);
    }
    m_registered.clear();
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
    m_registered.clear();
}

ThreadPoolStats ThreadPool::Snapshot() const {
    ThreadPoolStats stats;
    stats.rounds = m_rounds.load(std::memory_order_relaxed);
    stats.turns = m_turns.load(std::memory_order_relaxed);
    stats.blockedRounds = m_blockedRounds.load(std::memory_order_relaxed);
    stats.idleRounds = m_idleRounds.load(std::memory_order_relaxed);
    return stats;
}

void ThreadPool::Loop(std::size_t index) {
    std::vector<std::shared_ptr<Worker>>& mine = m_workers[index];
    unsigned spins = 0;
    while (!m_stop.load(std::memory_order_relaxed)) {
        m_rounds.fetch_add(1, std::memory_order_relaxed);
        bool didWork = false;
        bool blocked = false;
        // Downstream first: a stage that shares this thread with the one feeding
        // it must hand its output on before upstream fills the edge again.
        for (auto it = mine.rbegin(); it != mine.rend(); ++it) {
            const std::shared_ptr<Worker>& worker = *it;
            for (int turn = 0; turn < kTurnBudget; ++turn) {
                if (!worker->StepOnce()) {
                    break;
                }
                didWork = true;
                m_turns.fetch_add(1, std::memory_order_relaxed);
            }
            if (!worker->OutputsHaveRoom()) {
                blocked = true;
            }
        }
        if (didWork) {
            spins = 0;
            continue;
        }
        if (blocked) {
            m_blockedRounds.fetch_add(1, std::memory_order_relaxed);
            spins = 0;
            // Pause, do not yield(): a blocked slot unblocks within a few
            // hundred cycles once the consumer drains one slot, while a
            // yield hands the core away for microseconds.
            Pause();
            continue;
        }
        m_idleRounds.fetch_add(1, std::memory_order_relaxed);
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
