#ifndef NEXUSFLOW_COMMON_SPSC_QUEUE_HPP
#define NEXUSFLOW_COMMON_SPSC_QUEUE_HPP

#include <nexusflow/common/ConcurrentQueue.hpp>
#include <nexusflow/common/Optional.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <new>
#include <thread>
#include <type_traits>
#include <utility>

#if defined(_MSC_VER) && (defined(_M_X64) || defined(_M_IX86))
#include <intrin.h>    // _mm_pause()
#endif

namespace nexusflow {

/**
 * @class SpscQueue
 * @brief Bounded single-producer / single-consumer queue used for graph edges.
 *
 * Every edge of a pipeline has exactly one producer (the upstream module's
 * worker thread) and one consumer (the downstream module's worker thread), so
 * the per-edge queue can use a lock-free ring buffer instead of taking a mutex
 * on every push/pop. Only the blocking paths (a full queue or an empty queue)
 * touch the mutex and the condition variables, so idle workers still sleep
 * instead of busy-spinning.
 *
 * The push side is the fast path: a producer stores the item and publishes the
 * new tail with a release store. The consumer only has to notice that tail from
 * a back-off loop before it takes the wait lock, and the producer publishes to
 * the consumer the same way. The wake-up handshake therefore goes through one
 * release store / acquire load instead of two independent relaxed flags, which
 * is what a partially ordered relaxed flag pair cannot guarantee on x86.
 *
 * A non-positive capacity falls back to ConcurrentQueue, which keeps the
 * unbounded case (-1) and its exact semantics. The ring recycles slots as soon
 * as the consumer has moved the head, so a bounded capacity N needs exactly
 * N * sizeof(T) bytes and never grows.
 *
 * A waiting thread announces itself under the wait mutex *before* it checks the
 * queue one last time, and the fast path takes that same mutex after it has
 * published the item and before it notifies. A notification can therefore never
 * slip into the window between the waiter's last check and its actual wait.
 *
 * Announcing is a store followed by a load of the ring index, and waking is a
 * store of that index followed by a load of the flag: a store-buffer (Dekker)
 * handshake, which is only correct when both sides put a StoreLoad barrier
 * between their own store and their own load. The waiter's barrier is the
 * seq_cst store of the flag (an xchg on x86), the fast path's barrier is the
 * seq_cst store of the ring index it publishes anyway (also an xchg on x86,
 * on a line the producer/consumer already owns). Without that barrier the fast
 * path may read a stale "nobody is waiting" while the waiter's "I am waiting"
 * store is still in its store buffer, and the item stays in the ring with both
 * threads asleep.
 *
 * The hot indices are padded to a full cache line each: head is written by the
 * consumer and tail by the producer, so letting them share a line would make
 * every message invalidate the other core's copy (false sharing) and the
 * lock-free ring would lose against a mutex-protected deque. The pushed/popped
 * statistics are not counted separately for the same reason -- they *are* the
 * two indices, which is exact because a successful push/pop moves the
 * respective index by one.
 *
 * @tparam T The type of elements stored in the queue.
 */
template <typename T>
class SpscQueue {
public:
    /**
     * @brief Snapshot of the queue size and its lifetime counters.
     */
    struct QueueCounters {
        std::size_t size          = 0;
        std::size_t capacity      = 0;    // 0 means unbounded
        std::size_t highWaterMark = 0;
        std::uint64_t pushed      = 0;
        std::uint64_t popped      = 0;
        std::uint64_t discardedTotal = 0;
    };

    /**
     * @brief Constructs the queue.
     * @param capacity The maximum capacity. A value of -1 (default) means
     *        unbounded; other non-positive values are treated the same way.
     */
    explicit SpscQueue(int capacity = -1) : m_capacity(capacity) {
        if (capacity <= 0) {
            m_fallback.reset(new ConcurrentQueue<T>(-1));
            return;
        }
        m_slotCount = static_cast<std::size_t>(capacity);
        m_slots.reset(new Slot[m_slotCount]);
    }

    ~SpscQueue() {
        if (m_fallback || !m_slots) {
            return;
        }
        const std::size_t head = m_head.load(std::memory_order_relaxed);
        const std::size_t tail = m_tail.load(std::memory_order_relaxed);
        for (std::size_t index = head; index != tail; ++index) {
            ElementAt(index % m_slotCount)->~T();
        }
    }

    SpscQueue(const SpscQueue&)            = delete;
    SpscQueue& operator=(const SpscQueue&) = delete;

    /**
     * @brief Pushes an item (blocking while the queue is full).
     * @return false when the queue has been shut down.
     */
    bool push(T&& item) {
        if (m_fallback) {
            return m_fallback->push(std::move(item));
        }
        if (m_shutdown.load(std::memory_order_relaxed)) {
            return false;
        }
        // A producer waiting for space is parked on m_condNotFull, which only a
        // consumer wakes. A pipeline that stops its workers while one of them is
        // blocked on a full edge has no consumer left to do that, so the waiter
        // must find the shutdown flag itself. Reading it there costs a second
        // relaxed load only after the first one has already failed, not a locked
        // read-modify-write on every message.
        if (m_producerWaiting.load(std::memory_order_relaxed) && m_shutdown.load(std::memory_order_relaxed)) {
            return false;
        }
        if (PutIfSpace(item)) {
            WakeConsumer();
            return true;
        }
        // The item is still alive here: PutIfSpace() only moves once it takes a
        // free slot. The producer therefore retries the very same object after
        // every wake-up instead of re-publishing a moved-from one.
        const int spun = SpinAttempt([this, &item]() { return PutIfSpace(item); },
                                     [this]() { return m_head.load(std::memory_order_acquire); });
        if (spun > 0) {
            WakeConsumer();
            return true;
        }
        if (spun < 0) {
            return false;
        }
        std::unique_lock<std::mutex> lock(m_waitMutex);
        // seq_cst, not release: this store must be globally visible before the
        // last PutIfSpace() below reads the tail, see the class comment.
        m_producerWaiting.store(true, std::memory_order_seq_cst);
        for (;;) {
            if (PutIfSpace(item)) {
                m_producerWaiting.store(false, std::memory_order_relaxed);
                // The consumer may be asleep on this very lock; notify while
                // holding it, the waiter re-checks after it acquires the lock.
                m_condNotEmpty.notify_one();
                return true;
            }
            if (m_shutdown.load(std::memory_order_relaxed)) {
                m_producerWaiting.store(false, std::memory_order_relaxed);
                return false;
            }
            // Bounded, not a plain wait(): a producer parked on a full queue has
            // to come back regularly. A consumer that stopped without notifying
            // would otherwise strand it here forever, and the pipeline stop path
            // could never join the worker. shutdown() also wakes this wait, but
            // only once the stop path gets far enough to close the queue -- which
            // it cannot do while this producer is still inside push().
            m_condNotFull.wait_for(lock, std::chrono::milliseconds(kParkTimeoutMs));
        }
    }

    /**
     * @brief Pushes an item (lvalue reference overload).
     */
    bool push(const T& item) {
        T temp = item;
        return push(std::move(temp));
    }

    /**
     * @brief Tries to push an item, waiting up to the given timeout.
     * @return false when the queue stayed full or has been shut down.
     */
    template <class Rep, class Per>
    bool pushFor(T&& item, const std::chrono::duration<Rep, Per>& timeout) {
        if (m_fallback) {
            return m_fallback->pushFor(std::move(item), timeout);
        }
        if (m_shutdown.load(std::memory_order_relaxed)) {
            return false;
        }
        if (m_producerWaiting.load(std::memory_order_relaxed) && m_shutdown.load(std::memory_order_relaxed)) {
            return false;
        }
        if (PutIfSpace(item)) {
            WakeConsumer();
            return true;
        }
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        // The spin comes out of the caller's timeout, not on top of it, and it is
        // skipped entirely when the timeout is shorter than the spin budget, so a
        // short timeout is never stretched into a long busy wait.
        const int spun = timeout < std::chrono::microseconds(kSpinBudgetMicros)
                             ? 0
                             : SpinAttempt([this, &item]() { return PutIfSpace(item); },
                                           [this]() { return m_head.load(std::memory_order_acquire); });
        if (spun > 0) {
            WakeConsumer();
            return true;
        }
        if (spun < 0) {
            return false;
        }
        std::unique_lock<std::mutex> lock(m_waitMutex);
        // seq_cst, not release: this store must be globally visible before the
        // last PutIfSpace() below reads the tail, see the class comment.
        m_producerWaiting.store(true, std::memory_order_seq_cst);
        for (;;) {
            if (PutIfSpace(item)) {
                m_producerWaiting.store(false, std::memory_order_relaxed);
                m_condNotEmpty.notify_one();
                return true;
            }
            if (m_shutdown.load(std::memory_order_relaxed)) {
                m_producerWaiting.store(false, std::memory_order_relaxed);
                return false;
            }
            if (m_condNotFull.wait_until(lock, deadline) == std::cv_status::timeout) {
                const bool pushed = PutIfSpace(item);
                m_producerWaiting.store(false, std::memory_order_relaxed);
                if (pushed) {
                    m_condNotEmpty.notify_one();
                }
                return pushed;
            }
        }
    }

    template <class Rep, class Per>
    bool pushFor(const T& item, const std::chrono::duration<Rep, Per>& timeout) {
        T temp = item;
        return pushFor(std::move(temp), timeout);
    }

    /**
     * @brief Tries to push an item without blocking.
     * @return false when the queue was full or has been shut down.
     */
    bool tryPush(T&& item) {
        if (m_fallback) {
            return m_fallback->tryPush(std::move(item));
        }
        if (m_shutdown.load(std::memory_order_relaxed)) {
            return false;
        }
        if (!PutIfSpace(item)) {
            return false;
        }
        WakeConsumer();
        return true;
    }

    bool tryPush(const T& item) {
        T temp = item;
        return tryPush(std::move(temp));
    }

    /**
     * @brief Waits for and pops an item (blocking while the queue is empty).
     * Items already queued are still popped after shutdown; false is returned
     * once the queue is empty and shut down.
     */
    bool waitAndPop(T& itemRef) {
        if (m_fallback) {
            return m_fallback->waitAndPop(itemRef);
        }
        if (PopIfAvailable(itemRef)) {
            WakeProducer();
            return true;
        }
        const int spun = SpinAttempt([this, &itemRef]() { return PopIfAvailable(itemRef); },
                                     [this]() { return m_tail.load(std::memory_order_acquire); });
        if (spun > 0) {
            WakeProducer();
            return true;
        }
        if (spun < 0) {
            return false;
        }
        std::unique_lock<std::mutex> lock(m_waitMutex);
        // seq_cst, not release: this store must be globally visible before the
        // last PopIfAvailable() below reads the tail, see the class comment.
        m_consumerWaiting.store(true, std::memory_order_seq_cst);
        for (;;) {
            if (PopIfAvailable(itemRef)) {
                m_consumerWaiting.store(false, std::memory_order_relaxed);
                m_condNotFull.notify_one();
                return true;
            }
            if (m_shutdown.load(std::memory_order_acquire)) {
                m_consumerWaiting.store(false, std::memory_order_relaxed);
                return false;
            }
            // Bounded, not a plain wait(): a consumer that parks here must still
            // come back regularly. Otherwise a lost wake-up (or a producer that
            // stopped without notifying) strands the consumer forever, and the
            // pipeline stop path can never join it. The re-check above runs on
            // every wake-up, so a spurious return only costs one extra poll.
            m_condNotEmpty.wait_for(lock, std::chrono::milliseconds(kParkTimeoutMs));
        }
    }

    /**
     * @brief Waits for and pops an item, up to the given timeout.
     * @return false on timeout, or when the queue is empty and shut down.
     */
    template <class Rep, class Per>
    bool waitAndPopFor(T& itemRef, const std::chrono::duration<Rep, Per>& timeout) {
        if (m_fallback) {
            return m_fallback->waitAndPopFor(itemRef, timeout);
        }
        if (PopIfAvailable(itemRef)) {
            WakeProducer();
            return true;
        }
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        // The spin comes out of the caller's timeout, not on top of it, and it is
        // skipped entirely when the timeout is shorter than the spin budget.
        const int spun = timeout < std::chrono::microseconds(kSpinBudgetMicros)
                             ? 0
                             : SpinAttempt([this, &itemRef]() { return PopIfAvailable(itemRef); },
                                           [this]() { return m_tail.load(std::memory_order_acquire); });
        if (spun > 0) {
            WakeProducer();
            return true;
        }
        if (spun < 0) {
            return false;
        }
        std::unique_lock<std::mutex> lock(m_waitMutex);
        // seq_cst, not release: this store must be globally visible before the
        // last PopIfAvailable() below reads the tail, see the class comment.
        m_consumerWaiting.store(true, std::memory_order_seq_cst);
        for (;;) {
            if (PopIfAvailable(itemRef)) {
                m_consumerWaiting.store(false, std::memory_order_relaxed);
                m_condNotFull.notify_one();
                return true;
            }
            if (m_shutdown.load(std::memory_order_acquire)) {
                m_consumerWaiting.store(false, std::memory_order_relaxed);
                return false;
            }
            if (m_condNotEmpty.wait_until(lock, deadline) == std::cv_status::timeout) {
                const bool popped = PopIfAvailable(itemRef);
                m_consumerWaiting.store(false, std::memory_order_relaxed);
                if (popped) {
                    m_condNotFull.notify_one();
                }
                return popped;
            }
        }
    }

    /**
     * @brief Tries to pop an item without blocking.
     * @return An Optional holding the item, or nullOpt when the queue is empty.
     */
    Optional<T> tryPop() {
        if (m_fallback) {
            return m_fallback->tryPop();
        }
        T item;
        if (!PopIfAvailable(item)) {
            return nullOpt;
        }
        WakeProducer();
        return Optional<T>(std::move(item));
    }

    /**
     * @brief Tries to pop an item without blocking.
     */
    bool tryPop(T& itemRef) {
        if (m_fallback) {
            return m_fallback->tryPop(itemRef);
        }
        if (!PopIfAvailable(itemRef)) {
            return false;
        }
        WakeProducer();
        return true;
    }

    /**
     * @brief Shuts the queue down and wakes all waiting threads.
     * @details Both sides are woken: a producer that is parked on a full queue
     * would otherwise wait for a consumer that has already left its loop.
     */
    void shutdown() {
        if (m_fallback) {
            m_fallback->shutdown();
            return;
        }
        m_shutdown.store(true, std::memory_order_release);
        {
            // Pairs with the producers/consumers that announce themselves under
            // the same lock: once it is released here, every waiter that already
            // holds the lock has seen the flag, and the notify below reaches it.
            std::lock_guard<std::mutex> lock(m_waitMutex);
        }
        m_condNotEmpty.notify_all();
        m_condNotFull.notify_all();
    }

    /**
     * @brief Destroys whatever is still buffered, counting it as discarded.
     * @details Only safe while no thread is using the queue: Pipeline::Cancel()
     * calls it once every worker has been joined. The shut-down state is left
     * alone, so this throws the backlog away without reopening the queue.
     *
     * The destroyed messages are charged to discardedTotal and to neither
     * pushedTotal nor poppedTotal, which is what keeps
     * pushed == popped + size + discarded true across any number of clears.
     */
    void clear() {
        if (m_fallback) {
            m_fallback->clear();
            return;
        }
        std::lock_guard<std::mutex> lock(m_waitMutex);
        DiscardBufferedLocked();
    }

    /**
     * @brief Reopens a shut-down queue so the owning pipeline can start again.
     * @details Only safe while no thread is using the queue (Pipeline calls this
     * before it starts its workers). Whatever was still queued is discarded
     * through the same path as clear(): a shut-down queue is closed at teardown,
     * where anything left over is lost, and delivering it to a new run would
     * report stale work.
     */
    void restart() {
        if (m_fallback) {
            m_fallback->restart();
            return;
        }
        std::lock_guard<std::mutex> lock(m_waitMutex);
        DiscardBufferedLocked();
        m_shutdown.store(false, std::memory_order_release);
        m_consumerWaiting.store(false, std::memory_order_release);
        m_producerWaiting.store(false, std::memory_order_release);
    }

    /**
     * @brief Checks whether the queue was shut down.
     * @details A shut-down queue rejects pushes but still hands out what it
     * already holds, so a consumer uses this to tell "nothing will ever arrive
     * again" from "temporarily empty".
     */
    bool isShutdown() const {
        if (m_fallback) {
            return m_fallback->isShutdown();
        }
        return m_shutdown.load(std::memory_order_acquire);
    }

    /**
     * @brief Checks if the queue is currently empty (approximate under racing).
     */
    bool isEmpty() const { return getSize() == 0; }

    /**
     * @brief Returns the current number of queued items.
     */
    std::size_t getSize() const {
        if (m_fallback) {
            return m_fallback->getSize();
        }
        // The two indices are published by different threads, so a sample can
        // see a newer head than tail. The counters only ever grow, so retrying
        // is enough to keep the result sane instead of returning a huge wrap.
        for (int attempt = 0; attempt < 4; ++attempt) {
            const std::size_t head = m_head.load(std::memory_order_acquire);
            const std::size_t tail = m_tail.load(std::memory_order_acquire);
            if (tail >= head) {
                return tail - head;
            }
        }
        return 0;
    }

    /**
     * @brief Waits until the consumer has taken everything that was pushed.
     * @details The caller must already have joined every producer of this edge,
     * so an empty queue is the only state this can end in; the wait has no
     * deadline on purpose, because a deadline would turn "the consumer is slow"
     * into "the drain gave up and left work behind". It reports as soon as the
     * queue is empty, so the usual use is "stop the producer, then wait for the
     * consumer to catch up". A shut-down queue is still waited on: shutdown()
     * only rejects further pushes, and the consumer keeps popping what is
     * buffered.
     */
    void waitUntilEmpty() {
        if (m_fallback) {
            while (!m_fallback->isEmpty()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            return;
        }
        for (;;) {
            if (getSize() == 0) {
                return;
            }
            // The consumer holds the wake-up lock while it checks for work, so a
            // short wait here either sees the next pop or simply re-checks.
            std::unique_lock<std::mutex> lock(m_waitMutex);
            m_condNotFull.wait_for(lock, std::chrono::milliseconds(1));
        }
    }

    /**
     * @brief Whether a consumer is currently blocked inside waitAndPop().
     * @details Callers that would otherwise sleep and poll the queue (a source
     * module that produced nothing waits for its edge to get room) use this to
     * tell an edge whose consumer will wake them from one whose consumer only
     * looks at the queue now and then -- in the latter case nothing will ever
     * signal, so the caller has to fall back to a timed wait.
     */
    bool isEventDriven() const {
        if (m_fallback) {
            return false;
        }
        return m_consumerWaiting.load(std::memory_order_seq_cst);
    }

    /**
     * @brief Waits until the queue has room for another item, or the timeout ends.
     * @details The producer-side counterpart of waitUntilEmpty(): it is for a
     * thread that is neither pushing nor popping but only wants to know when it
     * may produce again. It spins while the consumer is making progress, parks on
     * the consumer's wake-up path, and re-checks the shutdown flag on every poll,
     * so a queued wait never outlives teardown.
     * @return true when there is room (or the queue is empty); false on timeout.
     */
    template <class Rep, class Per>
    bool waitForRoom(const std::chrono::duration<Rep, Per>& timeout) {
        if (m_fallback) {
            const auto fallbackDeadline = std::chrono::steady_clock::now() + timeout;
            while (getSize() >= static_cast<std::size_t>(m_capacity)) {
                if (m_fallback->isShutdown() || std::chrono::steady_clock::now() >= fallbackDeadline) {
                    return false;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            return true;
        }
        if (!Full()) {
            return true;
        }
        const int spun = SpinAttempt([this]() { return !Full(); },
                                     [this]() { return m_head.load(std::memory_order_acquire); });
        if (spun > 0) {
            return true;
        }
        if (spun < 0) {
            return false;
        }
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        std::unique_lock<std::mutex> lock(m_waitMutex);
        // Raise the flag *before* re-checking room, because WakeProducer() only
        // notifies while it is set: announcing the wait after the check would
        // miss a wake-up that lands in between, and the producer would sit out
        // the whole timeout. A stale "waiter" only costs one spurious notify.
        m_producerWaiting.store(true, std::memory_order_seq_cst);
        bool hasRoom = false;
        for (;;) {
            if (!Full()) {
                hasRoom = true;
                break;
            }
            if (m_shutdown.load(std::memory_order_acquire)) {
                break;
            }
            if (m_condNotFull.wait_until(lock, deadline) == std::cv_status::timeout) {
                hasRoom = !Full();
                break;
            }
        }
        m_producerWaiting.store(false, std::memory_order_seq_cst);
        return hasRoom;
    }

    /**
     * @brief Returns a snapshot of the queue counters.
     */
    QueueCounters GetStats() const {
        QueueCounters counters;
        if (m_fallback) {
            const typename ConcurrentQueue<T>::QueueCounters fallbackCounters = m_fallback->GetStats();
            counters.size          = fallbackCounters.size;
            counters.capacity      = fallbackCounters.capacity;
            counters.highWaterMark = fallbackCounters.highWaterMark;
            counters.pushed        = fallbackCounters.pushed;
            counters.popped        = fallbackCounters.popped;
            counters.discardedTotal = fallbackCounters.discardedTotal;
            return counters;
        }
        counters.size          = getSize();
        counters.capacity      = static_cast<std::size_t>(m_capacity);
        counters.highWaterMark = m_highWaterMark.load(std::memory_order_relaxed);
        // The ring keeps no separate per-message counters: every successful
        // push/pop moves the respective index by exactly one, and the hot path
        // must not touch a third cache line for statistics. restart() and clear()
        // fold the indices into a baseline first, so these two still only grow
        // across a Stop()/Start() pair.
        counters.pushed    = m_pushedBase + m_tail.load(std::memory_order_relaxed);
        counters.popped    = m_poppedBase + m_head.load(std::memory_order_relaxed);
        counters.discardedTotal = m_discardedTotal.load(std::memory_order_relaxed);
        return counters;
    }

    /**
     * @brief Forgets the deepest backlog seen so far.
     * @details Only the water mark restarts; the pushed/popped counters keep
     * growing so two snapshots can still be subtracted. Producers race with this
     * by design: a push that lands during the reset may record a depth against
     * the previous window, which a caller avoids by resetting a stopped queue.
     */
    void resetHighWaterMark() {
        if (m_fallback) {
            m_fallback->resetHighWaterMark();
            return;
        }
        m_highWaterMark.store(getSize(), std::memory_order_relaxed);
    }

    /**
     * @brief Capacity passed to the constructor (non-positive means unbounded).
     */
    int capacity() const { return m_capacity; }

    /**
     * @brief True when the queue uses the lock-free ring (bounded capacity).
     */
    bool hasLockFreeStorage() const { return m_fallback == nullptr; }

private:
    /// Padding unit for the hot indices: one cache line, doubled to also keep
    /// the adjacent-line prefetcher of recent cores from pulling in the
    /// neighbouring index.
    static constexpr std::size_t kCacheLineSize = 128;

    /// How long a blocking path re-checks the ring before it parks on the
    /// condition variable. Both ends of an edge are usually only a processing
    /// batch apart, while parking costs a round trip through the OS scheduler:
    /// measured on a saturated 3-stage pipeline with a small (single-digit) edge capacity
    /// the wake round trip dominates and caps throughput at ~480k msg/s with a
    /// 2 us budget, at ~1.06M with 8 us, ~1.63M with 20 us and ~1.71M with 24 us
    /// (average latency dropping from ~34 us to ~10 us on the way). 20 us is the
    /// flat part of that curve, so it is the default; the budget is a bound, not
    /// a promise, so a genuinely idle stage still parks and sleeps.
    static constexpr int kSpinBudgetMicros = 20;
    /// The clock is only read every kSpinClockMask+1 pauses: a steady_clock
    /// read costs about as much as a dozen pauses.
    static constexpr int kSpinClockMask = 15;
    /// Upper bound for a parked consumer in waitAndPop(): it re-checks the queue
    /// and the shutdown flag at most this often, so a missed wake-up delays it by
    /// one poll instead of parking it for the rest of the run.
    static constexpr int kParkTimeoutMs = 5;

    /// One relaxed spin iteration for the calling core.
    static void SpinPause() {
#if defined(_MSC_VER) && (defined(_M_X64) || defined(_M_IX86))
        _mm_pause();
#elif defined(__GNUC__) && (defined(__x86_64__) || defined(__i386__))
        __builtin_ia32_pause();
#else
        std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
    }

    /**
     * @brief Retries @p attempt while the other side keeps making progress.
     *
     * The blocking paths are entered on every full/empty transition of an edge,
     * and both ends of an edge are normally only one processing batch apart.
     * Parking on a condition variable costs a round trip through the OS
     * scheduler (microseconds), while re-checking the ring costs nanoseconds, so
     * a thread spins as long as the other end is actually moving: every change
     * of @p progress (the far index) renews the budget. A stage whose peer is
     * idle never renews it and parks after kSpinBudgetMicros, so an idle
     * pipeline still sleeps instead of burning a core.
     *
     * @return 1 on success, 0 when the budget expired, -1 on shutdown.
     */
    template <typename Attempt, typename Progress>
    int SpinAttempt(Attempt attempt, Progress progress) {
        const auto budget    = std::chrono::microseconds(kSpinBudgetMicros);
        std::size_t observed = progress();
        auto deadline        = std::chrono::steady_clock::now() + budget;
        for (int tick = 0;; ++tick) {
            if (attempt()) {
                return 1;
            }
            if (m_shutdown.load(std::memory_order_acquire)) {
                return -1;
            }
            if ((tick & kSpinClockMask) == 0) {
                const auto now            = std::chrono::steady_clock::now();
                const std::size_t current = progress();
                if (current != observed) {
                    observed = current;
                    deadline = now + budget;
                } else if (now >= deadline) {
                    return 0;
                }
            }
            SpinPause();
        }
    }

    struct Slot {
        typename std::aligned_storage<sizeof(T), alignof(T)>::type storage;
    };

    T* ElementAt(std::size_t index) const {
        return reinterpret_cast<T*>(
            const_cast<typename std::aligned_storage<sizeof(T), alignof(T)>::type*>(&m_slots[index].storage));
    }

    /**
     * @brief Destroys the buffered items and counts them as discarded.
     *
     * Caller must hold m_waitMutex. The items are destroyed *before* the indices
     * are rewound: ~SpscQueue() only walks [head, tail), so zeroing them first
     * would make the leftovers unreachable -- every T still parked in a slot
     * would keep its payload alive for good, and the next run would placement-new
     * over a live object. Neither is recoverable afterwards.
     *
     * The indices are then folded into the published baselines so pushed/popped
     * never go backwards, and the count of what was destroyed goes into
     * discardedTotal instead of either of them.
     */
    void DiscardBufferedLocked() {
        const std::size_t head = m_head.load(std::memory_order_relaxed);
        const std::size_t tail = m_tail.load(std::memory_order_relaxed);
        for (std::size_t index = head; index != tail; ++index) {
            ElementAt(index % m_slotCount)->~T();
        }
        m_pushedBase += tail;
        m_poppedBase += head;
        m_discardedTotal.fetch_add(static_cast<std::uint64_t>(tail - head), std::memory_order_relaxed);
        m_head.store(0, std::memory_order_release);
        m_tail.store(0, std::memory_order_release);
    }

    /// Producer-side "is there no room" check, used by waitForRoom().
    bool Full() const {
        if (m_fallback) {
            // The fallback has no lock-free size: a negative capacity means
            // unbounded there, so a full check only means anything when the
            // fallback queue carries a capacity of its own.
            return getSize() >= static_cast<std::size_t>(m_capacity);
        }
        return m_tail.load(std::memory_order_relaxed) - m_head.load(std::memory_order_acquire) >= m_slotCount;
    }

    /**
     * @brief Publishes @p item when a free slot is available.
     *
     * @p item is moved only on success, so a caller that retries keeps a valid
     * object. The caller must be the single producer.
     */
    bool PutIfSpace(T& item) {
        const std::size_t head = m_head.load(std::memory_order_acquire);
        const std::size_t tail = m_tail.load(std::memory_order_relaxed);
        const std::size_t size = tail - head;
        if (size >= m_slotCount) {
            return false;
        }
        new (ElementAt(tail % m_slotCount)) T(std::move(item));
        // seq_cst, not release: this store publishes the item *and* acts as the
        // StoreLoad barrier before WakeConsumer() reads the waiter flag (the
        // index line is already owned here, so the locked store is cheap), see
        // the class comment.
        m_tail.store(tail + 1, std::memory_order_seq_cst);
        if (size + 1 > m_highWaterMark.load(std::memory_order_relaxed)) {
            m_highWaterMark.store(size + 1, std::memory_order_relaxed);
        }
        return true;
    }

    /**
     * @brief Takes the head item when one is published. Caller: single consumer.
     */
    bool PopIfAvailable(T& itemRef) {
        const std::size_t head = m_head.load(std::memory_order_relaxed);
        const std::size_t tail = m_tail.load(std::memory_order_acquire);    // sees the item
        if (head == tail) {
            return false;
        }
        T* slot = ElementAt(head % m_slotCount);
        itemRef = std::move(*slot);
        slot->~T();
        // The slot is free again: publish the new head so the producer may reuse
        // it. seq_cst for the same reason as the tail in PutIfSpace(): the store
        // is also the StoreLoad barrier before WakeProducer() reads its flag.
        m_head.store(head + 1, std::memory_order_seq_cst);
        return true;
    }

    /**
     * @brief Wakes a consumer that is already waiting. Caller must not hold m_waitMutex.
     */
    void WakeConsumer() {
        // seq_cst: pairs with the waiter's seq_cst store so that a false result
        // is ordered before that store in the flag's modification order (the
        // StoreLoad barrier is the seq_cst tail store in PutIfSpace), see the
        // class comment. Reading a stale "no waiter" here loses the wake-up for
        // good, reading a stale "waiter" only costs one spurious notify.
        if (m_consumerWaiting.load(std::memory_order_seq_cst)) {
            std::lock_guard<std::mutex> lock(m_waitMutex);
            m_condNotEmpty.notify_one();
        }
    }

    /**
     * @brief Wakes a producer that is already waiting. Caller must not hold m_waitMutex.
     */
    void WakeProducer() {
        // seq_cst for the same reason as WakeConsumer().
        if (m_producerWaiting.load(std::memory_order_seq_cst)) {
            std::lock_guard<std::mutex> lock(m_waitMutex);
            m_condNotFull.notify_one();
        }
    }

    int m_capacity          = -1;
    std::size_t m_slotCount = 0;
    std::unique_ptr<Slot[]> m_slots;
    std::unique_ptr<ConcurrentQueue<T>> m_fallback;

    // Producer-owned hot line: tail moves on every push, the high-water mark is
    // only ever written by the producer (read by observers via GetStats()).
    std::atomic<std::size_t> m_tail{0};
    std::atomic<std::size_t> m_highWaterMark{0};
    char m_producerLine[kCacheLineSize - 2 * sizeof(std::atomic<std::size_t>)];

    // Consumer-owned hot line.
    std::atomic<std::size_t> m_head{0};
    char m_consumerLine[kCacheLineSize - sizeof(std::atomic<std::size_t>)];

    // Handshake flags: written by the side that is about to sleep, read by the
    // other side's wake path on every message, so each gets its own line as
    // well -- a sleeping thread must not invalidate a hot index line.
    std::atomic<bool> m_consumerWaiting{false};
    char m_consumerWaitingLine[kCacheLineSize - sizeof(std::atomic<bool>)];
    std::atomic<bool> m_producerWaiting{false};
    char m_producerWaitingLine[kCacheLineSize - sizeof(std::atomic<bool>)];

    // Cold state: touched only when a thread actually has to sleep.
    std::atomic<bool> m_shutdown{false};
    mutable std::mutex m_waitMutex;
    std::condition_variable m_condNotEmpty;
    std::condition_variable m_condNotFull;

    // Offsets folded in by restart()/clear(): the hot indices are rewound there,
    // but the published *Total values must not go backwards, so GetStats() adds
    // these. Written only when no thread is using the queue (the caller's
    // contract for both calls); read by GetStats().
    std::size_t m_pushedBase = 0;
    std::size_t m_poppedBase = 0;
    // Items clear()/restart() destroyed. Kept apart from the pushed/popped
    // indices so pushed == popped + size + discarded stays exact.
    std::atomic<std::uint64_t> m_discardedTotal{0};
};

// C++14 needs an out-of-class definition for a static constexpr data member as
// soon as it is odr-used, and std::chrono::milliseconds(kParkTimeoutMs) does that
// at -O0. Release builds constant-fold the value and link, so the omission only
// shows up as a link error in a Debug build. A member of a class template is
// exempt from the one-definition rule, so defining it in the header is safe.
template <typename T>
constexpr int SpscQueue<T>::kParkTimeoutMs;

}    // namespace nexusflow

#endif    // NEXUSFLOW_COMMON_SPSC_QUEUE_HPP
