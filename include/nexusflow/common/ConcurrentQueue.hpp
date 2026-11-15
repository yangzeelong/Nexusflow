#ifndef NEXUSFLOW_COMMON_CONCURRENT_QUEUE_HPP_
#define NEXUSFLOW_COMMON_CONCURRENT_QUEUE_HPP_

#include <nexusflow/common/Optional.hpp>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <queue>

/**
 * @class ConcurrentQueue
 * @brief A thread-safe, blocking queue for producer-consumer scenarios.
 *
 * This queue can be configured as bounded (with a fixed capacity) or unbounded.
 * It uses condition variables to block producing threads when the queue is full
 * and consuming threads when the queue is empty, preventing busy-waiting.
 *
 * @tparam T The type of elements stored in the queue.
 */
template <typename T>
class ConcurrentQueue {
public:
    /**
     * @brief Constructs a ConcurrentQueue.
     * @param capacity The maximum capacity of the queue. A value of -1 (default)
     *                 indicates an unbounded queue.
     */
    explicit ConcurrentQueue(int capacity = -1) : m_capacity(capacity), m_shutdown(false) {}

    /**
     * @brief Snapshot of the queue size and its lifetime counters.
     */
    struct Stats {
        std::size_t size = 0;
        std::size_t capacity = 0; // 0 means unbounded
        std::size_t highWaterMark = 0;
        std::uint64_t pushed = 0;
        std::uint64_t popped = 0;
    };

    /**
     * @brief Returns a consistent snapshot, sampled under the queue lock.
     */
    Stats GetStats() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        Stats stats;
        stats.size = m_queue.size();
        stats.capacity = m_capacity < 0 ? 0 : static_cast<std::size_t>(m_capacity);
        stats.highWaterMark = m_highWaterMark;
        stats.pushed = m_pushed;
        stats.popped = m_popped;
        return stats;
    }

    /**
     * @brief Forgets the deepest backlog seen so far.
     * @details Only the water mark restarts; pushed/popped keep growing so two
     * snapshots are still subtractable.
     */
    void resetHighWaterMark() {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_highWaterMark = m_queue.size();
    }

    // Disable copy and move semantics to ensure a single owner.
    ConcurrentQueue(const ConcurrentQueue&) = delete;
    ConcurrentQueue& operator=(const ConcurrentQueue&) = delete;

    /**
     * @brief Pushes an item into the queue (blocking).
     * If the queue is bounded and full, this call will block until space becomes available
     * or the queue is shut down.
     * @param item The item to be pushed (rvalue reference for move semantics).
     * @return true if the item was successfully pushed, false if the queue has been shut down.
     */
    bool push(T&& item) {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_condNotFull.wait(lock, [this] { return m_shutdown || !isFull(); });

        if (m_shutdown) {
            return false;
        }

        PushLocked(std::move(item));
        return true;
    }

    /**
     * @brief Pushes an item into the queue (lvalue reference overload).
     */
    bool push(const T& item) {
        T temp = item;
        return push(std::move(temp));
    }

    /**
     * @brief Tries to push an item, waiting up to a specified timeout.
     * @param item The item to push.
     * @param timeout The maximum duration to wait.
     * @return true if pushed, false if timed out or shutdown.
     */
    template <class Rep, class Per>
    bool pushFor(T&& item, const std::chrono::duration<Rep, Per>& timeout) {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (!m_condNotFull.wait_for(lock, timeout, [this] { return m_shutdown || !isFull(); })) {
            // wait_for returned false, meaning it timed out.
            return false;
        }
        if (m_shutdown) return false;
        PushLocked(std::move(item));
        return true;
    }

    template <class Rep, class Per>
    bool pushFor(const T& item, const std::chrono::duration<Rep, Per>& timeout) {
        return pushFor(T(item), timeout);
    }

    /**
     * @brief Tries to push an item into the queue without blocking.
     * If the queue is bounded and currently full, this method will return immediately
     * with false instead of waiting for space.
     * @param item The item to be pushed (rvalue reference for move semantics).
     * @return true if the item was successfully pushed, false if the queue was full or has been shut down.
     */
    bool tryPush(T&& item) {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_shutdown || isFull()) {
            return false;
        }

        PushLocked(std::move(item));
        return true;
    }

    /**
     * @brief Tries to push an item into the queue without blocking (lvalue reference overload).
     */
    bool tryPush(const T& item) {
        T temp = item;
        return tryPush(std::move(temp));
    }

    /**
     * @brief Tries to pop an item, waiting up to a specified timeout.
     * @param itemRef Reference to store the popped item.
     * @param timeout The maximum duration to wait.
     * @return true if popped, false if timed out or shutdown.
     */
    template <class Rep, class Per>
    bool waitAndPopFor(T& itemRef, const std::chrono::duration<Rep, Per>& timeout) {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (!m_condNotEmpty.wait_for(lock, timeout, [this] { return m_shutdown || !m_queue.empty(); })) {
            // wait_for returned false, meaning it timed out.
            return false;
        }
        if (m_shutdown && m_queue.empty()) return false;
        itemRef = PopLocked();
        return true;
    }

    /**
     * @brief Waits for and pops an item from the queue (blocking).
     * If the queue is empty, this call will block until an item becomes available
     * or the queue is shut down.
     * @param itemRef A reference to store the popped item.
     * @return true if an item was successfully popped, false if the queue is empty and has been shut down.
     */
    bool waitAndPop(T& itemRef) {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_condNotEmpty.wait(lock, [this] { return m_shutdown || !m_queue.empty(); });

        if (m_shutdown && m_queue.empty()) {
            return false;
        }

        itemRef = PopLocked();
        return true;
    }

    /**
     * @brief Tries to pop an item from the queue without blocking.
     * @return An Optional holding the item if the queue was not empty,
     *         otherwise nullOpt.
     */
    Optional<T> tryPop() {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_queue.empty()) {
            return nullOpt;
        }
        return PopLocked();
    }

    /**
     * @brief Tries to pop an item from the queue without blocking.
     * @param itemRef Reference to store the popped item (rvalue reference for move semantics).
     * @return true if an item was successfully popped, false if the queue was empty or has been shut down.
     */
    bool tryPop(T& itemRef) {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_queue.empty()) {
            return false;
        }
        itemRef = PopLocked();
        return true;
    }

    /**
     * @brief Shuts down the queue.
     * This will wake up all waiting producer and consumer threads.
     */
    void shutdown() {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_shutdown = true;
        m_condNotEmpty.notify_all();
        m_condNotFull.notify_all();
    }

    /**
     * @brief Reopens a shut-down queue, discarding anything still buffered.
     * Only safe while no thread is using the queue.
     */
    void restart() {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_shutdown = false;
        std::queue<T> empty;
        m_queue.swap(empty);
    }

    /**
     * @brief Checks if the queue is currently empty.
     */
    bool isEmpty() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_queue.empty();
    }

    /**
     * @brief True once shutdown() was called; no further pushes are accepted.
     */
    bool isShutdown() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_shutdown;
    }

    /**
     * @brief Returns the current number of items in the queue.
     */
    size_t getSize() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_queue.size();
    }

private:
    /**
     * @brief Checks if the queue is full. Must be called while holding the lock.
     */
    bool isFull() const {
        if (m_capacity == -1) {
            return false;
        }
        return m_queue.size() >= static_cast<size_t>(m_capacity);
    }

    /**
     * @brief Pushes an item and updates the counters. Caller must hold m_mutex.
     */
    void PushLocked(T&& item) {
        m_queue.push(std::move(item));
        ++m_pushed;
        if (m_queue.size() > m_highWaterMark) {
            m_highWaterMark = m_queue.size();
        }
        m_condNotEmpty.notify_one();
    }

    /**
     * @brief Pops an item and updates the counters. Caller must hold m_mutex.
     */
    T PopLocked() {
        T item = std::move(m_queue.front());
        m_queue.pop();
        ++m_popped;
        m_condNotFull.notify_one();
        return item;
    }

private:
    mutable std::mutex m_mutex;
    std::condition_variable m_condNotEmpty;
    std::condition_variable m_condNotFull;
    std::queue<T> m_queue;
    std::uint64_t m_pushed = 0;
    std::uint64_t m_popped = 0;
    std::size_t m_highWaterMark = 0;
    const int m_capacity;
    bool m_shutdown;
};

#endif // CONCURRENT_QUEUE_HPP