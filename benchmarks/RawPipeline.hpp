#ifndef NEXUSFLOW_BENCHMARKS_RAW_PIPELINE_HPP
#define NEXUSFLOW_BENCHMARKS_RAW_PIPELINE_HPP

#include "BenchmarkNodes.hpp"
#include "BenchmarkTopology.hpp"
#include "LatencyHistogram.hpp"

#include <nexusflow/common/SpscQueue.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

namespace nexusflow_bench {

/// Counters a raw run exposes after Stop(), shaped like the framework's edges.
struct RawCounters {
    std::uint64_t pushed = 0;     ///< successful pushes into any queue
    std::uint64_t popped = 0;     ///< pops out of any queue
    std::uint64_t residual = 0;   ///< still queued after Stop(); must be 0
    std::uint64_t sinkPopped = 0; ///< pops out of the queue that feeds the sink
    std::uint64_t delivered = 0;  ///< messages the sink processed
};

/**
 * @brief Raw message: a plain struct that travels inside the queue.
 *
 * No heap allocation per message, no metadata, no type erasure. This is a lower
 * bound on what any framework can cost; the gap to `RawBoxedFactory` is what the
 * per-message wrapper allocation costs on its own.
 */
struct RawInlineFactory {
    using Message = BenchmarkPayload;

    RawInlineFactory(PayloadSpec spec, bool freshPerMessage, bool measureLatency = true)
        : m_spec(spec), m_fresh(freshPerMessage), m_measure(measureLatency) {}

    Message Make() {
        Message message;
        if (m_measure) {
            message.creationTime = Clock::now();
        }
        if (!m_spec.empty()) {
            if (m_fresh) {
                message.frame = MakeBenchmarkFrame(m_spec);
            } else {
                if (!m_sharedFrame) {
                    m_sharedFrame = MakeBenchmarkFrame(m_spec);
                }
                message.frame = m_sharedFrame;
            }
        }
        return message;
    }

    static Clock::time_point CreatedAt(const Message& message) { return message.creationTime; }

    PayloadSpec m_spec;
    bool m_fresh = false;
    bool m_measure = true;
    std::shared_ptr<const BenchmarkFrame> m_sharedFrame;
};

/**
 * @brief Raw message that is heap-allocated per message.
 *
 * The framework builds every message as a heap-allocated payload wrapper
 * (`MakeMessage`) and copies a shared_ptr from hop to hop; this factory does the
 * same, so comparing the framework against *this* baseline prices the framework's
 * per-hop work without also charging it for the allocation.
 */
struct RawBoxedFactory {
    using Message = std::shared_ptr<const BenchmarkPayload>;

    RawBoxedFactory(PayloadSpec spec, bool freshPerMessage, bool measureLatency = true)
        : m_spec(spec), m_fresh(freshPerMessage), m_measure(measureLatency) {}

    Message Make() {
        auto payload = std::make_shared<BenchmarkPayload>();
        if (m_measure) {
            payload->creationTime = Clock::now();
        }
        if (!m_spec.empty()) {
            if (m_fresh) {
                payload->frame = MakeBenchmarkFrame(m_spec);
            } else {
                if (!m_sharedFrame) {
                    m_sharedFrame = MakeBenchmarkFrame(m_spec);
                }
                payload->frame = m_sharedFrame;
            }
        }
        return payload;
    }

    static Clock::time_point CreatedAt(const Message& message) { return message->creationTime; }

    PayloadSpec m_spec;
    bool m_fresh = false;
    bool m_measure = true;
    std::shared_ptr<const BenchmarkFrame> m_sharedFrame;
};

/**
 * @brief Raw message that is the framework's own `nexusflow::Message`.
 *
 * The framework's edge queue is `SpscQueue<Message>`, so this factory keeps both
 * the queue and the message layer identical and leaves only Module / Worker /
 * Dispatcher as the difference. Comparing against it is what splits the
 * framework's cost into "the Message itself" (allocation, metadata, type
 * erasure, COW) and "the dispatch around it".
 */
struct RawMessageFactory {
    using Message = nexusflow::Message;

    RawMessageFactory(PayloadSpec spec, bool freshPerMessage, bool measureLatency = true)
        : m_spec(spec), m_fresh(freshPerMessage), m_measure(measureLatency) {}

    Message Make() {
        BenchmarkPayload payload;
        if (m_measure) {
            payload.creationTime = Clock::now();
        }
        if (!m_spec.empty()) {
            if (m_fresh) {
                payload.frame = MakeBenchmarkFrame(m_spec);
            } else {
                if (!m_sharedFrame) {
                    m_sharedFrame = MakeBenchmarkFrame(m_spec);
                }
                payload.frame = m_sharedFrame;
            }
        }
        return nexusflow::MakeMessage(payload);
    }

    static Clock::time_point CreatedAt(const Message& message) {
        const BenchmarkPayload* payload = message.BorrowPtr<BenchmarkPayload>();
        return payload != nullptr ? payload->creationTime : Clock::now();
    }

    PayloadSpec m_spec;
    bool m_fresh = false;
    bool m_measure = true;
    std::shared_ptr<const BenchmarkFrame> m_sharedFrame;
};

/**
 * @class RawChain
 * @brief A pipeline of the requested shape built from std::thread and SpscQueue.
 *
 * This is the baseline HARNESS.md §3 asks for: same stage count, one thread per
 * stage, same queue depth, same blocking hand-off, one copy per hop. What the
 * framework costs per stage is whatever it costs *above* this line.
 *
 * Two message flavours, chosen by the factory:
 *   - `RawInlineFactory`: the message is a plain struct, so nothing is allocated
 *     per message -- the lower bound;
 *   - `RawBoxedFactory`: the message is a heap-allocated payload wrapper, the way
 *     the framework builds one -- so the gap between the two flavours is the
 *     allocation, and the gap from there to the framework is its dispatch work.
 *
 * Neither flavour batches (one message per hand-off), so a like-for-like
 * comparison runs the framework with `--batch 1`.
 */
template <typename Factory>
class RawChain {
public:
    using Message = typename Factory::Message;

    RawChain(const TopologyShape& shape, std::size_t capacity, Factory factory, bool measureLatency = true)
        : m_factory(std::move(factory)), m_measure(measureLatency) {
        m_stages = shape.modules < 2 ? 2u : static_cast<std::size_t>(shape.modules);
        const std::size_t edges = m_stages - 1;
        m_edges.reserve(edges);
        for (std::size_t i = 0; i < edges; ++i) {
            m_edges.push_back(std::unique_ptr<nexusflow::SpscQueue<Message>>(
                new nexusflow::SpscQueue<Message>(static_cast<int>(capacity))));
        }
        m_pushed.assign(edges, 0);
        m_popped.assign(edges, 0);
    }

    ~RawChain() { Stop(); }

    RawChain(const RawChain&) = delete;
    RawChain& operator=(const RawChain&) = delete;

    void Start() {
        if (m_started) {
            return;
        }
        m_stop.store(false, std::memory_order_relaxed);
        m_threads.reserve(m_stages);
        m_threads.emplace_back(&RawChain::RunSource, this);
        for (std::size_t stage = 1; stage + 1 < m_stages; ++stage) {
            m_threads.emplace_back(&RawChain::RunStage, this, stage - 1);
        }
        m_threads.emplace_back(&RawChain::RunSink, this);
        m_started = true;
    }

    void Stop() {
        if (!m_started) {
            return;
        }
        m_stop.store(true, std::memory_order_relaxed);
        for (std::size_t i = 0; i < m_threads.size(); ++i) {
            if (m_threads[i].joinable()) {
                m_threads[i].join();
            }
            // Closing the queue in this order is what makes the run drain: the
            // stage reading it is still alive and exits only once it is empty.
            if (i < m_edges.size()) {
                m_edges[i]->shutdown();
            }
        }
        m_threads.clear();
        m_started = false;
    }

    /// Only valid after Stop(): every counter here is written by a live thread.
    RawCounters TakeSnapshot() const {
        RawCounters counters;
        for (std::size_t i = 0; i < m_edges.size(); ++i) {
            counters.pushed += m_pushed[i];
            counters.popped += m_popped[i];
            counters.residual += m_edges[i]->getSize();
        }
        counters.sinkPopped = m_edges.empty() ? 0 : m_popped[m_edges.size() - 1];
        counters.delivered = m_delivered;
        return counters;
    }

    const LatencyHistogram& Latency() const { return m_latency; }

private:
    void RunSource() {
        while (!m_stop.load(std::memory_order_relaxed)) {
            Message message = m_factory.Make();
            if (!m_edges[0]->push(std::move(message))) {
                return;
            }
            ++m_pushed[0];
        }
    }

    /// One pass-through stage: pop from `edge`, push into `edge + 1`. The push
    /// copies the message, which is what the framework's Broadcast does too.
    void RunStage(std::size_t edge) {
        Message message;
        while (m_edges[edge]->waitAndPop(message)) {
            ++m_popped[edge];
            if (!m_edges[edge + 1]->push(message)) {
                return;
            }
            ++m_pushed[edge + 1];
        }
    }

    void RunSink() {
        const std::size_t edge = m_edges.size() - 1;
        Message message;
        while (m_edges[edge]->waitAndPop(message)) {
            ++m_popped[edge];
            ++m_delivered;
            // `--observe none` skips the second clock read and the histogram
            // sample: they are the harness's cost, not the pipeline's.
            if (m_measure) {
                const auto latency = Clock::now() - Factory::CreatedAt(message);
                const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(latency).count();
                m_latency.Record(ns > 0 ? static_cast<std::uint64_t>(ns) : 0);
            }
        }
    }

    std::size_t m_stages = 0;
    bool m_started = false;
    bool m_measure = true;
    Factory m_factory;
    std::atomic<bool> m_stop{false};
    std::vector<std::unique_ptr<nexusflow::SpscQueue<Message>>> m_edges;
    std::vector<std::thread> m_threads;
    /// One writer per entry (edge i is written by stage i and read by stage i+1),
    /// which is why plain integers are enough: they are only read after Stop().
    std::vector<std::uint64_t> m_pushed;
    std::vector<std::uint64_t> m_popped;
    LatencyHistogram m_latency;
    std::uint64_t m_delivered = 0;
};

} // namespace nexusflow_bench

#endif // NEXUSFLOW_BENCHMARKS_RAW_PIPELINE_HPP
