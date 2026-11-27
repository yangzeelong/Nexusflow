// TBB flow::graph implementation of the 6-node comparison chain.
//
// Graph: input_node -> limiter(capacity) -> queue -> stage1 -> queue -> stage2
//        -> queue -> stage3 -> queue -> stage4 -> sink
// Each stage is `serial` (one message at a time), so the chain buys concurrency
// from the four stages only, exactly like the NexusFlow per-module-thread build.
//
// Note on fairness: TBB's queue_node is unbounded, so `--capacity` is enforced
// on the first edge with a limiter_node (the standard TBB backpressure pattern).
// Everything downstream is bounded implicitly by upstream throughput.

#include <atomic>
#include <cstdio>
#include <thread>

#include <tbb/flow_graph.h>
#if defined(__has_include)
#if __has_include(<tbb/version.h>)
#include <tbb/version.h>
#endif
#endif
#include <tbb/global_control.h>

#include "common.hpp"

int main(int argc, char** argv)
{
    const cmp::Options opt = cmp::ParseOptions(argc, argv);
    const cmp::TimerResolutionGuard timerGuard;
    cmp::PinToLogicalCpus(opt.cpus);

    std::atomic<bool> stop{false};
    std::atomic<bool> measuring{false};  // true only inside the measured window
    std::atomic<std::uint64_t> seq{0};
    std::atomic<std::uint64_t> delivered{0};
    std::atomic<std::int64_t> inFlight{0};  // produced minus consumed; bounds the chain
    cmp::LatencyHistogram histogram;

    tbb::global_control parallelism(tbb::global_control::max_allowed_parallelism,
                                   static_cast<std::size_t>(opt.workers));
    tbb::flow::graph graph;

    tbb::flow::queue_node<cmp::Frame> q1(graph);
    const std::size_t capacity = opt.capacity == 0 ? (1u << 20) : opt.capacity;

    // oneTBB 2021 renamed source_node to input_node; keep both buildable.
    // Backpressure: spin in the source until the chain has fewer than `capacity`
    // frames in flight (TBB's own queues are unbounded).
#if defined(TBB_INTERFACE_VERSION) && TBB_INTERFACE_VERSION >= 12000
    tbb::flow::input_node<cmp::Frame> source(graph, [&](tbb::flow_control& fc) -> cmp::Frame {
        while (!stop.load(std::memory_order_relaxed) &&
               inFlight.load(std::memory_order_relaxed) >= static_cast<std::int64_t>(capacity))
        {
            std::this_thread::yield();
        }
        if (stop.load(std::memory_order_relaxed))
        {
            fc.stop();
            return cmp::Frame{};
        }
        inFlight.fetch_add(1, std::memory_order_relaxed);
        return cmp::MakeFrame(seq.fetch_add(1, std::memory_order_relaxed));
    });
#else
    tbb::flow::source_node<cmp::Frame> source(
        graph,
        [&](cmp::Frame& out) -> bool {
            if (stop.load(std::memory_order_relaxed))
            {
                return false;
            }
            while (!stop.load(std::memory_order_relaxed) &&
                   inFlight.load(std::memory_order_relaxed) >= static_cast<std::int64_t>(capacity))
            {
                std::this_thread::yield();
            }
            inFlight.fetch_add(1, std::memory_order_relaxed);
            out = cmp::MakeFrame(seq.fetch_add(1, std::memory_order_relaxed));
            return true;
        },
        false);
#endif
    tbb::flow::queue_node<cmp::Frame> q2(graph);
    tbb::flow::queue_node<cmp::Frame> q3(graph);
    tbb::flow::queue_node<cmp::Frame> q4(graph);

    auto stage_body = [opt](std::uint64_t offset) {
        return [opt, offset](cmp::Frame frame) {
            frame.payload[offset] = cmp::Work(frame.seq + offset, opt.iters);
            if (opt.slowMs > 0)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(opt.slowMs));
            }
            return frame;
        };
    };
    tbb::flow::function_node<cmp::Frame, cmp::Frame> s1(graph, tbb::flow::serial, stage_body(1));
    tbb::flow::function_node<cmp::Frame, cmp::Frame> s2(graph, tbb::flow::serial, stage_body(2));
    tbb::flow::function_node<cmp::Frame, cmp::Frame> s3(graph, tbb::flow::serial, stage_body(3));
    tbb::flow::function_node<cmp::Frame, cmp::Frame> s4(graph, tbb::flow::serial, stage_body(4));

    tbb::flow::function_node<cmp::Frame, tbb::flow::continue_msg> sink(
        graph, tbb::flow::serial, [&](cmp::Frame frame) {
            // Only frames that reach the sink inside the window count, so the
            // warm-up and the shutdown drain are not part of the throughput.
            if (measuring.load(std::memory_order_relaxed))
            {
                histogram.Add(cmp::NowNs() - frame.stampNs);
                delivered.fetch_add(1, std::memory_order_relaxed);
            }
            inFlight.fetch_sub(1, std::memory_order_relaxed);
            return tbb::flow::continue_msg{};
        });

    tbb::flow::make_edge(source, q1);
    tbb::flow::make_edge(q1, s1);
    tbb::flow::make_edge(s1, q2);
    tbb::flow::make_edge(q2, s2);
    tbb::flow::make_edge(s2, q3);
    tbb::flow::make_edge(q3, s3);
    tbb::flow::make_edge(s3, q4);
    tbb::flow::make_edge(q4, s4);
    tbb::flow::make_edge(s4, sink);

    source.activate();

    // Warm-up: the same load for `--warmup-ms`, then the counters start clean.
    std::this_thread::sleep_for(std::chrono::milliseconds(opt.warmupMs));
    histogram.Reset();
    delivered.store(0, std::memory_order_relaxed);
    measuring.store(true, std::memory_order_relaxed);

    const double cpu0 = cmp::CpuSeconds();
    const std::uint64_t wall0 = cmp::NowNs();
    std::this_thread::sleep_for(std::chrono::duration<double>(opt.seconds));
    const std::uint64_t wall1 = cmp::NowNs();
    const double cpu1 = cmp::CpuSeconds();

    measuring.store(false, std::memory_order_relaxed);
    stop.store(true, std::memory_order_relaxed);
    graph.wait_for_all();

    const double wall = static_cast<double>(wall1 - wall0) * 1e-9;
    cmp::PrintCsv("tbb-flow", opt, wall, delivered.load(std::memory_order_relaxed), cpu1 - cpu0, histogram);
    return 0;
}
