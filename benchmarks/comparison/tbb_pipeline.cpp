// TBB `parallel_pipeline`: a thread-pipeline API, not a dataflow graph.
//
// Five filters form one chain (source -> four work stages -> sink). Every filter
// is serial_in_order, which is the same ordering the other implementations
// enforce, and `max_live_tokens` bounds how many frames are in flight. There is
// no queue node and no graph: the pipeline scheduler moves tokens between
// filters, so the cost per stage is the scheduler's, not a queue's.

#include <atomic>
#include <cstdint>
#include <cstdio>

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#endif

#include "common.hpp"
#include <tbb/global_control.h>
#include <tbb/tbb.h>

#if defined(TBB_INTERFACE_VERSION) && TBB_INTERFACE_VERSION >= 12000
#define CMP_SERIAL tbb::filter_mode::serial_in_order
#else
#define CMP_SERIAL tbb::filter::serial_in_order
#endif

namespace
{

/// The same four work stages every implementation runs: `--iters` rounds of the
/// shared Work() arithmetic, plus the optional slow sleep.
cmp::Frame WorkStage(const cmp::Frame& in, const cmp::Options& opt)
{
    cmp::Frame out = in;
    out.payload[0] = cmp::Work(out.seq, opt.iters);
    if (opt.slowMs > 0)
    {
#if defined(_WIN32)
        ::Sleep(static_cast<DWORD>(opt.slowMs));
#else
        struct timespec ts;
        ts.tv_sec = opt.slowMs / 1000;
        ts.tv_nsec = static_cast<long>(opt.slowMs % 1000) * 1000000L;
        nanosleep(&ts, nullptr);
#endif
    }
    return out;
}

}  // namespace

int main(int argc, char** argv)
{
    const cmp::Options opt = cmp::ParseOptions(argc, argv);
    if (opt.cpus > 0)
    {
        cmp::PinToLogicalCpus(opt.cpus);
    }
    cmp::TimerResolutionGuard timerGuard;

    // Same concurrency budget as the other implementations: without this the
    // arena defaults to every logical CPU, which is not the four-worker shape
    // this comparison is about.
    tbb::global_control parallelism(tbb::global_control::max_allowed_parallelism,
                                   static_cast<std::size_t>(opt.workers > 0 ? opt.workers : 1));

    std::atomic<std::uint64_t> nextSeq{0};
    std::atomic<bool> measuring{false};
    std::atomic<std::uint64_t> windowStartNs{0};
    std::atomic<std::uint64_t> windowEndNs{0};
    std::atomic<double> cpuAtWindowStart{0.0};
    const std::uint64_t processStartNs = cmp::NowNs();
    const std::uint64_t warmupNs = static_cast<std::uint64_t>(opt.warmupMs) * 1000000ull;
    const std::uint64_t targetNs = static_cast<std::uint64_t>(opt.seconds * 1e9);

    std::uint64_t delivered = 0;
    cmp::LatencyHistogram histogram;

    // `--capacity` bounds how much work may be in flight, the same bound the
    // other implementations enforce on their queues; `--workers` only sets the
    // concurrency of the arena.
    const std::size_t tokens = opt.capacity == 0 ? 1024u : opt.capacity;

    const auto stage1 = [&opt](const cmp::Frame& f) { return WorkStage(f, opt); };
    const auto stage2 = [&opt](const cmp::Frame& f) { return WorkStage(f, opt); };
    const auto stage3 = [&opt](const cmp::Frame& f) { return WorkStage(f, opt); };
    const auto stage4 = [&opt](const cmp::Frame& f) { return WorkStage(f, opt); };

    tbb::parallel_pipeline(
        tokens,
        tbb::make_filter<void, cmp::Frame>(CMP_SERIAL, [&](tbb::flow_control& fc) -> cmp::Frame {
            const std::uint64_t now = cmp::NowNs();
            std::uint64_t start = windowStartNs.load(std::memory_order_relaxed);
            if (start == 0)
            {
                // Warm-up frames are produced but not counted anywhere.
                if (now - processStartNs < warmupNs)
                {
                    return cmp::MakeFrame(nextSeq.fetch_add(1, std::memory_order_relaxed));
                }
                cpuAtWindowStart.store(cmp::CpuSeconds(), std::memory_order_relaxed);
                windowStartNs.store(now, std::memory_order_release);
                measuring.store(true, std::memory_order_release);
                start = now;
            }
            if (now - start >= targetNs)
            {
                measuring.store(false, std::memory_order_release);
                windowEndNs.store(now, std::memory_order_release);
                fc.stop();
                return cmp::Frame{};
            }
            return cmp::MakeFrame(nextSeq.fetch_add(1, std::memory_order_relaxed));
        }) &
            tbb::make_filter<cmp::Frame, cmp::Frame>(CMP_SERIAL, stage1) &
            tbb::make_filter<cmp::Frame, cmp::Frame>(CMP_SERIAL, stage2) &
            tbb::make_filter<cmp::Frame, cmp::Frame>(CMP_SERIAL, stage3) &
            tbb::make_filter<cmp::Frame, cmp::Frame>(CMP_SERIAL, stage4) &
            tbb::make_filter<cmp::Frame, void>(CMP_SERIAL, [&](const cmp::Frame& f) {
                if (measuring.load(std::memory_order_relaxed))
                {
                    ++delivered;
                    histogram.Add(cmp::NowNs() - f.stampNs);
                }
            }));

    const std::uint64_t windowStart = windowStartNs.load(std::memory_order_relaxed);
    const std::uint64_t windowEnd = windowEndNs.load(std::memory_order_relaxed);
    const double wallSeconds = windowEnd > windowStart ? static_cast<double>(windowEnd - windowStart) / 1e9 : 0.0;
    const double cpuSeconds = cmp::CpuSeconds() - cpuAtWindowStart.load(std::memory_order_relaxed);
    cmp::PrintCsv("tbb-pipeline", opt, wallSeconds, delivered, cpuSeconds, histogram);
    return 0;
}
