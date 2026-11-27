// A hand-written five-stage pipeline. No framework, no graph, no allocator on
// the hot path: one thread per stage, one bounded SPSC ring per edge, and a
// 64-byte frame copied by value. This is the lower bound the frameworks are
// measured against -- the smallest amount of machinery that still runs the same
// graph with the same ordering and blocking behaviour.

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <thread>
#include <vector>

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#endif

#include "common.hpp"

namespace
{

/// Bounded single-producer single-consumer ring. Full means "try again", never
/// "drop": this is the blocking behaviour the graph wants.
class SpscRing
{
public:
    explicit SpscRing(std::size_t capacity) : m_capacity(capacity == 0 ? 1 : capacity), m_slots(new cmp::Frame[m_capacity]) {}

    bool TryPush(const cmp::Frame& frame)
    {
        const std::size_t write = m_write.load(std::memory_order_relaxed);
        if (write - m_read.load(std::memory_order_acquire) >= m_capacity)
        {
            return false;
        }
        m_slots[write % m_capacity] = frame;
        m_write.store(write + 1, std::memory_order_release);
        return true;
    }

    bool TryPop(cmp::Frame& frame)
    {
        const std::size_t read = m_read.load(std::memory_order_relaxed);
        if (read == m_write.load(std::memory_order_acquire))
        {
            return false;
        }
        frame = m_slots[read % m_capacity];
        m_read.store(read + 1, std::memory_order_release);
        return true;
    }

private:
    std::size_t m_capacity;
    std::unique_ptr<cmp::Frame[]> m_slots;
    alignas(64) std::atomic<std::size_t> m_write{0};
    alignas(64) std::atomic<std::size_t> m_read{0};
};

void SleepMs(int ms)
{
    if (ms <= 0)
    {
        return;
    }
#if defined(_WIN32)
    ::Sleep(static_cast<DWORD>(ms));
#else
    struct timespec ts;
    ts.tv_sec = ms / 1000;
    ts.tv_nsec = static_cast<long>(ms % 1000) * 1000000L;
    nanosleep(&ts, nullptr);
#endif
}

/// Push with backpressure. Returns false only if the consumer vanished, which
/// cannot happen here, so the caller may ignore the return value.
bool PushBlocking(SpscRing& ring, const cmp::Frame& frame)
{
    unsigned spins = 0;
    while (!ring.TryPush(frame))
    {
        if (++spins < 64)
        {
            continue;
        }
        std::this_thread::yield();
    }
    return true;
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

    const std::size_t capacity = opt.capacity == 0 ? 4096 : opt.capacity;
    std::vector<std::unique_ptr<SpscRing>> edges;
    edges.reserve(5);
    for (int i = 0; i < 5; ++i)
    {
        edges.emplace_back(new SpscRing(capacity));
    }

    std::atomic<bool> stop{false};
    std::atomic<bool> measuring{false};
    std::atomic<std::uint64_t> windowStartNs{0};
    std::atomic<std::uint64_t> windowEndNs{0};
    std::atomic<double> cpuAtWindowStart{0.0};
    std::atomic<std::uint64_t> delivered{0};
    cmp::LatencyHistogram histogram;

    std::thread producer([&]() {
        const std::uint64_t warmupNs = static_cast<std::uint64_t>(opt.warmupMs) * 1000000ull;
        const std::uint64_t targetNs = static_cast<std::uint64_t>(opt.seconds * 1e9);
        std::uint64_t seq = 0;

        // Warm-up: the same graph, the same load, nothing counted. Keeps the
        // first-second page faults and thread start-up out of the window.
        const std::uint64_t processStart = cmp::NowNs();
        while (cmp::NowNs() - processStart < warmupNs)
        {
            PushBlocking(*edges[0], cmp::MakeFrame(seq++));
        }

        const std::uint64_t windowStart = cmp::NowNs();
        cpuAtWindowStart.store(cmp::CpuSeconds(), std::memory_order_relaxed);
        windowStartNs.store(windowStart, std::memory_order_release);
        measuring.store(true, std::memory_order_release);
        while (cmp::NowNs() - windowStart < targetNs)
        {
            PushBlocking(*edges[0], cmp::MakeFrame(seq++));
        }
        // The measured window ends when the producer stops, not when the last
        // stage has drained its ring: draining is shutdown cost, and counting it
        // as throughput would make a 2 s run look 3x slower than it is.
        windowEndNs.store(cmp::NowNs(), std::memory_order_release);
        measuring.store(false, std::memory_order_release);
        stop.store(true, std::memory_order_release);
    });

    std::vector<std::thread> stages;
    stages.reserve(4);
    for (int i = 0; i < 4; ++i)
    {
        stages.emplace_back([&, i]() {
            SpscRing& in = *edges[i];
            SpscRing& out = *edges[i + 1];
            std::uint64_t stopSeenNs = 0;
            for (;;)
            {
                cmp::Frame frame;
                if (!in.TryPop(frame))
                {
                    if (stop.load(std::memory_order_acquire))
                    {
                        // Upstream may still be draining its own ring; wait for
                        // it, but do not wait forever if it is already gone.
                        if (stopSeenNs == 0)
                        {
                            stopSeenNs = cmp::NowNs();
                        }
                        else if (cmp::NowNs() - stopSeenNs > 2000000000ull)
                        {
                            break;
                        }
                    }
                    std::this_thread::yield();
                    continue;
                }
                stopSeenNs = 0;
                frame.payload[0] = cmp::Work(frame.seq, opt.iters);
                SleepMs(opt.slowMs);
                PushBlocking(out, frame);
            }
        });
    }

    std::thread sink([&]() {
        SpscRing& in = *edges[4];
        std::uint64_t stopSeenNs = 0;
        for (;;)
        {
            cmp::Frame frame;
            if (!in.TryPop(frame))
            {
                if (stop.load(std::memory_order_acquire))
                {
                    if (stopSeenNs == 0)
                    {
                        stopSeenNs = cmp::NowNs();
                    }
                    else if (cmp::NowNs() - stopSeenNs > 2000000000ull)
                    {
                        break;
                    }
                }
                std::this_thread::yield();
                continue;
            }
            stopSeenNs = 0;
            // Only frames that reach the sink inside the window count; the
            // drain after `stop` is shutdown cost.
            if (measuring.load(std::memory_order_relaxed))
            {
                ++delivered;
                histogram.Add(cmp::NowNs() - frame.stampNs);
            }
        }
    });

    producer.join();
    for (auto& thread : stages)
    {
        thread.join();
    }
    sink.join();

    const std::uint64_t windowStart = windowStartNs.load(std::memory_order_relaxed);
    const std::uint64_t windowEnd = windowEndNs.load(std::memory_order_relaxed);
    const double wallSeconds = windowEnd > windowStart ? static_cast<double>(windowEnd - windowStart) / 1e9 : 0.0;
    const double cpuSeconds = cmp::CpuSeconds() - cpuAtWindowStart.load(std::memory_order_relaxed);
    cmp::PrintCsv("raw", opt, wallSeconds, delivered.load(), cpuSeconds, histogram);
    return 0;
}
