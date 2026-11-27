// Shared measurement helpers for the cross-framework comparison.
//
// One message ("frame") is a 64-byte record carrying a sequence number and the
// timestamp taken when it was produced. Every implementation runs the same
// four processing stages, each doing `--iters` rounds of the same integer work,
// and reports the same numbers: delivered frames / wall second, process CPU
// seconds, and a latency histogram read at the sink.
//
// Not part of the library build: these are standalone programs with their own
// build scripts (`build_wsl.sh`, `build_win.bat`).

#pragma once

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <mmsystem.h>
#pragma comment(lib, "winmm.lib")
#include <psapi.h>
#pragma comment(lib, "psapi.lib")
#else
#include <sys/resource.h>
#endif

namespace cmp
{

using Clock = std::chrono::steady_clock;

/// Windows rounds a sleep up to the current timer tick (15.6 ms by default),
/// while the NexusFlow harness raises it to 1 ms for its lifetime. Without the
/// same guard a 2 ms slow stage means 15.6 ms here and the two are not
/// comparable.
class TimerResolutionGuard
{
public:
    TimerResolutionGuard()
    {
#if defined(_WIN32)
        timeBeginPeriod(1);
#endif
    }
    ~TimerResolutionGuard()
    {
#if defined(_WIN32)
        timeEndPeriod(1);
#endif
    }
    TimerResolutionGuard(const TimerResolutionGuard&) = delete;
    TimerResolutionGuard& operator=(const TimerResolutionGuard&) = delete;
};

inline std::uint64_t NowNs()
{
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count());
}

/// Process CPU seconds (user + kernel), the same quantity `pipeline_benchmark` reports.
inline double CpuSeconds()
{
#if defined(_WIN32)
    FILETIME created{};
    FILETIME exited{};
    FILETIME kernel{};
    FILETIME user{};
    if (!GetProcessTimes(GetCurrentProcess(), &created, &exited, &kernel, &user))
    {
        return 0.0;
    }
    auto toSeconds = [](const FILETIME& t) {
        const std::uint64_t ticks = (static_cast<std::uint64_t>(t.dwHighDateTime) << 32) | t.dwLowDateTime;
        return static_cast<double>(ticks) * 1e-7;  // 100 ns ticks
    };
    return toSeconds(kernel) + toSeconds(user);
#else
    rusage usage{};
    if (getrusage(RUSAGE_SELF, &usage) != 0)
    {
        return 0.0;
    }
    return static_cast<double>(usage.ru_utime.tv_sec) + static_cast<double>(usage.ru_stime.tv_sec) +
           1e-6 * (static_cast<double>(usage.ru_utime.tv_usec) + static_cast<double>(usage.ru_stime.tv_usec));
#endif
}

/// One message. 64 bytes, so an implementation that copies it pays the same cost.
struct Frame
{
    std::uint64_t seq = 0;
    std::uint64_t stampNs = 0;
    std::uint64_t payload[6] = {0, 0, 0, 0, 0, 0};
};

/// Deliberately the same arithmetic as NexusFlow's Compute workload
/// (benchmarks/BenchmarkNodes.hpp, RunCompute): a serial sin/cos chain per
/// stage. Keeping the arithmetic identical is what lets "500 iterations" mean
/// the same thing in all three frameworks.
inline std::uint64_t Work(std::uint64_t seed, int iterations)
{
    double value = static_cast<double>(seed % 97u) * 0.01;
    for (int i = 0; i < iterations; ++i)
    {
        const double x = static_cast<double>(i) * 0.001;
        value += std::sin(x) * std::cos(x);
    }
    std::uint64_t bits = 0;
    std::memcpy(&bits, &value, sizeof(bits));
    return bits;
}

inline Frame MakeFrame(std::uint64_t seq)
{
    Frame f;
    f.seq = seq;
    f.stampNs = NowNs();
    f.payload[0] = Work(seq, 4);
    return f;
}

/// 1 µs buckets up to 100 ms; enough for latency work and cheap to fill under load.
class LatencyHistogram
{
public:
    static constexpr std::size_t kBuckets = 100000;

    void Add(std::uint64_t ns)
    {
        std::size_t idx = static_cast<std::size_t>(ns / 1000);
        if (idx >= kBuckets)
        {
            idx = kBuckets - 1;
        }
        ++m_buckets[idx];
        ++m_count;
    }

    std::uint64_t Percentile(double p) const
    {
        if (m_count == 0)
        {
            return 0;
        }
        const std::uint64_t target = static_cast<std::uint64_t>(p * static_cast<double>(m_count - 1));
        std::uint64_t seen = 0;
        for (std::size_t i = 0; i < kBuckets; ++i)
        {
            seen += m_buckets[i];
            if (seen > target)
            {
                return static_cast<std::uint64_t>(i) * 1000;  // bucket -> ns
            }
        }
        return 0;
    }

    std::uint64_t Count() const { return m_count; }

    void Reset()
    {
        std::fill(m_buckets.begin(), m_buckets.end(), 0);
        m_count = 0;
    }

private:
    std::vector<std::uint64_t> m_buckets = std::vector<std::uint64_t>(kBuckets, 0);
    std::uint64_t m_count = 0;
};

struct Options
{
    int workers = 4;       ///< concurrency: threads (TBB arena, Taskflow) / pool size
    std::size_t capacity = 64;  ///< per-edge queue capacity (0 = unbounded)
    double seconds = 5.0;  ///< measured window
    int warmupMs = 1000;   ///< unmeasured run-in before the window opens
    int iters = 500;       ///< integer work per message per stage
    int cpus = 0;          ///< pin to the first N logical CPUs (0 = leave it alone)
    int slowMs = 0;        ///< extra sleep in every work stage, to make the consumer slower than the source
    std::string label;
};

/// Pin the process to the first N logical CPUs, the same mask `pipeline_benchmark --cpus` uses.
inline void PinToLogicalCpus(int count)
{
    if (count <= 0)
    {
        return;
    }
#if defined(_WIN32)
    const DWORD_PTR mask = count >= 64 ? ~static_cast<DWORD_PTR>(0) : ((static_cast<DWORD_PTR>(1) << count) - 1);
    SetProcessAffinityMask(GetCurrentProcess(), mask);
#else
    cpu_set_t set;
    CPU_ZERO(&set);
    for (int i = 0; i < count && i < CPU_SETSIZE; ++i)
    {
        CPU_SET(i, &set);
    }
    sched_setaffinity(0, sizeof(set), &set);
#endif
}

inline Options ParseOptions(int argc, char** argv)
{
    Options opt;
    for (int i = 1; i + 1 < argc; i += 2)
    {
        const std::string key = argv[i];
        const std::string value = argv[i + 1];
        if (key == "--workers")
        {
            opt.workers = std::atoi(value.c_str());
        }
        else if (key == "--capacity")
        {
            opt.capacity = static_cast<std::size_t>(std::strtoull(value.c_str(), nullptr, 10));
        }
        else if (key == "--seconds")
        {
            opt.seconds = std::atof(value.c_str());
        }
        else if (key == "--slow-ms")
        {
            opt.slowMs = std::atoi(value.c_str());
        }
        else if (key == "--warmup-ms")
        {
            opt.warmupMs = std::atoi(value.c_str());
        }
        else if (key == "--iters")
        {
            opt.iters = std::atoi(value.c_str());
        }
        else if (key == "--cpus")
        {
            opt.cpus = std::atoi(value.c_str());
        }
        else if (key == "--label")
        {
            opt.label = value;
        }
    }
    return opt;
}

/// One CSV row per run, prefixed so a `grep '^cmp,'` collection stays simple.
/// Whole-process resident memory, so a framework that buffers without bound shows up in the CSV.
inline double ResidentMiB()
{
#if defined(_WIN32)
    PROCESS_MEMORY_COUNTERS counters{};
    if (GetProcessMemoryInfo(GetCurrentProcess(), &counters, sizeof(counters)) == 0)
    {
        return 0.0;
    }
    return static_cast<double>(counters.WorkingSetSize) / (1024.0 * 1024.0);
#else
    long pages = 0, resident = 0;
    if (std::FILE* statm = std::fopen("/proc/self/statm", "r"))
    {
        if (std::fscanf(statm, "%ld %ld", &pages, &resident) != 2)
        {
            resident = 0;
        }
        std::fclose(statm);
    }
    return static_cast<double>(resident) * 4096.0 / (1024.0 * 1024.0);
#endif
}

inline void PrintCsv(const char* impl, const Options& opt, double wallSeconds, std::uint64_t delivered,
                     double cpuSeconds, const LatencyHistogram& histogram)
{
    const double fps = wallSeconds > 0.0 ? static_cast<double>(delivered) / wallSeconds : 0.0;
    const double usPerFrame = delivered > 0 ? cpuSeconds * 1e6 / static_cast<double>(delivered) : 0.0;
    std::printf("cmp,%s,%s,%d,%zu,%d,%.3f,%llu,%.3f,%.4f,%.2f,%.2f,%.2f\n", impl,
                opt.label.empty() ? "-" : opt.label.c_str(), opt.workers, opt.capacity, opt.iters, wallSeconds,
                static_cast<unsigned long long>(delivered), cpuSeconds, usPerFrame,
                static_cast<double>(histogram.Percentile(0.50)) / 1000.0,
                static_cast<double>(histogram.Percentile(0.95)) / 1000.0,
                static_cast<double>(histogram.Percentile(0.99)) / 1000.0);
    std::printf("rss,%.1f\n", ResidentMiB());
    std::fflush(stdout);
}

}  // namespace cmp
