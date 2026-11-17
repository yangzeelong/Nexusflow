#ifndef NEXUSFLOW_BENCHMARKS_PROCESS_CPU_TIME_HPP
#define NEXUSFLOW_BENCHMARKS_PROCESS_CPU_TIME_HPP

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <sys/resource.h>
#endif

namespace nexusflow_bench {

/**
 * @brief CPU time consumed by this process so far, in seconds.
 *
 * Reported next to wall-clock throughput because the two answer different
 * questions: wall time says how fast the pipeline delivers frames, CPU time says
 * how much machine it burns to do that. A pipeline that spins while waiting
 * looks fine in wall time and terrible here.
 *
 * std::clock() is not used: on Windows it returns wall time, not CPU time.
 */
inline double ProcessCpuSeconds() {
#if defined(_WIN32)
    FILETIME creation = {}, exit = {}, kernel = {}, user = {};
    if (!GetProcessTimes(GetCurrentProcess(), &creation, &exit, &kernel, &user)) {
        return 0.0;
    }
    auto toSeconds = [](const FILETIME& time) -> double {
        ULARGE_INTEGER value;
        value.LowPart = time.dwLowDateTime;
        value.HighPart = time.dwHighDateTime;
        // 100-nanosecond units.
        return static_cast<double>(value.QuadPart) / 1.0e7;
    };
    return toSeconds(kernel) + toSeconds(user);
#else
    struct rusage usage = {};
    if (getrusage(RUSAGE_SELF, &usage) != 0) {
        return 0.0;
    }
    return static_cast<double>(usage.ru_utime.tv_sec) + static_cast<double>(usage.ru_utime.tv_usec) / 1.0e6 +
           static_cast<double>(usage.ru_stime.tv_sec) + static_cast<double>(usage.ru_stime.tv_usec) / 1.0e6;
#endif
}

} // namespace nexusflow_bench

#endif // NEXUSFLOW_BENCHMARKS_PROCESS_CPU_TIME_HPP
