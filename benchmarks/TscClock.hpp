#pragma once

/**
 * @file TscClock.hpp
 * @brief Time-stamp counter helpers for the cycles-per-message metric.
 *
 * The harness reports one throughput and two costs (HARNESS.md section 4):
 *   - delivered / wall seconds   -- what a deployable node delivers;
 *   - cpu seconds / message      -- the CPU cost of one message;
 *   - cycles / message           -- the same cost in TSC ticks, so a number
 *                                   survives a change of clock frequency.
 *
 * Only x86 reads a TSC. Elsewhere `TscHz()` returns 0 and the cycles column
 * stays 0.0 instead of fabricating a value.
 */

#include <chrono>
#include <cstdint>

#if defined(_MSC_VER)
#include <intrin.h>
#elif defined(__i386__) || defined(__x86_64__)
#include <x86intrin.h>
#endif

namespace nexusflow_bench
{

/// True when the platform can read a TSC.
inline bool TscAvailable()
{
#if defined(_MSC_VER) || defined(__i386__) || defined(__x86_64__)
    return true;
#else
    return false;
#endif
}

/// One TSC tick; never called unless TscAvailable() is true.
inline std::uint64_t TscNow()
{
#if defined(_MSC_VER) || defined(__i386__) || defined(__x86_64__)
    return __rdtsc();
#else
    return 0;
#endif
}

/**
 * @brief TSC ticks per second, measured against steady_clock.
 *
 * Three 20 ms windows, largest count wins: a window that straddled a
 * preemption reads low, while the TSC itself never slows down.
 */
inline double TscHz()
{
    if (!TscAvailable())
    {
        return 0.0;
    }
    double best = 0.0;
    for (int window = 0; window < 3; ++window)
    {
        const std::uint64_t ticks0 = TscNow();
        const std::chrono::steady_clock::time_point wall0 = std::chrono::steady_clock::now();
        while (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - wall0).count() <
               20)
        {
        }
        const double seconds =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - wall0).count();
        const double ticks = static_cast<double>(TscNow() - ticks0);
        if (seconds > 0.0 && ticks / seconds > best)
        {
            best = ticks / seconds;
        }
    }
    return best;
}

} // namespace nexusflow_bench
