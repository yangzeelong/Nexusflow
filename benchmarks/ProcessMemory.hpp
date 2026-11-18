#ifndef NEXUSFLOW_BENCHMARKS_PROCESS_MEMORY_HPP
#define NEXUSFLOW_BENCHMARKS_PROCESS_MEMORY_HPP

// Resident memory of this process, for the memory questions no counter answers.
//
// The edge memory a topology holds is `capacity * bytes` and can be computed on
// paper. Two allocations are invisible to Pipeline::GetSnapshot() though: the
// frames themselves (shared_ptr payloads) and the fusion sink's `messageCache`,
// which is a local inside Worker::RunFusion(). The process's own page accounting
// is the only witness, so the harness samples it while a run is in flight and
// reports the peak next to the throughput.

#if defined(_WIN32)
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <psapi.h>
#pragma comment(lib, "psapi.lib")
#elif defined(__linux__)
#include <unistd.h>
#include <cstdio>
#endif

#include <cstddef>

namespace nexusflow_bench {

/// Resident bytes right now; 0 when the platform cannot report it.
inline std::size_t ResidentBytes() {
#if defined(_WIN32)
    PROCESS_MEMORY_COUNTERS counters = {};
    counters.cb = static_cast<DWORD>(sizeof(counters));
    if (!GetProcessMemoryInfo(GetCurrentProcess(), &counters, static_cast<DWORD>(sizeof(counters)))) {
        return 0;
    }
    return static_cast<std::size_t>(counters.WorkingSetSize);
#elif defined(__linux__)
    // /proc/self/statm: size, resident, shared, text, lib, data, dt, in pages.
    std::FILE* statm = std::fopen("/proc/self/statm", "r");
    if (statm == nullptr) {
        return 0;
    }
    unsigned long totalPages = 0;
    unsigned long residentPages = 0;
    const int parsed = std::fscanf(statm, "%lu %lu", &totalPages, &residentPages);
    std::fclose(statm);
    if (parsed != 2) {
        return 0;
    }
    const long pageSize = sysconf(_SC_PAGESIZE);
    return static_cast<std::size_t>(residentPages) * static_cast<std::size_t>(pageSize > 0 ? pageSize : 4096);
#else
    return 0;
#endif
}

} // namespace nexusflow_bench

#endif // NEXUSFLOW_BENCHMARKS_PROCESS_MEMORY_HPP
