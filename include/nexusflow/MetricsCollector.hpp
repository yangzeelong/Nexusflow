#ifndef NEXUSFLOW_METRICS_COLLECTOR_HPP
#define NEXUSFLOW_METRICS_COLLECTOR_HPP

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace nexusflow {

/// Profiling and diagnostics utilities.
///
/// The collector is public on purpose: applications and extensions instrument
/// their own stages with the TIMER_* macros and print the collected profile. It
/// is a diagnostics surface, not a pipeline primitive, so it is not on the same
/// compatibility footing as Message/Module/Pipeline.
namespace profiling {

/// One timer's cumulative numbers. Every duration describes a single item, so
/// avg/p95/min/max are directly comparable without knowing how the timer was
/// instrumented. An item is whatever the instrumented code counted; a
/// measurement that declares nothing counts one item.
struct TimerStats {
    std::string name;
    /// Items measured so far.
    uint64_t items = 0;
    double totalMs = 0.0;
    /// Durations of one item, not of one call: a call that declared N items
    /// contributes elapsedMs / N, which keeps the tail comparable with the
    /// average instead of mixing per-call and per-item numbers.
    double minMs = 0.0;
    double maxMs = 0.0;
    /// 95th percentile over the most recent items, not over the whole run: the
    /// report describes the workload the pipeline is running now. The registry
    /// keeps a bounded window, so this costs no more than the cumulative
    /// fields above.
    double p95Ms = 0.0;

    /// Call site of the first measurement: `__FILE__` and `__LINE__` of the
    /// macro.
    std::string file;
    int line = 0;

    double AvgItemMs() const;
    double ItemQps() const;

    /// "file.cpp:123" with the directory stripped, for the printed profile.
    std::string Where() const;
};

/// When a timer reports. The registry always accumulates; the policy only
/// decides how often a line is logged and which numbers that line carries.
struct TimerPrintPolicy {
    /// One line per measurement (TIMER_START / TIMER_STOP).
    bool printEachCall = false;
    /// Cumulative report every N milliseconds (TIMER_START_AVERAGE_MS).
    uint64_t everyMs = 0;
    /// Cumulative report every N measurements (TIMER_START_AVERAGE_N).
    uint64_t everyCalls = 0;
    /// Report the window since the previous report instead of the whole run
    /// (TIMER_START_INTERVAL). A throughput probe wants the current load, and
    /// a cumulative average hides it.
    bool windowed = false;

    static TimerPrintPolicy EachCall();
    static TimerPrintPolicy EveryMs(uint64_t ms);
    static TimerPrintPolicy EveryCalls(uint64_t calls);
    static TimerPrintPolicy Interval(uint64_t calls);
};

class MetricsCollector {
public:
    class ScopedTimer;

    /// Interned timer name. Intern() resolves a name to a handle once, so a
    /// measurement can then address the timer by handle: the handle forms of
    /// the calls below skip the per-call name hashing and string copy that the
    /// name forms pay. A handle stays valid for the lifetime of the registry,
    /// and 0 means "not interned" — every handle form treats 0 as a no-op.
    using TimerHandle = uint64_t;

    static MetricsCollector& Instance();

    /// Resolve a timer name to a handle, interning it on first use. Call this
    /// once — at startup, or on the first message — and use the handle from
    /// then on. The name forms below intern internally, so the only difference
    /// between the two families is where the name is resolved.
    TimerHandle Intern(const std::string& name);

    // Start and end one measurement on the calling thread. Nested measurements
    // with the same name are supported by a thread-local stack. A measurement
    // counts one item unless IncrementAverage declares more, which is how a
    // call that processes a batch reports a per-item duration. The start call
    // configures the print policy; file/line come from the calling macro
    // (__FILE__/__LINE__) and are reported with the timer so a printed profile
    // can be traced back to the code that produced it.
    void StartAverage(const std::string& name, TimerPrintPolicy policy,
                      const char* file = "", int line = 0);
    void IncrementAverage(const std::string& name, uint64_t items = 1);
    double EndAverage(const std::string& name);

    // Record an already measured duration.
    void AddItems(const std::string& name, double elapsedMs, uint64_t items = 1,
                  const char* file = "", int line = 0);

    bool GetStats(const std::string& name, TimerStats& stats) const;
    std::vector<TimerStats> Snapshot() const;

    void Print() const;
    bool PrintIfDue(uint64_t intervalMs = 1000);
    void Reset();

    // Time one scope. The print policy is a parameter rather than a family of
    // overloads; the macros below build it, so call sites never name it.
    ScopedTimer Scope(const std::string& name, TimerPrintPolicy policy,
                      const char* file = "", int line = 0);

    // Handle forms: same behaviour, but the name is already resolved, so a
    // measurement only touches an index, the clock and the aggregate. Prefer
    // them in code that measures per message or per frame.
    void StartAverage(TimerHandle handle, TimerPrintPolicy policy,
                      const char* file = "", int line = 0);
    void IncrementAverage(TimerHandle handle, uint64_t items = 1);
    double EndAverage(TimerHandle handle);
    void AddItems(TimerHandle handle, double elapsedMs, uint64_t items = 1,
                  const char* file = "", int line = 0);
    bool GetStats(TimerHandle handle, TimerStats& stats) const;
    ScopedTimer Scope(TimerHandle handle, TimerPrintPolicy policy,
                      const char* file = "", int line = 0);

private:
    /// Bounded ring of the most recent per-item durations, used for the tail
    /// percentile. Fixed capacity keeps the registry footprint flat however
    /// long a process runs.
    struct RecentWindow {
        static constexpr std::size_t kCapacity = 1024;

        std::vector<double> values;
        std::size_t cursor = 0;

        void Push(double ms) {
            if (values.size() < kCapacity) {
                values.push_back(ms);
                return;
            }
            // Full: overwrite the oldest entry. Order does not matter here
            // because only percentiles are read back.
            values[cursor] = ms;
            cursor = (cursor + 1) % kCapacity;
        }

        /// Nearest-rank percentile over the window. It copies the window
        /// because nth_element reorders in place; that is fine at report
        /// frequency and keeps this const.
        double Percentile(double percentile) const {
            if (values.empty()) return 0.0;
            std::vector<double> scratch(values);
            const std::size_t rank = static_cast<std::size_t>(
                percentile * static_cast<double>(scratch.size() - 1) + 0.5);
            std::nth_element(scratch.begin(), scratch.begin() + rank,
                             scratch.end());
            return scratch[rank];
        }
    };

    struct Aggregate {
        /// Measurements taken. Kept for the "first measurement sets the call
        /// site" rule and for the print policy; it is not reported, because an
        /// item count is what the profile is about.
        uint64_t calls = 0;
        uint64_t items = 0;
        double totalMs = 0.0;
        double minMs = 0.0;
        double maxMs = 0.0;
        uint64_t callsSincePrint = 0;
        std::chrono::steady_clock::time_point lastPrint;
        /// Windowed reporting state (TIMER_START_INTERVAL): reset every time a
        /// window is reported.
        std::chrono::steady_clock::time_point windowStart;
        uint64_t windowItems = 0;
        double windowServiceMs = 0.0;
        std::string file;
        int line = 0;
        RecentWindow recent;
    };

    /// One interned timer: the name is kept so reports stay readable and the
    /// aggregate is what measurements update. Slots are addressed by
    /// handle - 1 and are never removed, which is what keeps handles stable.
    struct Slot {
        std::string name;
        Aggregate aggregate;
    };

    struct CompletedSample {
        std::string name;
        double elapsedMs = 0.0;
        uint64_t items = 0;
        bool printSample = false;
        bool printAverage = false;
        /// Cumulative report, filled when the policy is not windowed.
        TimerStats stats;
        /// Windowed report: the window that just closed.
        bool windowed = false;
        uint64_t windowItems = 0;
        double windowWallMs = 0.0;
        double windowServiceMs = 0.0;
    };

    /// A measurement covers at least one item, so a call that declares nothing
    /// still lands in the per-item numbers.
    static uint64_t ItemCount(uint64_t items) {
        return items == 0 ? 1 : items;
    }

    /// Fold one measurement into an aggregate. The caller holds m_mutex.
    static void Accumulate(Aggregate& aggregate, double elapsedMs,
                           uint64_t items, const char* file, int line);
    /// Read one aggregate back out. p95Ms stays 0 because it walks the window,
    /// so the callers that report it fill it in.
    static TimerStats MakeStats(const std::string& name,
                                const Aggregate& aggregate);

    /// Resolve a handle to its slot. The caller holds m_mutex, and the result
    /// must not outlive the lock: interning can reallocate m_slots.
    Slot* FindSlot(TimerHandle handle);
    const Slot* FindSlot(TimerHandle handle) const;

    /// Handle of a name that was interned before, or 0. Unlike Intern() this
    /// never adds a timer, so stopping or reading an unknown name stays free.
    TimerHandle FindInterned(const std::string& name) const;

    CompletedSample AddItemsWithPolicy(
        TimerHandle handle, double elapsedMs, uint64_t items,
        const TimerPrintPolicy& policy,
        std::chrono::steady_clock::time_point started,
        std::chrono::steady_clock::time_point ended,
        const char* file, int line);
    void PrintStats(const TimerStats& stats) const;
    void PrintWindow(const CompletedSample& sample) const;
    void PrintSample(const CompletedSample& sample) const;

    mutable std::mutex m_mutex;
    /// Interned timers, indexed by handle - 1. Appended by Intern() and never
    /// removed, so a handle names the same slot forever.
    std::vector<Slot> m_slots;
    /// name -> handle, so the name forms resolve in one hash lookup. Guarded by
    /// m_mutex together with m_slots, which interning appends to.
    std::unordered_map<std::string, TimerHandle> m_names;
    mutable std::mutex m_reportMutex;
    std::chrono::steady_clock::time_point m_lastReport;
};

class MetricsCollector::ScopedTimer {
public:
    ScopedTimer(MetricsCollector& registry, std::string name,
                TimerPrintPolicy policy, const char* file, int line);
    ScopedTimer(MetricsCollector& registry, TimerHandle handle,
                TimerPrintPolicy policy, const char* file, int line);
    ~ScopedTimer();

    ScopedTimer(const ScopedTimer&) = delete;
    ScopedTimer& operator=(const ScopedTimer&) = delete;

    ScopedTimer(ScopedTimer&& other) noexcept;
    ScopedTimer& operator=(ScopedTimer&& other) noexcept;

private:
    MetricsCollector* m_registry = nullptr;
    TimerHandle m_handle = 0;
    bool m_active = false;
};

} // namespace profiling
} // namespace nexusflow

// ---------------------------------------------------------------------------
// Instrumentation macros
//
// Every macro takes a single timer: a name, or a handle interned from one. Call
// sites compose the name as "<module>.<label>", using the owning module's name
// (usually Module::GetModuleName()) so a timer stays attributable when a helper
// runs for several modules.
//
// One measurement counts one item. Per-call timing, useful while bringing a
// stage up:
//
//     TIMER_START(moduleName + ".Decode");
//     // do something
//     TIMER_STOP(moduleName + ".Decode");
//
// Aggregated timing, which reports the running average, percentile and
// extremes every `ms` milliseconds or every `calls` measurements:
//
//     TIMER_START_AVERAGE_MS(moduleName + ".Batch", 5000);
//     // do something
//     TIMER_STOP_AVERAGE(moduleName + ".Batch");
//
// When one measurement covers several items, declare how many so the report
// stays per item:
//
//     TIMER_START_AVERAGE_MS(moduleName + ".Batch", 5000);
//     TIMER_INCREMENT_AVERAGE(moduleName + ".Batch", batchSize);
//     // do something
//     TIMER_STOP_AVERAGE(moduleName + ".Batch");
//
// Windowed timing reports the window that just closed instead of the whole
// run, which is what a throughput probe wants:
//
//     TIMER_START_INTERVAL(moduleName + ".Decode", 1000);
//     // do something
//     TIMER_STOP_INTERVAL(moduleName + ".Decode");
//
// The TIMER_SCOPE* forms take the same measurements but end when the scope
// exits. Prefer them wherever the scope has an early return: returning before
// TIMER_STOP leaves the measurement open, and the elapsed time of the whole
// gap is then reported by a later measurement of the same name.
//
//     TIMER_SCOPE_AVERAGE_MS(moduleName + ".Preprocess", 5000);
//
// A name costs a hash lookup and a string copy on every measurement. When a
// timer runs per message or per frame, intern the name once and hand the
// handle to the same macros — they accept either:
//
//     const auto decodeTimer =
//         MetricsCollector::Instance().Intern(moduleName + ".Decode");
//     TIMER_SCOPE_AVERAGE_MS(decodeTimer, 5000);
//
// Each macro forwards __FILE__/__LINE__, which the registry reports next to the
// timer so a printed profile points back at the code that produced it. Use
// these macros instead of calling MetricsCollector directly.
//
// Wrap arguments that contain a comma (template arguments, for example) in
// parentheses, or bind the value to a local first.
// ---------------------------------------------------------------------------

#define NEXUSFLOW_TIMER_CONCAT_INNER(a, b) a##b
#define NEXUSFLOW_TIMER_CONCAT(a, b) NEXUSFLOW_TIMER_CONCAT_INNER(a, b)

// --- Per-call timing -------------------------------------------------------

#define TIMER_START(name) \
    ::nexusflow::profiling::MetricsCollector::Instance().StartAverage( \
        (name), ::nexusflow::profiling::TimerPrintPolicy::EachCall(), __FILE__, __LINE__)

#define TIMER_STOP(name) \
    ::nexusflow::profiling::MetricsCollector::Instance().EndAverage((name))

// --- Aggregated timing -----------------------------------------------------

#define TIMER_START_AVERAGE_MS(name, ms) \
    ::nexusflow::profiling::MetricsCollector::Instance().StartAverage( \
        (name), ::nexusflow::profiling::TimerPrintPolicy::EveryMs(ms), __FILE__, __LINE__)

#define TIMER_START_AVERAGE_N(name, calls) \
    ::nexusflow::profiling::MetricsCollector::Instance().StartAverage( \
        (name), ::nexusflow::profiling::TimerPrintPolicy::EveryCalls(calls), \
        __FILE__, __LINE__)

#define TIMER_STOP_AVERAGE(name) TIMER_STOP(name)

/// Declare that the measurement in progress covers `items` more items, so the
/// report stays per item. Without it a measurement counts as one item.
#define TIMER_INCREMENT_AVERAGE(name, items) \
    ::nexusflow::profiling::MetricsCollector::Instance().IncrementAverage((name), (items))

// --- Windowed timing -------------------------------------------------------

#define TIMER_START_INTERVAL(name, calls) \
    ::nexusflow::profiling::MetricsCollector::Instance().StartAverage( \
        (name), ::nexusflow::profiling::TimerPrintPolicy::Interval(calls), \
        __FILE__, __LINE__)

#define TIMER_STOP_INTERVAL(name) TIMER_STOP(name)

// --- Scoped (RAII) timing --------------------------------------------------

#define TIMER_SCOPE(name) \
    auto NEXUSFLOW_TIMER_CONCAT(nexusflowTimer_, __LINE__) = \
        ::nexusflow::profiling::MetricsCollector::Instance().Scope( \
            (name), ::nexusflow::profiling::TimerPrintPolicy::EachCall(), \
            __FILE__, __LINE__)

#define TIMER_SCOPE_AVERAGE_MS(name, ms) \
    auto NEXUSFLOW_TIMER_CONCAT(nexusflowTimer_, __LINE__) = \
        ::nexusflow::profiling::MetricsCollector::Instance().Scope( \
            (name), ::nexusflow::profiling::TimerPrintPolicy::EveryMs(ms), \
            __FILE__, __LINE__)

/// Record an already measured duration.
#define TIMER_ADD_ITEMS(name, ms, items) \
    ::nexusflow::profiling::MetricsCollector::Instance().AddItems( \
        (name), (ms), (items), __FILE__, __LINE__)

#endif
