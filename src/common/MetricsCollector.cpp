#include "nexusflow/MetricsCollector.hpp"

#include <algorithm>
#include <unordered_map>

#include "nexusflow/Logging.hpp"

namespace nexusflow {
namespace profiling {
namespace {

struct ActiveTimer {
    std::chrono::steady_clock::time_point started;
    TimerPrintPolicy policy;
    /// Items declared by IncrementAverage. Zero means the measurement did not
    /// declare anything, which counts as one item.
    uint64_t items = 0;
    /// __FILE__/__LINE__ of the instrumentation macro that started the sample.
    const char* file = "";
    int line = 0;
};

/// Open measurements of the calling thread, keyed by interned handle. An entry
/// is never erased, so measuring the same timer again reuses its vector and its
/// capacity instead of allocating one per measurement, and the key is a small
/// integer rather than a string, so a lookup hashes no name.
thread_local std::unordered_map<MetricsCollector::TimerHandle,
                                std::vector<ActiveTimer>> g_activeTimers;

/// Strip the directory from __FILE__ so the printed profile stays readable.
std::string BaseName(const char* path) {
    if (path == nullptr) return {};
    const std::string full(path);
    const std::size_t slash = full.find_last_of("/\\");
    return slash == std::string::npos ? full : full.substr(slash + 1);
}

} // namespace

std::string TimerStats::Where() const {
    if (file.empty()) return {};
    if (line <= 0) return BaseName(file.c_str());
    return BaseName(file.c_str()) + ":" + std::to_string(line);
}

double TimerStats::AvgItemMs() const {
    return items == 0 ? 0.0 : totalMs / static_cast<double>(items);
}

double TimerStats::ItemQps() const {
    const double seconds = totalMs / 1000.0;
    return seconds <= 0.0 ? 0.0 : static_cast<double>(items) / seconds;
}

TimerPrintPolicy TimerPrintPolicy::EachCall() {
    TimerPrintPolicy policy;
    policy.printEachCall = true;
    return policy;
}

TimerPrintPolicy TimerPrintPolicy::EveryMs(uint64_t ms) {
    TimerPrintPolicy policy;
    policy.everyMs = ms;
    return policy;
}

TimerPrintPolicy TimerPrintPolicy::EveryCalls(uint64_t calls) {
    TimerPrintPolicy policy;
    policy.everyCalls = calls;
    return policy;
}

TimerPrintPolicy TimerPrintPolicy::Interval(uint64_t calls) {
    TimerPrintPolicy policy;
    policy.everyCalls = calls;
    policy.windowed = true;
    return policy;
}

MetricsCollector& MetricsCollector::Instance() {
    static MetricsCollector registry;
    return registry;
}

MetricsCollector::TimerHandle MetricsCollector::Intern(const std::string& name) {
    if (name.empty()) return 0;
    std::lock_guard<std::mutex> lock(m_mutex);
    const auto found = m_names.find(name);
    if (found != m_names.end()) return found->second;

    Slot slot;
    slot.name = name;
    m_slots.push_back(std::move(slot));
    const TimerHandle handle = static_cast<TimerHandle>(m_slots.size());
    m_names.emplace(m_slots.back().name, handle);
    return handle;
}

MetricsCollector::TimerHandle MetricsCollector::FindInterned(
    const std::string& name) const {
    // The stop/read forms must not intern: a measurement that was never started
    // is not a timer, and it should not show up in Snapshot() either.
    if (name.empty()) return 0;
    std::lock_guard<std::mutex> lock(m_mutex);
    const auto found = m_names.find(name);
    return found == m_names.end() ? 0 : found->second;
}

MetricsCollector::Slot* MetricsCollector::FindSlot(TimerHandle handle) {
    if (handle == 0 || handle > static_cast<TimerHandle>(m_slots.size())) {
        return nullptr;
    }
    return &m_slots[static_cast<std::size_t>(handle - 1)];
}

const MetricsCollector::Slot* MetricsCollector::FindSlot(TimerHandle handle) const {
    if (handle == 0 || handle > static_cast<TimerHandle>(m_slots.size())) {
        return nullptr;
    }
    return &m_slots[static_cast<std::size_t>(handle - 1)];
}

void MetricsCollector::Accumulate(Aggregate& aggregate, double elapsedMs,
                               uint64_t items, const char* file, int line) {
    // Normalizing here is what keeps every reported duration on the item axis:
    // a measurement that covered N items contributes elapsedMs / N, so min,
    // max and the percentile window stay comparable with the average.
    const uint64_t counted = ItemCount(items);
    // One item per measurement is the common case; skipping the division there
    // keeps the enabled path a few instructions shorter.
    const double perItemMs = counted == 1
        ? elapsedMs : elapsedMs / static_cast<double>(counted);

    ++aggregate.calls;
    aggregate.items += counted;
    aggregate.totalMs += elapsedMs;
    aggregate.recent.Push(perItemMs);
    if (aggregate.calls == 1) {
        aggregate.minMs = perItemMs;
        aggregate.maxMs = perItemMs;
        // The first measurement fixes the reported call site. A name used from
        // several places (a loop, or an inlined helper) keeps the first one.
        aggregate.file = file != nullptr ? file : "";
        aggregate.line = line;
    } else {
        aggregate.minMs = std::min(aggregate.minMs, perItemMs);
        aggregate.maxMs = std::max(aggregate.maxMs, perItemMs);
    }
}

TimerStats MetricsCollector::MakeStats(const std::string& name,
                                    const Aggregate& aggregate) {
    TimerStats stats;
    stats.name = name;
    stats.items = aggregate.items;
    stats.totalMs = aggregate.totalMs;
    stats.minMs = aggregate.minMs;
    stats.maxMs = aggregate.maxMs;
    stats.file = aggregate.file;
    stats.line = aggregate.line;
    return stats;
}

void MetricsCollector::StartAverage(const std::string& name,
                                 TimerPrintPolicy policy,
                                 const char* file, int line) {
    StartAverage(Intern(name), policy, file, line);
}

void MetricsCollector::StartAverage(TimerHandle handle, TimerPrintPolicy policy,
                                 const char* file, int line) {
    if (handle == 0) return;
    g_activeTimers[handle].push_back(
        ActiveTimer{std::chrono::steady_clock::now(), policy, 0, file, line});
}

void MetricsCollector::IncrementAverage(const std::string& name, uint64_t items) {
    IncrementAverage(FindInterned(name), items);
}

void MetricsCollector::IncrementAverage(TimerHandle handle, uint64_t items) {
    if (handle == 0) return;
    const auto it = g_activeTimers.find(handle);
    if (it == g_activeTimers.end() || it->second.empty()) return;
    it->second.back().items += items;
}

double MetricsCollector::EndAverage(const std::string& name) {
    return EndAverage(FindInterned(name));
}

double MetricsCollector::EndAverage(TimerHandle handle) {
    if (handle == 0) return 0.0;
    const auto it = g_activeTimers.find(handle);
    if (it == g_activeTimers.end() || it->second.empty()) return 0.0;

    const ActiveTimer active = it->second.back();
    it->second.pop_back();

    const auto ended = std::chrono::steady_clock::now();
    const double elapsedMs = std::chrono::duration<double, std::milli>(
        ended - active.started).count();
    const CompletedSample completed = AddItemsWithPolicy(
        handle, elapsedMs, active.items, active.policy, active.started,
        ended, active.file, active.line);
    if (completed.printSample) PrintSample(completed);
    if (completed.printAverage) {
        if (completed.windowed) {
            PrintWindow(completed);
        } else {
            PrintStats(completed.stats);
        }
    }
    return elapsedMs;
}

void MetricsCollector::AddItems(const std::string& name, double elapsedMs,
                             uint64_t items,
                             const char* file, int line) {
    AddItems(Intern(name), elapsedMs, items, file, line);
}

void MetricsCollector::AddItems(TimerHandle handle, double elapsedMs,
                             uint64_t items,
                             const char* file, int line) {
    elapsedMs = std::max(0.0, elapsedMs);
    std::lock_guard<std::mutex> lock(m_mutex);
    Slot* slot = FindSlot(handle);
    if (slot == nullptr) return;
    Accumulate(slot->aggregate, elapsedMs, items, file, line);
}

MetricsCollector::CompletedSample MetricsCollector::AddItemsWithPolicy(
    TimerHandle handle, double elapsedMs, uint64_t items,
    const TimerPrintPolicy& policy,
    std::chrono::steady_clock::time_point started,
    std::chrono::steady_clock::time_point ended,
    const char* file, int line) {
    elapsedMs = std::max(0.0, elapsedMs);

    CompletedSample completed;
    completed.elapsedMs = elapsedMs;
    completed.items = ItemCount(items);
    completed.printSample = policy.printEachCall;

    const uint64_t counted = ItemCount(items);
    std::lock_guard<std::mutex> lock(m_mutex);
    Slot* slot = FindSlot(handle);
    if (slot == nullptr) {
        completed.printSample = false;
        return completed;
    }

    Aggregate& aggregate = slot->aggregate;
    Accumulate(aggregate, elapsedMs, items, file, line);
    ++aggregate.callsSincePrint;
    aggregate.windowItems += counted;
    aggregate.windowServiceMs += elapsedMs;
    if (aggregate.windowStart.time_since_epoch().count() == 0) {
        aggregate.windowStart = started;
    }

    const bool dueByCalls = policy.everyCalls > 0 &&
        aggregate.callsSincePrint >= policy.everyCalls;
    bool dueByTime = false;
    if (policy.everyMs > 0) {
        if (aggregate.lastPrint.time_since_epoch().count() == 0) {
            aggregate.lastPrint = started;
        }
        dueByTime = std::chrono::duration_cast<std::chrono::milliseconds>(
            ended - aggregate.lastPrint).count() >=
            static_cast<int64_t>(policy.everyMs);
    }

    if (dueByCalls || dueByTime) {
        aggregate.callsSincePrint = 0;
        aggregate.lastPrint = ended;
        completed.printAverage = true;
        if (policy.windowed) {
            // A window is described by wall time (what a throughput probe
            // measures) and by service time (what the average needs).
            completed.windowed = true;
            completed.windowItems = aggregate.windowItems;
            completed.windowWallMs = std::chrono::duration<double, std::milli>(
                ended - aggregate.windowStart).count();
            completed.windowServiceMs = aggregate.windowServiceMs;
            aggregate.windowStart = ended;
            aggregate.windowItems = 0;
            aggregate.windowServiceMs = 0.0;
        } else {
            completed.stats = MakeStats(slot->name, aggregate);
            // Only computed for a report: the percentile walks the window,
            // which is not worth doing on every measurement of a hot timer.
            completed.stats.p95Ms = aggregate.recent.Percentile(0.95);
        }
    }
    if (completed.printSample || completed.printAverage) {
        // Only a report needs the name, so a hot measurement copies no string.
        completed.name = slot->name;
    }

    return completed;
}

bool MetricsCollector::GetStats(const std::string& name, TimerStats& stats) const {
    std::lock_guard<std::mutex> lock(m_mutex);
    const auto found = m_names.find(name);
    if (found == m_names.end()) return false;
    const Slot* slot = FindSlot(found->second);
    if (slot == nullptr || slot->aggregate.calls == 0) return false;

    stats = MakeStats(slot->name, slot->aggregate);
    stats.p95Ms = slot->aggregate.recent.Percentile(0.95);
    return true;
}

bool MetricsCollector::GetStats(TimerHandle handle, TimerStats& stats) const {
    std::lock_guard<std::mutex> lock(m_mutex);
    const Slot* slot = FindSlot(handle);
    if (slot == nullptr || slot->aggregate.calls == 0) return false;

    stats = MakeStats(slot->name, slot->aggregate);
    stats.p95Ms = slot->aggregate.recent.Percentile(0.95);
    return true;
}

std::vector<TimerStats> MetricsCollector::Snapshot() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    std::vector<TimerStats> snapshot;
    snapshot.reserve(m_slots.size());
    for (const auto& slot : m_slots) {
        // A slot outlives a Reset(), which is why an untouched one is skipped
        // rather than removed: a reset profile shows what was measured since.
        if (slot.aggregate.calls == 0) continue;
        TimerStats stats = MakeStats(slot.name, slot.aggregate);
        stats.p95Ms = slot.aggregate.recent.Percentile(0.95);
        snapshot.push_back(std::move(stats));
    }
    return snapshot;
}

void MetricsCollector::Print() const {
    for (const auto& stats : Snapshot()) {
        PrintStats(stats);
    }
}

void MetricsCollector::PrintStats(const TimerStats& stats) const {
    // One count and one unit: an item, so a line is directly comparable with
    // the next one regardless of how each timer was instrumented. Throughput
    // comes from the same count, which means items/s is the rate of the work
    // the timer measures rather than of its call pattern.
    //
    // p95 covers the recent window while min/max cover the whole run, so a
    // pipeline whose tail is worsening shows p95 climbing towards max.
    LOG_INFO("Execute '{}' for {} items cost: {:.3f} ms, qps: {:.3f} items/s, "
             "avg: {:.3f} ms/item, p95: {:.3f} ms/item, "
             "min/max: {:.3f}/{:.3f} ms/item",
             stats.name, stats.items, stats.totalMs, stats.ItemQps(),
             stats.AvgItemMs(), stats.p95Ms, stats.minMs, stats.maxMs);
}

void MetricsCollector::PrintWindow(const CompletedSample& sample) const {
    // The window is the run since the previous report, so qps comes from wall
    // time while the average comes from the service time the calls took.
    const double wallMs = sample.windowWallMs;
    const double itemsPerSecond = wallMs <= 0.0
        ? 0.0 : static_cast<double>(sample.windowItems) * 1000.0 / wallMs;
    const double avgItemMs = sample.windowItems == 0
        ? 0.0 : sample.windowServiceMs / static_cast<double>(sample.windowItems);
    LOG_INFO("Execute '{}' for {} items in {:.3f} ms, qps: {:.3f} items/s, "
             "avg: {:.3f} ms/item",
             sample.name, sample.windowItems, wallMs, itemsPerSecond, avgItemMs);
}

void MetricsCollector::PrintSample(const CompletedSample& sample) const {
    if (sample.items <= 1) {
        LOG_INFO("Execute '{}' once cost: {:.3f} ms",
                 sample.name, sample.elapsedMs);
        return;
    }
    LOG_INFO("Execute '{}' once: {} items cost: {:.3f} ms ({:.3f} ms/item)",
             sample.name, sample.items, sample.elapsedMs,
             sample.elapsedMs / static_cast<double>(sample.items));
}

bool MetricsCollector::PrintIfDue(uint64_t intervalMs) {
    const auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(m_reportMutex);
    if (m_lastReport.time_since_epoch().count() != 0 &&
        std::chrono::duration_cast<std::chrono::milliseconds>(
            now - m_lastReport).count() < static_cast<int64_t>(intervalMs)) {
        return false;
    }
    m_lastReport = now;
    Print();
    return true;
}

void MetricsCollector::Reset() {
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        // Handles stay valid: a reset clears what was measured, so the slots
        // are kept (and skipped by Snapshot()) instead of being removed. The
        // next measurement of a slot re-captures its call site, exactly as it
        // would after the name had never been seen.
        for (auto& slot : m_slots) {
            slot.aggregate = Aggregate();
        }
    }
    std::lock_guard<std::mutex> lock(m_reportMutex);
    m_lastReport = std::chrono::steady_clock::time_point();
}

MetricsCollector::ScopedTimer MetricsCollector::Scope(const std::string& name,
                                               TimerPrintPolicy policy,
                                               const char* file, int line) {
    return ScopedTimer(*this, name, policy, file, line);
}

MetricsCollector::ScopedTimer MetricsCollector::Scope(TimerHandle handle,
                                               TimerPrintPolicy policy,
                                               const char* file, int line) {
    return ScopedTimer(*this, handle, policy, file, line);
}

MetricsCollector::ScopedTimer::ScopedTimer(MetricsCollector& registry,
                                        std::string name,
                                        TimerPrintPolicy policy,
                                        const char* file, int line)
    : m_registry(&registry), m_handle(registry.Intern(name)) {
    if (m_handle != 0) {
        m_registry->StartAverage(m_handle, policy, file, line);
        m_active = true;
    }
}

MetricsCollector::ScopedTimer::ScopedTimer(MetricsCollector& registry,
                                        TimerHandle handle,
                                        TimerPrintPolicy policy,
                                        const char* file, int line)
    : m_registry(&registry), m_handle(handle) {
    if (m_handle != 0) {
        m_registry->StartAverage(m_handle, policy, file, line);
        m_active = true;
    }
}

MetricsCollector::ScopedTimer::~ScopedTimer() {
    if (m_active && m_registry != nullptr) {
        m_registry->EndAverage(m_handle);
    }
}

MetricsCollector::ScopedTimer::ScopedTimer(ScopedTimer&& other) noexcept
    : m_registry(other.m_registry),
      m_handle(other.m_handle),
      m_active(other.m_active) {
    other.m_registry = nullptr;
    other.m_active = false;
}

MetricsCollector::ScopedTimer& MetricsCollector::ScopedTimer::operator=(
    ScopedTimer&& other) noexcept {
    if (this == &other) return *this;
    if (m_active && m_registry != nullptr) {
        m_registry->EndAverage(m_handle);
    }
    m_registry = other.m_registry;
    m_handle = other.m_handle;
    m_active = other.m_active;
    other.m_registry = nullptr;
    other.m_active = false;
    return *this;
}

} // namespace profiling
} // namespace nexusflow
