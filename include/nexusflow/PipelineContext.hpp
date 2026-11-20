#ifndef NEXUSFLOW_PIPELINE_CONTEXT_HPP
#define NEXUSFLOW_PIPELINE_CONTEXT_HPP

#include <nexusflow/Logging.hpp>
#include <nexusflow/MetricsCollector.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace nexusflow {

/**
 * @class PipelineContext
 * @brief Services shared by all modules of one Pipeline.
 *
 * A pipeline owns one context and hands it to every module before Init() runs
 * (see Module::GetContext()). It deliberately contains only cross-module
 * services; the graph, the edge queues and the module tables stay private to
 * Pipeline::Impl.
 *
 * The context is optional from a module's point of view: modules that do not
 * care about per-pipeline logging, profiling, stop signals or custom metrics
 * keep using the process-wide defaults and do not need to touch it.
 *
 * Thread safety: all query and mutate methods are safe to call from module
 * worker threads while the pipeline runs.
 */
class PipelineContext {
public:
    explicit PipelineContext(std::string name);
    ~PipelineContext();

    PipelineContext(const PipelineContext&) = delete;
    PipelineContext& operator=(const PipelineContext&) = delete;

    /// Name of the pipeline this context belongs to (the graph name).
    const std::string& GetName() const;

    /// Installs a sink for records logged by this pipeline's module threads.
    /// Passing nullptr removes the override and falls back to the process-wide
    /// sink. The override only affects threads owned by this pipeline.
    /// Chainable, like the other setters on this context.
    PipelineContext& SetSink(std::shared_ptr<log::LogSink> sink);
    std::shared_ptr<log::LogSink> GetSink() const;
    bool HasOwnSink() const;

    /// Per-pipeline timer registry. Modules instrumented with the TIMER_* macros
    /// still report to the process-wide registry; use Profiler() directly (or
    /// the worker's `profileTimers` path) when timings must stay isolated.
    profiling::MetricsCollector& Profiler() { return m_profiler; }

    /**
     * @brief One accumulated custom metric reported by a module.
     */
    struct Metric {
        std::string name;
        std::uint64_t samples = 0;
        double total = 0.0;
        double last = 0.0;

        double Average() const { return samples == 0 ? 0.0 : total / static_cast<double>(samples); }
    };

    /// A metric name resolved once, so a module on the hot path pays one map
    /// lookup instead of hashing a string per sample. 0 means "not resolved".
    using MetricHandle = std::uint64_t;

    /**
     * @brief Resolves a metric name to a handle, creating it on first use.
     * @details A module must prefix its own name ("<module>.<metric>"), because
     * the context is shared by the whole pipeline and two modules reporting
     * "latency" would otherwise be indistinguishable. A name without a dot gets
     * the prefix of the caller-less form "<unnamed>." rejected at the call site
     * in debug builds only; at runtime the name is kept as given, but
     * GetReportedMetrics() reports it verbatim, so the convention is the
     * caller's contract.
     */
    MetricHandle Intern(const std::string& name);

    /// Adds one sample to an interned metric. This is the hot-path form: a
    /// target is recorded under its handle, so no string is hashed per sample.
    void Record(MetricHandle handle, double value);

    /// Adds one sample to a named metric, interning the name first. Convenient
    /// for setup code and tests; a hot loop should hold the handle instead.
    void Record(const std::string& name, double value);

    /// Copies every metric reported so far. Convenient when a caller keeps the
    /// list; a poller that only renders or counts should take ForEachMetric(),
    /// which spends one lock and no allocation.
    std::vector<Metric> GetReportedMetrics() const;

    /// Number of metrics interned so far. Handles are 1..Count(), so this is also
    /// the highest valid handle.
    std::size_t GetReportedMetricCount() const;

    /// One metric by handle, or nullptr when the handle was never interned in
    /// this context. The pointer is only valid until the next Intern().
    const Metric* FindMetric(MetricHandle handle) const;

    /// Visits every metric under one lock, in intern order. `fn` receives
    /// `const Metric&` and must not call back into this context.
    template <typename Fn>
    void ForEachMetric(Fn&& fn) const {
        std::lock_guard<std::mutex> lock(m_metricsMutex);
        for (std::size_t i = 0; i < m_metrics.size(); ++i) {
            fn(m_metrics[i]);
        }
    }

    /// Set by Pipeline::Stop() before the queues and workers are torn down, so a
    /// module in a long call can notice the pipeline is going away.
    void RequestStop();
    bool StopRequested() const;

    /**
     * @brief Enables or disables the per-hop stage timing chain for this pipeline.
     *
     * When enabled, every message a module dispatches carries the accumulated
     * per-stage elapsed times (`MessageMeta::GetModuleTimings()`). That chain
     * deep-copies a map on every hop: measured on a 3-stage linear pipeline it
     * cost roughly 2x the end-to-end throughput, so it is **off by default**.
     *
     * Chainable with the other setters:
     *     pipeline->GetContext()->SetSink(sink).EnableModuleTimings();
     *
     * Safe to call while the pipeline runs; modules observe the change on their
     * next message. A module can also opt in on its own through
     * Module::SetModuleTimingsEnabled().
     */
    PipelineContext& EnableModuleTimings(bool enabled = true);

    /// Whether this pipeline's modules record per-hop stage timings.
    bool ModuleTimingsEnabled() const;

    /**
     * @brief Sets how many host threads drive the modules of this pipeline.
     * @details 0 (the default) keeps the historical model: every module owns
     * one thread. A positive value shares that many threads between the
     * modules; module i is hosted by thread i % threadCount for its whole
     * run, which is what lets a worker keep its single-writer counters.
     * Fusion modules (a sink with several synchronised inputs) block on
     * every input of a group and therefore keep their own thread.
     *
     * Chainable with the other setters:
     *     pipeline->GetContext()->WithThreadPoolSize(4);
     *
     * Must be set before Start(); the value is read when the pipeline
     * starts and stays fixed for that run.
     */
    PipelineContext& WithThreadPoolSize(std::size_t threadCount);

    /// Threads the next Start() will share between this pipeline's modules;
    /// 0 means one thread per module.
    std::size_t GetThreadPoolSize() const;

private:
    std::string m_name;

    mutable std::mutex m_sinkMutex;
    std::shared_ptr<log::LogSink> m_sink;

    profiling::MetricsCollector m_profiler;

    mutable std::mutex m_metricsMutex;
    /// Indexed by handle - 1; appended by Intern() and never removed, so a handle
    /// stays valid for the life of the context.
    std::vector<Metric> m_metrics;
    std::unordered_map<std::string, MetricHandle> m_metricNames;

    std::atomic<bool> m_stopRequested{false};
    /// See EnableModuleTimings(): pressing this switch is the only pipeline-wide
    /// reason to pay for the per-hop timing map.
    std::atomic<bool> m_moduleTimingsEnabled{false};
    /// 0 means one thread per module; see WithThreadPoolSize().
    std::atomic<std::size_t> m_threadPoolSize{0};
};

} // namespace nexusflow

#endif // NEXUSFLOW_PIPELINE_CONTEXT_HPP
