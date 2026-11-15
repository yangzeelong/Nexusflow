#ifndef NEXUSFLOW_PIPELINE_CONTEXT_HPP
#define NEXUSFLOW_PIPELINE_CONTEXT_HPP

#include <nexusflow/Logging.hpp>
#include <nexusflow/TimerRegistry.hpp>

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
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
    void SetSink(std::shared_ptr<log::LogSink> sink);
    std::shared_ptr<log::LogSink> GetSink() const;
    bool HasOwnSink() const;

    /// Per-pipeline timer registry. Modules instrumented with the TIMER_* macros
    /// still report to the process-wide registry; use Profiler() directly (or
    /// the worker's `profileTimers` path) when timings must stay isolated.
    profiling::TimerRegistry& Profiler() { return m_profiler; }

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

    /// Adds one sample to a named metric. Cheap enough for the hot path (a map
    /// lookup under a mutex); modules should prefer batching when possible.
    void Report(const std::string& name, double value);
    std::vector<Metric> GetReportedMetrics() const;

    /// Set by Pipeline::Stop() before the queues and workers are torn down, so a
    /// module in a long call can notice the pipeline is going away.
    void RequestStop();
    bool StopRequested() const;

private:
    std::string m_name;

    mutable std::mutex m_sinkMutex;
    std::shared_ptr<log::LogSink> m_sink;

    profiling::TimerRegistry m_profiler;

    mutable std::mutex m_metricsMutex;
    std::map<std::string, Metric> m_metrics;

    std::atomic<bool> m_stopRequested{false};
};

} // namespace nexusflow

#endif // NEXUSFLOW_PIPELINE_CONTEXT_HPP
