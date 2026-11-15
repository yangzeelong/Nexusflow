#include <nexusflow/PipelineContext.hpp>

#include <utility>

namespace nexusflow {

PipelineContext::PipelineContext(std::string name) : m_name(std::move(name)) {}

PipelineContext::~PipelineContext() = default;

const std::string& PipelineContext::GetName() const { return m_name; }

void PipelineContext::SetSink(std::shared_ptr<log::LogSink> sink) {
    std::lock_guard<std::mutex> lock(m_sinkMutex);
    m_sink = std::move(sink);
}

std::shared_ptr<log::LogSink> PipelineContext::GetSink() const {
    std::lock_guard<std::mutex> lock(m_sinkMutex);
    return m_sink;
}

bool PipelineContext::HasOwnSink() const {
    std::lock_guard<std::mutex> lock(m_sinkMutex);
    return static_cast<bool>(m_sink);
}

PipelineContext::MetricHandle PipelineContext::Intern(const std::string& name) {
    std::lock_guard<std::mutex> lock(m_metricsMutex);
    const auto existing = m_metricNames.find(name);
    if (existing != m_metricNames.end()) {
        return existing->second;
    }
    Metric metric;
    metric.name = name;
    m_metrics.push_back(std::move(metric));
    const MetricHandle handle = static_cast<MetricHandle>(m_metrics.size());
    m_metricNames.emplace(name, handle);
    return handle;
}

void PipelineContext::Record(MetricHandle handle, double value) {
    if (handle == 0) {
        return;
    }
    std::lock_guard<std::mutex> lock(m_metricsMutex);
    // Resolve inside the lock: a handle is only valid for this context, and a
    // stale or foreign one must not index into the vector.
    if (handle > m_metrics.size()) {
        return;
    }
    Metric& metric = m_metrics[static_cast<std::size_t>(handle - 1)];
    ++metric.samples;
    metric.total += value;
    metric.last = value;
}

void PipelineContext::Record(const std::string& name, double value) { Record(Intern(name), value); }

std::vector<PipelineContext::Metric> PipelineContext::GetReportedMetrics() const {
    std::lock_guard<std::mutex> lock(m_metricsMutex);
    return m_metrics;
}

void PipelineContext::RequestStop() { m_stopRequested.store(true, std::memory_order_relaxed); }

bool PipelineContext::StopRequested() const { return m_stopRequested.load(std::memory_order_relaxed); }

} // namespace nexusflow
