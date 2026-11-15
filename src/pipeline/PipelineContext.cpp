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

void PipelineContext::Report(const std::string& name, double value) {
    std::lock_guard<std::mutex> lock(m_metricsMutex);
    Metric& metric = m_metrics[name];
    metric.name = name;
    ++metric.samples;
    metric.total += value;
    metric.last = value;
}

std::vector<PipelineContext::Metric> PipelineContext::GetReportedMetrics() const {
    std::lock_guard<std::mutex> lock(m_metricsMutex);
    std::vector<Metric> metrics;
    metrics.reserve(m_metrics.size());
    for (const auto& entry : m_metrics) {
        metrics.push_back(entry.second);
    }
    return metrics;
}

void PipelineContext::RequestStop() { m_stopRequested.store(true, std::memory_order_relaxed); }

bool PipelineContext::StopRequested() const { return m_stopRequested.load(std::memory_order_relaxed); }

} // namespace nexusflow
