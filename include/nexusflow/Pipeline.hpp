#ifndef NEXUSFLOW_PIPELINE_HPP
#define NEXUSFLOW_PIPELINE_HPP

#include <nexusflow/ErrorCode.hpp>
#include <nexusflow/Module.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

// TODO 能否做不依赖Graph
// Forward declaration
class Graph;
namespace nexusflow {
class PipelineBuilder;
}

namespace nexusflow {

/**
 * @brief Default capacity of the message queue created for every graph edge.
 *
 * It bounds the number of in-flight messages between two stages, i.e. the
 * backpressure depth of the pipeline. Override it per pipeline through
 * PipelineBuilder::SetMessageQueueCapacity() or the optional
 * `graph.messageQueueCapacity` key in a YAML config.
 */
constexpr int kDefaultMessageQueueCapacity = 5;

/**
 * @brief Runtime counters for one edge queue.
 */
struct QueueMetrics {
    std::string name;
    std::size_t capacity = 0; // 0 means unbounded
    std::size_t size = 0;
    std::size_t highWaterMark = 0;
    std::uint64_t pushed = 0;
    std::uint64_t popped = 0;
};

/**
 * @brief Runtime counters for one module.
 */
struct ModuleMetrics {
    std::string name;
    std::uint64_t processed = 0;
};

/**
 * @brief Snapshot of the pipeline runtime state, taken without stopping it.
 */
struct PipelineMetrics {
    std::vector<QueueMetrics> queues;
    std::vector<ModuleMetrics> modules;
};

/**
 * @brief Pipeline
 *
 */
class Pipeline {
public:
    static std::unique_ptr<Pipeline> CreateFromYaml(const std::string& configPath);

    ErrorCode Init();

    ErrorCode Start();

    ErrorCode Stop();

    ErrorCode DeInit();

    /**
     * @brief Returns queue/module counters. Safe to call while the pipeline runs.
     */
    PipelineMetrics GetMetrics() const;

    ~Pipeline();

private:
    friend PipelineBuilder;

    Pipeline();

    ErrorCode InitWithGraph(std::unique_ptr<Graph> graph);

    class Impl;
    std::unique_ptr<Impl> m_pImpl;
};

} // namespace nexusflow

#endif