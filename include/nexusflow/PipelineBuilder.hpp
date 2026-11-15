#ifndef NEXUSFLOW_PIPELINE_BUILDER_HPP
#define NEXUSFLOW_PIPELINE_BUILDER_HPP

#include <nexusflow/Config.hpp>
#include <nexusflow/Module.hpp>

#include <memory>
#include <string>
#include <vector>

namespace nexusflow {

// Forward declaration
class Pipeline;

/**
 * @class PipelineBuilder
 * @brief A utility class to programmatically construct a Pipeline.
 *
 * This class provides a fluent interface to define the topology of a pipeline
 * by adding modules and defining the connections between them, without exposing
 * the internal Graph data structure.
 */
class PipelineBuilder {
public:
    PipelineBuilder();
    ~PipelineBuilder();

    // PipelineBuilder
    PipelineBuilder(const PipelineBuilder&) = delete;
    PipelineBuilder& operator=(const PipelineBuilder&) = delete;

    PipelineBuilder(PipelineBuilder&&) noexcept;
    PipelineBuilder& operator=(PipelineBuilder&&) noexcept;

    /**
     * @brief Adds a module instance to the pipeline definition.
     * @param module A shared_ptr to the module instance.
     * @return A reference to this builder, for method chaining (fluent interface).
     */
    PipelineBuilder& AddModule(const std::shared_ptr<Module>& module);

    /**
     * @brief Defines a connection from one module to another.
     * @param srcModuleName The name of the source module.
     * @param dstModuleName The name of the destination module.
     * @return A reference to this builder for chaining.
     */
    PipelineBuilder& Connect(const std::string& srcModuleName, const std::string& dstModuleName);

    /**
     * @brief Supplies the per-module config of an already added module.
     * @details A module instance passed to AddModule() has no config of its own,
     * so a worker knob that lives in config (maxBatchSize, batchTimeoutMs,
     * sourceIdleMs, syncInputs, profileTimers…) could not be set from code.
     * YAML nodes carry their config; this is the same thing for a builder.
     * @param moduleName The module's name, as returned by GetModuleName().
     * @param config The config the module's worker reads when it starts.
     * @return A reference to this builder for chaining.
     */
    PipelineBuilder& SetModuleConfig(const std::string& moduleName, const Config& config);

    /**
     * @brief Builds the Pipeline instance from the defined configuration.
     * @param edgeQueueCapacity Maximum in-flight messages per edge. Required:
     * queue depth is a property of the pipeline being built, so the builder
     * will not guess one. Must be positive.
     * @return A unique_ptr to the constructed Pipeline, or nullptr on failure.
     * @note Overload resolution makes Build() with no argument a compile error,
     * so a caller cannot silently inherit a depth someone else picked.
     */
    std::unique_ptr<Pipeline> Build(int edgeQueueCapacity);

    /**
     * @brief Deprecated: this overload only exists so an old call site fails
     * loudly at run time instead of silently using an arbitrary capacity.
     * @return nullptr, after logging which capacity to pass to Build().
     */
    std::unique_ptr<Pipeline> Build();

private:
    class Impl;
    std::unique_ptr<Impl> m_pImpl;
};

} // namespace nexusflow

#endif