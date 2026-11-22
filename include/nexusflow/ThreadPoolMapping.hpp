#ifndef NEXUSFLOW_THREADPOOLMAPPING_HPP
#define NEXUSFLOW_THREADPOOLMAPPING_HPP

namespace nexusflow {

/**
 * @brief How a thread pool spreads modules over its threads.
 *
 * The default, kContiguous, gives each thread a block of consecutive stages:
 * a message crosses the whole block on one core, so only the block boundaries
 * pay a cross-core hand-off. kRoundRobin deals stages out one by one, which
 * spreads the load but puts every hop on a different core.
 *
 * Must be selected before Pipeline::Start(); see
 * PipelineContext::WithThreadPoolMapping().
 */
enum class ThreadPoolMapping {
    kContiguous = 0,  ///< consecutive stages share a thread (cache friendly)
    kRoundRobin = 1,  ///< every stage lands on the next thread (maximum spread)
};

}  // namespace nexusflow

#endif  // NEXUSFLOW_THREADPOOLMAPPING_HPP
