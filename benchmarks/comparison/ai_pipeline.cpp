// ai-pipe, measured with the same protocol as the other four comparison
// programs: six nodes (source + 4 stages + sink), one producer thread pushing
// into the source's output port, a fixed unmeasured warm-up, then a fixed wall
// window in which delivered frames and producer->sink latency are recorded.
//
// The only protocol difference is ai-pipe's backpressure policy: its queues
// drop instead of blocking, so the producer polls `hasQueueCapacity` on the
// first edge (as ai-pipe's own match probe does) and the row carries an extra
// `drop,<n>` line with the frames dropped inside the measured window.
//
// Row format is `cmp::PrintCsv`'s, unchanged, so analysis scripts keep working.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "ai_pipe/data_types.hpp"
#include "ai_pipe/execution_engine.hpp"
#include "ai_pipe/graph.hpp"
#include "ai_pipe/i_logic_node.hpp"
#include "ai_pipe/logger.hpp"

#include "common.hpp"

namespace
{

using namespace ai_pipe;

constexpr std::size_t kStages = 4;
constexpr const char* kSourceName = "source";
constexpr const char* kSinkName = "sink";
constexpr const char* kTimestampKey = "ts";

/// Stands in for the graph's source. ai-pipe's normal input path is
/// `pushInput(source, "output", packet)`, so this node is never scheduled; it
/// only exists so the graph has the same six-node shape as the other programs.
class SourceStub final : public ILogicNode
{
public:
    explicit SourceStub(const std::string& name) : ILogicNode(name) {}

    void process(const PortDataMap& /*inputs*/, PortDataMap& /*outputs*/,
                 std::shared_ptr<PipelineContext> /*context*/) override
    {
    }

    std::vector<std::string> getExpectedInputPorts() const override { return {}; }
    std::vector<std::string> getExpectedOutputPorts() const override { return {"output"}; }
};

/// One pipeline stage. Passes the packet through by reference (shared_ptr, as
/// ai-pipe's own nodes do) and runs the same `cmp::Work` loop as the other four
/// programs when `--iters > 0`; `--iters 0` is a pure passthrough.
class WorkStage final : public ILogicNode
{
public:
    WorkStage(const std::string& name, int iterations) : ILogicNode(name), m_iterations(iterations) {}

    void process(const PortDataMap& inputs, PortDataMap& outputs,
                 std::shared_ptr<PipelineContext> /*context*/) override
    {
        auto it = inputs.find("input");
        if (it != inputs.end() && it->second)
        {
            if (m_iterations > 0)
            {
                m_sink.store(cmp::Work(it->second->id, m_iterations), std::memory_order_relaxed);
            }
            outputs["output"] = it->second;
        }
    }

    std::vector<std::string> getExpectedInputPorts() const override { return {"input"}; }
    std::vector<std::string> getExpectedOutputPorts() const override { return {"output"}; }

private:
    int m_iterations;
    std::atomic<std::uint64_t> m_sink{0};  ///< keeps the work loop from being elided
};

/// Records producer->sink latency into the shared histogram, but only for
/// frames that arrive while the measured window is open (drain is shutdown
/// cost, exactly as in the other programs).
class HistogramSink final : public ILogicNode
{
public:
    HistogramSink(const std::string& name, cmp::LatencyHistogram& histogram,
                  const std::atomic<bool>& measuring)
        : ILogicNode(name), m_histogram(histogram), m_measuring(measuring)
    {
    }

    void process(const PortDataMap& inputs, PortDataMap& /*outputs*/,
                 std::shared_ptr<PipelineContext> /*context*/) override
    {
        auto it = inputs.find("input");
        if (it == inputs.end() || !it->second)
        {
            return;
        }
        if (!m_measuring.load(std::memory_order_relaxed))
        {
            return;
        }
        const auto now = cmp::NowNs();
        const auto stamp = it->second->getOptionalParam<std::uint64_t>(kTimestampKey);
        if (stamp)
        {
            m_histogram.Add(now - *stamp);
        }
        m_delivered.fetch_add(1, std::memory_order_relaxed);
    }

    std::vector<std::string> getExpectedInputPorts() const override { return {"input"}; }
    std::vector<std::string> getExpectedOutputPorts() const override { return {}; }

    std::uint64_t delivered() const { return m_delivered.load(std::memory_order_relaxed); }

private:
    cmp::LatencyHistogram& m_histogram;
    const std::atomic<bool>& m_measuring;
    std::atomic<std::uint64_t> m_delivered{0};
};

struct Topology
{
    std::unique_ptr<Graph> graph;
    std::shared_ptr<HistogramSink> sink;
};

Topology BuildTopology(int iterations, cmp::LatencyHistogram& histogram,
                       const std::atomic<bool>& measuring)
{
    Topology topology;
    topology.graph = std::make_unique<Graph>();

    auto source = std::make_shared<SourceStub>(kSourceName);
    topology.graph->addNode(source);

    std::string previous = kSourceName;
    for (std::size_t i = 0; i < kStages; ++i)
    {
        const std::string name = "node_" + std::to_string(i);
        auto stage = std::make_shared<WorkStage>(name, iterations);
        topology.graph->addNode(stage);
        topology.graph->addEdge(previous, "output", name, "input");
        previous = name;
    }

    topology.sink = std::make_shared<HistogramSink>(kSinkName, histogram, measuring);
    topology.graph->addNode(topology.sink);
    topology.graph->addEdge(previous, "output", kSinkName, "input");

    return topology;
}

}  // namespace

int main(int argc, char** argv)
{
    const cmp::Options opt = cmp::ParseOptions(argc, argv);

    // Same configuration the probe used: statistics on, join-aware sync off
    // (a linear chain has nothing to coordinate), per-drop logging off (at
    // saturation each drop would otherwise format a log line from a worker).
    auto config = EngineConfig::stream(static_cast<std::uint8_t>(opt.workers), opt.capacity);
    config.enable_statistics = true;
    config.enable_sync_coordination = false;
    config.enable_drop_logging = false;

    // Keep stdout reserved for the CSV rows.
    logging::Logger::instance().setLevel(logging::LogLevel::Off);

    if (opt.cpus > 0)
    {
        cmp::PinToLogicalCpus(opt.cpus);
    }
    cmp::TimerResolutionGuard timerGuard;

    std::atomic<bool> measuring{false};
    cmp::LatencyHistogram histogram;
    Topology topology = BuildTopology(opt.iters, histogram, measuring);

    auto engine = ExecutionEngine::create(config);
    if (!engine->initialize(topology.graph.get(), static_cast<std::uint8_t>(opt.workers)))
    {
        std::fprintf(stderr, "ai-pipe: engine initialize failed\n");
        return 1;
    }
    if (!engine->startStreaming())
    {
        std::fprintf(stderr, "ai-pipe: startStreaming failed\n");
        return 1;
    }

    const std::string first_stage = kStages > 0 ? "node_0" : kSinkName;
    const std::string first_port = "input";

    std::uint64_t pushedInWindow = 0;
    std::uint64_t refusedInWindow = 0;
    std::uint64_t seq = 0;

    // Unmeasured run-in: same graph, same load, nothing counted. Keeps the
    // first-second page faults and worker start-up out of the window.
    const std::uint64_t warmupNs = static_cast<std::uint64_t>(opt.warmupMs) * 1000000ull;
    const std::uint64_t warmupStart = cmp::NowNs();
    while (cmp::NowNs() - warmupStart < warmupNs)
    {
        if (!engine->hasQueueCapacity(first_stage, first_port))
        {
            std::this_thread::yield();
            continue;
        }
        auto packet = std::make_shared<PortData>();
        packet->id = seq++;
        packet->setParam(kTimestampKey, cmp::NowNs());
        (void)engine->pushInput(kSourceName, "output", packet);
    }

    const std::uint64_t windowStart = cmp::NowNs();
    const double cpuAtWindowStart = cmp::CpuSeconds();
    const EngineStatisticsSnapshot statsAtWindowStart = engine->statistics();
    measuring.store(true, std::memory_order_release);

    const std::uint64_t targetNs = static_cast<std::uint64_t>(opt.seconds * 1e9);
    while (cmp::NowNs() - windowStart < targetNs)
    {
        if (!engine->hasQueueCapacity(first_stage, first_port))
        {
            std::this_thread::yield();
            continue;
        }
        auto packet = std::make_shared<PortData>();
        packet->id = seq++;
        packet->setParam(kTimestampKey, cmp::NowNs());
        if (engine->pushInput(kSourceName, "output", packet))
        {
            ++pushedInWindow;
        }
        else
        {
            ++refusedInWindow;
        }
    }

    // The window ends when the producer stops, not when the last stage drains
    // (draining is shutdown cost for every implementation here).
    const std::uint64_t windowEnd = cmp::NowNs();
    const EngineStatisticsSnapshot statsAtWindowEnd = engine->statistics();
    measuring.store(false, std::memory_order_release);

    (void)engine->waitForDrain(0, std::chrono::milliseconds{10000});
    engine->stopStreaming(true);
    const double cpuSeconds = cmp::CpuSeconds() - cpuAtWindowStart;

    const double wallSeconds = windowEnd > windowStart ? static_cast<double>(windowEnd - windowStart) / 1e9 : 0.0;
    const std::uint64_t delivered = topology.sink->delivered();
    cmp::PrintCsv("ai-pipe", opt, wallSeconds, delivered, cpuSeconds, histogram);

    // ai-pipe drops instead of blocking, so the row carries how many frames
    // the engine dropped inside the window (queue drop callbacks counted by
    // the engine's own statistics, difference across the window).
    const std::uint64_t dropped =
        statsAtWindowEnd.total_dropped_frames - statsAtWindowStart.total_dropped_frames;
    std::printf("drop,%llu\n", static_cast<unsigned long long>(dropped));
    std::fflush(stdout);

    std::fprintf(stderr, "ai-pipe: pushed=%llu refused=%llu delivered=%llu dropped=%llu\n",
                 static_cast<unsigned long long>(pushedInWindow),
                 static_cast<unsigned long long>(refusedInWindow),
                 static_cast<unsigned long long>(delivered), static_cast<unsigned long long>(dropped));
    return 0;
}
