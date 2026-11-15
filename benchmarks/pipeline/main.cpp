#include  <nexusflow/Logging.hpp>
#include "nexusflow/Message.hpp"
#include "nexusflow/Module.hpp"
#include "nexusflow/Pipeline.hpp"
#include "nexusflow/PipelineBuilder.hpp"

#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace nexusflow;
using namespace std::chrono;

// --- 1. Define a specialized Message for benchmarking ---

struct BenchmarkPayloadMessage {
    steady_clock::time_point creationTime;
};

// --- 2. Define custom Modules for the benchmark ---

/**
 * @class SourceModule
 * @brief An "active" module that generates messages as fast as possible in its own thread.
 */
class SourceModule : public Module {
public:
    SourceModule(std::string name) : Module(std::move(name)) {}

    void Process(Message& _) override {
        Message msg(BenchmarkPayloadMessage{steady_clock::now()});
        Broadcast(msg);
    };
};

/**
 * @class PassThroughModule
 * @brief A simple module that immediately forwards any message it receives.
 */
class PassThroughModule : public Module {
public:
    PassThroughModule(std::string name) : Module(std::move(name)) {}

    void Process(Message& msg) override { Broadcast(msg); }
};

/**
 * @class SinkModule
 * @brief The final module that collects statistics.
 */
class SinkModule : public Module {
public:
    SinkModule(std::string name) : Module(std::move(name)) {}

    void Process(Message& msg) override {
        mMessageCount++;
        if (auto* data = msg.BorrowPtr<BenchmarkPayloadMessage>()) {
            auto latency = steady_clock::now() - data->creationTime;
            mTotalLatencyNs += duration_cast<nanoseconds>(latency).count();
        }
    }

    uint64_t GetMessageCount() const { return mMessageCount; }
    uint64_t GetTotalLatencyNs() const { return mTotalLatencyNs; }

private:
    std::atomic<uint64_t> mMessageCount{0};
    std::atomic<uint64_t> mTotalLatencyNs{0};
};

// --- 3. The Main Benchmark Function ---

namespace {

// Queue depth is the pipeline's own decision, so the benchmark states it on the
// command line instead of inventing a default: `--capacity N` (or --capacity=N).
// Sweeping this number is how the depth is chosen in the first place.
int ParseCapacity(int argc, char* argv[]) {
    int capacity = 64;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        std::string value;
        if (arg == "--capacity" && i + 1 < argc) {
            value = argv[++i];
        } else if (arg.rfind("--capacity=", 0) == 0) {
            value = arg.substr(std::string("--capacity=").size());
        } else {
            continue;
        }
        try {
            const int parsed = std::stoi(value);
            if (parsed > 0) {
                capacity = parsed;
            } else {
                LOG_WARN("--capacity {} ignored: it must be positive; using {}.", parsed, capacity);
            }
        } catch (const std::exception&) {
            LOG_WARN("--capacity {} is not a number; using {}.", value, capacity);
        }
    }
    return capacity;
}

} // namespace

int main(int argc, char* argv[]) {
    logger::InitializeGlobalLogger({logger::LogLevel::INFO});
    LOG_INFO("--- NexusFlow Performance Benchmark ---");

    const int BENCHMARK_DURATION_S = 10;
    const int edgeCapacity = ParseCapacity(argc, argv);
    LOG_INFO("Edge queue capacity: {}", edgeCapacity);

    try {
        // 1. Create module instances
        auto source = std::make_shared<SourceModule>("Source");
        auto pass1 = std::make_shared<PassThroughModule>("Pass1");
        auto pass2 = std::make_shared<PassThroughModule>("Pass2");
        auto sink = std::make_shared<SinkModule>("Sink");

        // 2. Build the Fan-out/Fan-in pipeline
        LOG_INFO("Building benchmark pipeline: Source -> (Pass1, Pass2) -> Sink");
        auto pipeline = PipelineBuilder()
                            .AddModule(source)
                            .AddModule(pass1)
                            .AddModule(pass2)
                            .AddModule(sink)
                            .Connect("Source", "Pass1")
                            .Connect("Source", "Pass2")
                            .Connect("Pass1", "Sink")
                            .Connect("Pass2", "Sink")
                            .Build(edgeCapacity);

        if (pipeline == nullptr) {
            LOG_CRITICAL("Failed to build pipeline.");
            return 1;
        }

        LOG_INFO("Initializing pipeline...");
        if (pipeline->Init() != ErrorCode::SUCCESS) {
            throw std::runtime_error("Pipeline initialization failed.");
            return -1;
        }

        auto startTime = steady_clock::now();

        LOG_INFO("Pipeline starting...");

        LOG_INFO("Starting pipeline for {} seconds...", BENCHMARK_DURATION_S);
        pipeline->Start();

        std::this_thread::sleep_for(seconds(BENCHMARK_DURATION_S));

        LOG_INFO("Pipeline stopping...");
        pipeline->Stop();

        auto endTime = steady_clock::now();

        LOG_INFO("De-initializing pipeline...");
        pipeline->DeInit();

        // 4. Calculate and report results
        double durationSeconds = duration<double>(endTime - startTime).count();
        uint64_t totalMessages = sink->GetMessageCount();
        uint64_t totalLatencyNs = sink->GetTotalLatencyNs();

        if (totalMessages > 0) {
            double throughput = totalMessages / durationSeconds;
            double avgLatencyUs = static_cast<double>(totalLatencyNs) / totalMessages / 1000.0;

            std::cout << "\n--- Benchmark Results ---\n";
            std::cout << "Edge Capacity:    " << edgeCapacity << " msg/edge\n";
            std::cout << "Total Duration:   " << durationSeconds << " s\n";
            std::cout << "Messages Processed: " << totalMessages << "\n";
            std::cout << "-------------------------\n";
            std::cout << "Throughput:       " << static_cast<uint64_t>(throughput) << " msg/s\n";
            std::cout << "Avg. Latency:     " << avgLatencyUs << " us\n";
            std::cout << "-------------------------\n";
        } else {
            LOG_WARN("No messages were processed during the benchmark.");
        }

    } catch (const std::exception& e) {
        LOG_CRITICAL("An exception occurred: {}", e.what());
        return 1;
    }

    return 0;
}