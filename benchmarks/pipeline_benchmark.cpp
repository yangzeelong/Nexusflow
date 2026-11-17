// End-to-end throughput and latency harness.
//
// One run builds a pipeline, runs it for --duration seconds, then reports what
// the sink delivered. Repetitions are separate pipelines (queues are one-shot
// after Stop), the first one is thrown away as warm-up, and the median of the
// rest is what the report quotes.
//
// Output is two things at once:
//   * one `csv,...` line per measured repetition, so a matrix run can be pasted
//     into a spreadsheet (grep '^csv,' over a batch of runs);
//   * a human-readable block with the median, for reading by eye.
//
// Delivered means "the sink processed this many messages". For a fused sink that
// is one per logical frame; for a fan-in that does not synchronise it is one per
// branch message. The line says which topology produced it.

#include "BenchmarkTopology.hpp"
#include "ProcessCpuTime.hpp"

#include <nexusflow/Logging.hpp>
#include <nexusflow/Module.hpp>
#include <nexusflow/Pipeline.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace nexusflow;
using namespace nexusflow_bench;
using Clock = std::chrono::steady_clock;

namespace {

struct Options {
    std::string topology = "diamond";  // diamond (default, matches the old harness) or linear
    std::string label = "local";
    int modules = 3;      // linear: total stages including source and sink
    int branches = 2;     // diamond: number of parallel branches
    int capacity = 0;     // 0 = derive from payload
    int durationSeconds = 10;
    int repeat = 3;
    bool syncInputs = false;
    bool freshPayload = false;
    PayloadKind payload = PayloadKind::None;
};

const char* Platform() {
#if defined(_WIN32)
    return "windows-msvc";
#else
    return "linux-gcc";
#endif
}

bool StartsWith(const std::string& text, const std::string& prefix) {
    return text.size() >= prefix.size() && text.compare(0, prefix.size(), prefix) == 0;
}

// Accepts `--flag value` and `--flag=value`; returns true when it consumed an arg.
bool TakeValue(const std::string& arg, const std::string& flag, int argc, char* argv[], int& index, std::string& out) {
    if (arg == flag) {
        if (index + 1 < argc) {
            out = argv[++index];
            return true;
        }
        return false;
    }
    const std::string withEquals = flag + "=";
    if (StartsWith(arg, withEquals)) {
        out = arg.substr(withEquals.size());
        return true;
    }
    return false;
}

int ToInt(const std::string& text, int fallback) {
    try {
        const int value = std::stoi(text);
        return value;
    } catch (const std::exception&) {
        return fallback;
    }
}

PayloadKind ParsePayload(const std::string& text) {
    if (text == "1080p-rgb") return PayloadKind::Rgb1080p;
    if (text == "1080p-yuv420") return PayloadKind::Yuv420_1080p;
    if (text == "2k-rgb") return PayloadKind::Rgb2k;
    if (text == "2k-yuv420") return PayloadKind::Yuv420_2k;
    return PayloadKind::None;
}

Options ParseOptions(int argc, char* argv[]) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        std::string value;
        if (TakeValue(arg, "--topology", argc, argv, i, value)) {
            options.topology = value;
        } else if (TakeValue(arg, "--label", argc, argv, i, value)) {
            options.label = value;
        } else if (TakeValue(arg, "--payload", argc, argv, i, value)) {
            options.payload = ParsePayload(value);
        } else if (TakeValue(arg, "--modules", argc, argv, i, value)) {
            options.modules = ToInt(value, options.modules);
        } else if (TakeValue(arg, "--branches", argc, argv, i, value)) {
            options.branches = ToInt(value, options.branches);
        } else if (TakeValue(arg, "--capacity", argc, argv, i, value)) {
            options.capacity = ToInt(value, options.capacity);
        } else if (TakeValue(arg, "--duration", argc, argv, i, value)) {
            options.durationSeconds = ToInt(value, options.durationSeconds);
        } else if (TakeValue(arg, "--repeat", argc, argv, i, value)) {
            options.repeat = ToInt(value, options.repeat);
        } else if (arg == "--sync-inputs") {
            options.syncInputs = true;
        } else if (arg == "--payload-fresh") {
            options.freshPayload = true;
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "usage: pipeline_benchmark [--topology diamond|linear] [--modules N] "
                         "[--branches N] [--capacity N] [--payload none|1080p-rgb|1080p-yuv420|2k-rgb|2k-yuv420] "
                         "[--duration S] [--repeat R] [--sync-inputs] [--payload-fresh] [--label TEXT]\n";
            std::exit(0);
        }
    }
    if (options.durationSeconds < 1) {
        options.durationSeconds = 1;
    }
    if (options.repeat < 1) {
        options.repeat = 1;
    }
    return options;
}

struct Sample {
    double throughput = 0.0;  // delivered messages per second
    std::uint64_t delivered = 0;
    double wallSeconds = 0.0;
    double cpuSeconds = 0.0;
    double latencyAvgUs = 0.0;
    double latencyP50Us = 0.0;
    double latencyP95Us = 0.0;
    double latencyP99Us = 0.0;
    double latencyMinUs = 0.0;
    double latencyMaxUs = 0.0;
    /// CPU seconds / wall seconds: how many cores this configuration burns.
    double cores = 0.0;
};

double Median(std::vector<double> values) {
    if (values.empty()) {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    return values[values.size() / 2];
}

Sample RunOnce(const Options& options, const PayloadSpec& spec, std::size_t capacity) {
    BuiltTopology built = options.topology == "linear"
                              ? BuildLinear(options.modules, capacity, spec, options.freshPayload)
                              : BuildDiamond(options.branches, capacity, spec, options.freshPayload, options.syncInputs);
    if (built.pipeline == nullptr) {
        std::cerr << "failed to build the " << options.topology << " topology\n";
        std::exit(1);
    }
    if (built.pipeline->Init() != ErrorCode::SUCCESS) {
        std::cerr << "pipeline Init() failed\n";
        std::exit(1);
    }

    const double cpuBefore = ProcessCpuSeconds();
    const Clock::time_point started = Clock::now();
    if (built.pipeline->Start() != ErrorCode::SUCCESS) {
        std::cerr << "pipeline Start() failed\n";
        std::exit(1);
    }
    std::this_thread::sleep_for(std::chrono::seconds(options.durationSeconds));
    const Clock::time_point finished = Clock::now();

    // Capture the counters after Stop(): the sink's histogram is written by its
    // worker thread, so reading it while the pipeline still runs would race.
    built.pipeline->Stop();
    const double cpuAfter = ProcessCpuSeconds();

    const std::uint64_t delivered = built.sink->Count();
    const LatencyHistogram& latency = built.sink->Latency();

    Sample sample;
    sample.wallSeconds = std::chrono::duration<double>(finished - started).count();
    sample.cpuSeconds = cpuAfter - cpuBefore;
    sample.delivered = delivered;
    sample.throughput = sample.wallSeconds > 0.0 ? static_cast<double>(delivered) / sample.wallSeconds : 0.0;
    sample.latencyAvgUs = latency.MeanNs() / 1000.0;
    sample.latencyP50Us = latency.PercentileNs(50.0) / 1000.0;
    sample.latencyP95Us = latency.PercentileNs(95.0) / 1000.0;
    sample.latencyP99Us = latency.PercentileNs(99.0) / 1000.0;
    sample.latencyMinUs = static_cast<double>(latency.MinNs()) / 1000.0;
    sample.latencyMaxUs = static_cast<double>(latency.MaxNs()) / 1000.0;
    sample.cores = sample.wallSeconds > 0.0 ? sample.cpuSeconds / sample.wallSeconds : 0.0;

    built.pipeline->DeInit();
    return sample;
}

void PrintCsv(const Options& options, std::size_t capacity, int repetition, const Sample& sample) {
    std::cout << "csv," << options.label << ',' << Platform() << ',' << options.topology << ',' << options.modules << ','
              << options.branches << ',' << capacity << ',' << NameOf(options.payload) << ','
              << (options.freshPayload ? 1 : 0) << ',' << (options.syncInputs ? 1 : 0) << ',' << options.durationSeconds
              << ',' << repetition << ',' << sample.delivered << ',' << static_cast<std::uint64_t>(sample.throughput)
              << ',' << sample.cpuSeconds << ',' << sample.cores << ',' << sample.latencyAvgUs << ','
              << sample.latencyP50Us << ',' << sample.latencyP95Us << ',' << sample.latencyP99Us << ','
              << sample.latencyMinUs << ',' << sample.latencyMaxUs << '\n';
}

} // namespace

int main(int argc, char* argv[]) {
    const Options options = ParseOptions(argc, argv);
    logger::InitializeGlobalLogger({logger::LogLevel::WARN});

    const PayloadSpec spec = SpecFor(options.payload);
    // Queue depth times payload size is the memory each edge holds: with 1080p
    // RGB a capacity of 64 would park ~380 MiB per edge, so payload runs default
    // to a shallow queue unless the caller asks for a specific one.
    const std::size_t capacity =
        options.capacity > 0 ? static_cast<std::size_t>(options.capacity) : (spec.empty() ? 64u : 4u);

    std::cout << "--- NexusFlow pipeline benchmark ---\n";
    std::cout << "topology: " << options.topology << "  modules=" << options.modules << "  branches=" << options.branches
              << "  sync_inputs=" << (options.syncInputs ? "yes" : "no") << "\n";
    std::cout << "payload: " << NameOf(options.payload) << " (" << spec.bytes << " bytes/frame, "
              << (options.freshPayload ? "fresh per message" : "one buffer reused") << ")  capacity=" << capacity
              << "  streams=" << options.repeat << " x " << options.durationSeconds << "s (+1 warm-up)\n";
    if (!spec.empty()) {
        const std::size_t edges = options.topology == "linear" ? static_cast<std::size_t>(options.modules)
                                                               : static_cast<std::size_t>(options.branches) * 2;
        std::cout << "edge memory at full depth: ~" << (capacity * spec.bytes * edges) / (1024 * 1024) << " MiB\n";
    }

    // Warm-up: first run pays page faults, thread start-up and cache warming.
    const Sample warmUp = RunOnce(options, spec, capacity);
    std::cout << "warm-up: " << static_cast<std::uint64_t>(warmUp.throughput) << " msg/s (discarded)\n";

    std::vector<Sample> samples;
    for (int repetition = 1; repetition <= options.repeat; ++repetition) {
        const Sample sample = RunOnce(options, spec, capacity);
        samples.push_back(sample);
        PrintCsv(options, capacity, repetition, sample);
    }

    std::vector<double> throughputs;
    std::vector<double> p50s;
    std::vector<double> p95s;
    std::vector<double> p99s;
    std::vector<double> mins;
    std::vector<double> maxs;
    std::vector<double> cpus;
    std::vector<double> cores;
    for (const Sample& sample : samples) {
        throughputs.push_back(sample.throughput);
        p50s.push_back(sample.latencyP50Us);
        p95s.push_back(sample.latencyP95Us);
        p99s.push_back(sample.latencyP99Us);
        mins.push_back(sample.latencyMinUs);
        maxs.push_back(sample.latencyMaxUs);
        cpus.push_back(sample.cpuSeconds);
        cores.push_back(sample.cores);
    }
    const double medianThroughput = Median(throughputs);
    const double medianP50 = Median(p50s);
    const double medianP95 = Median(p95s);
    const double medianP99 = Median(p99s);
    const double medianMin = Median(mins);
    const double medianMax = Median(maxs);
    const double medianCpu = Median(cpus);
    const double medianCores = Median(cores);

    std::cout << "-------------------------\n";
    std::cout << "median throughput: " << static_cast<std::uint64_t>(medianThroughput) << " msg/s\n";
    std::cout << "median latency:    p50 " << medianP50 << " / p95 " << medianP95 << " / p99 " << medianP99 << " / min "
              << medianMin << " / max " << medianMax << " us\n";
    std::cout << "median cpu time:   " << medianCpu << " s over " << options.durationSeconds << " s wall = "
              << medianCores << " cores\n";
    std::cout << "-------------------------\n";
    return 0;
}
