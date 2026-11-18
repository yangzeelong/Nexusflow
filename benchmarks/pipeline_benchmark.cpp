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

#if defined(_WIN32)
// windows.h defines a function-like `max` macro, which breaks the
// std::numeric_limits<...>::max() calls inside the framework headers below.
#ifndef NOMINMAX
#define NOMINMAX
#endif
#endif

#include "BenchmarkTopology.hpp"
#include "ProcessCpuTime.hpp"
#include "ProcessMemory.hpp"

#include <nexusflow/Logging.hpp>
#include <nexusflow/Module.hpp>
#include <nexusflow/Pipeline.hpp>
#include <nexusflow/PipelineContext.hpp>
#include <nexusflow/Stats.hpp>

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

#if defined(__linux__)
#include <sched.h>
#endif

#if defined(_WIN32)
#include <windows.h>
#include <mmsystem.h>
#pragma comment(lib, "winmm.lib")
#endif

using namespace nexusflow;
using namespace nexusflow_bench;
using Clock = std::chrono::steady_clock;

namespace {

struct Options {
    std::string topology = "diamond";  // diamond (default, matches the old harness) or linear
    std::string label = "local";
    int modules = 3;      // linear: total stages including source and sink
    int branches = 2;     // diamond: number of parallel branches
    int capacity = 4;     // queue depth per edge; 4 is where throughput saturates
    int durationSeconds = 10;
    int repeat = 3;
    int cpus = 0;  // 0 = let the OS use every logical CPU
    bool modulesGiven = false;
    bool branchesGiven = false;
    bool syncInputs = false;
    bool freshPayload = false;
    PayloadKind payload = PayloadKind::None;
    WorkloadSpec workload;
    /// Turn on the per-hop timing chain: the framework's own observability cost.
    bool timings = false;
    /// diamond: branch_0 sleeps this long per message while the others keep the
    /// workload, so fused groups stay half-complete and the cache grows.
    int stragglerUs = 0;
    /// inject: "block" (Push waits for room) or "try" (TryPush counts refusals).
    std::string pushMode = "block";
    /// Framework log level; TRACE writes one line per message on the hot path.
    std::string logLevel = "warn";
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

[[noreturn]] void Fail(const std::string& message) {
    std::cerr << "pipeline_benchmark: " << message << "\n";
    std::cerr << "try --help for the accepted options\n";
    std::exit(2);
}

void PrintUsage() {
    std::cout
        << "usage: pipeline_benchmark [options]\n"
           "  --topology diamond|linear|inject  default diamond (source -> branches -> sink);\n"
           "                             inject = passive source -> stages -> sink, fed by Pipeline::Push()\n"
           "  --modules N                linear stages incl. source and sink (default 3)\n"
           "  --branches N               diamond width (default 2)\n"
           "  --capacity N               queue depth per edge (default 4)\n"
           "  --payload none|1080p-rgb|1080p-yuv420|2k-rgb|2k-yuv420  default none\n"
           "  --payload-fresh            allocate a frame per message instead of reusing one\n"
           "  --workload none|delay|compute|memory  cost added by each intermediate stage (default none)\n"
           "  --delay-us N               workload=delay: sleep per message (default 100)\n"
           "  --compute-iters N          workload=compute: sin/cos iterations per message (default 5000)\n"
           "  --memory-bytes N           workload=memory: bytes copied per message (default 65536)\n"
           "  --sync-inputs              run the diamond sink through the fusion path\n"
           "  --straggler-us N           diamond: branch_0 sleeps N us per message (fused cache growth)\n"
           "  --push-mode block|try      inject: blocking Push (default) or TryPush counting refusals\n"
           "  --timings                  enable the per-hop timing chain (EnableModuleTimings)\n"
           "  --log-level LEVEL          trace|debug|info|warn|err|off (default warn)\n"
           "  --duration S               seconds per run (default 10)\n"
           "  --repeat R                 measured runs after one warm-up (default 3)\n"
           "  --cpus N                   pin the process to the first N logical CPUs (default 0 = all)\n"
           "  --label TEXT               written into every CSV line\n";
}

// Accepts `--flag value` and `--flag=value`; returns true when it consumed an arg.
bool TakeValue(const std::string& arg, const std::string& flag, int argc, char* argv[], int& index, std::string& out) {
    if (arg == flag) {
        if (index + 1 >= argc) {
            Fail("missing value for " + flag);
        }
        out = argv[++index];
        return true;
    }
    const std::string withEquals = flag + "=";
    if (StartsWith(arg, withEquals)) {
        out = arg.substr(withEquals.size());
        return true;
    }
    return false;
}

bool ParseInt(const std::string& text, int* out) {
    std::size_t consumed = 0;
    try {
        const int value = std::stoi(text, &consumed);
        if (consumed != text.size()) {
            return false;
        }
        *out = value;
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

bool ParsePayload(const std::string& text, PayloadKind* out) {
    if (text == "none") {
        *out = PayloadKind::None;
    } else if (text == "1080p-rgb") {
        *out = PayloadKind::Rgb1080p;
    } else if (text == "1080p-yuv420") {
        *out = PayloadKind::Yuv420_1080p;
    } else if (text == "2k-rgb") {
        *out = PayloadKind::Rgb2k;
    } else if (text == "2k-yuv420") {
        *out = PayloadKind::Yuv420_2k;
    } else {
        return false;
    }
    return true;
}

bool ParseWorkload(const std::string& text, WorkloadKind* out) {
    if (text == "none") {
        *out = WorkloadKind::None;
    } else if (text == "delay") {
        *out = WorkloadKind::Delay;
    } else if (text == "compute") {
        *out = WorkloadKind::Compute;
    } else if (text == "memory") {
        *out = WorkloadKind::Memory;
    } else {
        return false;
    }
    return true;
}

Options ParseOptions(int argc, char* argv[]) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        std::string value;
        if (TakeValue(arg, "--topology", argc, argv, i, value)) {
            if (value != "linear" && value != "diamond" && value != "inject") {
                Fail("unknown topology: " + value);
            }
            options.topology = value;
        } else if (TakeValue(arg, "--label", argc, argv, i, value)) {
            options.label = value;
        } else if (TakeValue(arg, "--payload", argc, argv, i, value)) {
            if (!ParsePayload(value, &options.payload)) {
                Fail("unknown payload: " + value);
            }
        } else if (TakeValue(arg, "--workload", argc, argv, i, value)) {
            if (!ParseWorkload(value, &options.workload.kind)) {
                Fail("unknown workload: " + value);
            }
        } else if (TakeValue(arg, "--delay-us", argc, argv, i, value)) {
            if (!ParseInt(value, &options.workload.delayUs)) {
                Fail("--delay-us expects an integer, got: " + value);
            }
        } else if (TakeValue(arg, "--compute-iters", argc, argv, i, value)) {
            if (!ParseInt(value, &options.workload.computeIterations)) {
                Fail("--compute-iters expects an integer, got: " + value);
            }
        } else if (TakeValue(arg, "--memory-bytes", argc, argv, i, value)) {
            int bytes = 0;
            if (!ParseInt(value, &bytes) || bytes < 1) {
                Fail("--memory-bytes expects a positive integer, got: " + value);
            }
            options.workload.memoryBytes = static_cast<std::size_t>(bytes);
        } else if (TakeValue(arg, "--modules", argc, argv, i, value)) {
            if (!ParseInt(value, &options.modules)) {
                Fail("--modules expects an integer, got: " + value);
            }
            options.modulesGiven = true;
        } else if (TakeValue(arg, "--branches", argc, argv, i, value)) {
            if (!ParseInt(value, &options.branches)) {
                Fail("--branches expects an integer, got: " + value);
            }
            options.branchesGiven = true;
        } else if (TakeValue(arg, "--capacity", argc, argv, i, value)) {
            if (!ParseInt(value, &options.capacity)) {
                Fail("--capacity expects an integer, got: " + value);
            }
        } else if (TakeValue(arg, "--duration", argc, argv, i, value)) {
            if (!ParseInt(value, &options.durationSeconds)) {
                Fail("--duration expects an integer, got: " + value);
            }
        } else if (TakeValue(arg, "--repeat", argc, argv, i, value)) {
            if (!ParseInt(value, &options.repeat)) {
                Fail("--repeat expects an integer, got: " + value);
            }
        } else if (TakeValue(arg, "--cpus", argc, argv, i, value)) {
            if (!ParseInt(value, &options.cpus)) {
                Fail("--cpus expects an integer, got: " + value);
            }
        } else if (TakeValue(arg, "--straggler-us", argc, argv, i, value)) {
            if (!ParseInt(value, &options.stragglerUs) || options.stragglerUs < 0) {
                Fail("--straggler-us expects a non-negative integer, got: " + value);
            }
        } else if (TakeValue(arg, "--push-mode", argc, argv, i, value)) {
            if (value != "block" && value != "try") {
                Fail("unknown --push-mode: " + value);
            }
            options.pushMode = value;
        } else if (TakeValue(arg, "--log-level", argc, argv, i, value)) {
            if (value != "trace" && value != "debug" && value != "info" && value != "warn" && value != "err" &&
                value != "off") {
                Fail("unknown --log-level: " + value);
            }
            options.logLevel = value;
        } else if (arg == "--timings") {
            options.timings = true;
        } else if (arg == "--sync-inputs") {
            options.syncInputs = true;
        } else if (arg == "--payload-fresh") {
            options.freshPayload = true;
        } else if (arg == "--help" || arg == "-h") {
            PrintUsage();
            std::exit(0);
        } else {
            Fail("unknown argument: " + arg);
        }
    }

    if (options.topology == "inject" && options.modules < 2) {
        Fail("--topology inject needs --modules >= 2 (passive source, sink)");
    }
    if (options.topology == "linear") {
        if (options.modules < 2) {
            Fail("--topology linear needs --modules >= 2 (source, sink)");
        }
        if (options.branchesGiven) {
            Fail("--branches only applies to --topology diamond");
        }
        if (options.syncInputs) {
            Fail("--sync-inputs only applies to --topology diamond");
        }
    } else if (options.topology == "diamond") {
        if (options.branches < 2) {
            Fail("--topology diamond needs --branches >= 2");
        }
        if (options.modulesGiven) {
            Fail("--modules only applies to --topology linear and inject");
        }
    }
    if (options.topology != "inject" && options.pushMode != "block") {
        Fail("--push-mode only applies to --topology inject");
    }
    if (options.topology != "diamond" && options.stragglerUs > 0) {
        Fail("--straggler-us only applies to --topology diamond");
    }
    if (options.topology != "diamond" && options.branchesGiven) {
        Fail("--branches only applies to --topology diamond");
    }
    if (options.capacity < 1) {
        Fail("--capacity must be >= 1");
    }
    if (options.durationSeconds < 1) {
        Fail("--duration must be >= 1");
    }
    if (options.repeat < 1) {
        Fail("--repeat must be >= 1");
    }
    if (options.cpus < 0) {
        Fail("--cpus must be >= 0");
    }
    if (options.workload.delayUs < 0) {
        Fail("--delay-us must be >= 0");
    }
    if (options.workload.computeIterations < 0) {
        Fail("--compute-iters must be >= 0");
    }
    return options;
}

int LogicalCpuCount() {
    const unsigned count = std::thread::hardware_concurrency();
    return count == 0 ? 1 : static_cast<int>(count);
}

/// Restricts this process to the first `count` logical CPUs; 0 leaves it alone.
/// Called before the first pipeline starts, so every worker thread inherits the mask.
bool PinToCpus(int count, std::string* error) {
    if (count == 0) {
        return true;
    }
    const int available = LogicalCpuCount();
    if (count > available) {
        *error = "--cpus " + std::to_string(count) + " exceeds the " + std::to_string(available) + " logical CPUs";
        return false;
    }
#if defined(_WIN32)
    DWORD_PTR mask = 0;
    for (int cpu = 0; cpu < count; ++cpu) {
        mask |= static_cast<DWORD_PTR>(1) << cpu;
    }
    if (!SetProcessAffinityMask(GetCurrentProcess(), mask)) {
        *error = "SetProcessAffinityMask failed (error " + std::to_string(GetLastError()) + ")";
        return false;
    }
    return true;
#elif defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    for (int cpu = 0; cpu < count; ++cpu) {
        CPU_SET(static_cast<std::size_t>(cpu), &set);
    }
    if (sched_setaffinity(0, sizeof(set), &set) != 0) {
        *error = "sched_setaffinity failed";
        return false;
    }
    return true;
#else
    *error = "--cpus is only implemented on Windows and Linux";
    return false;
#endif
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
    /// Whole-process resident memory, sampled every 25 ms during the run, in
    /// MiB. Edge memory is `capacity * frame * edges` on paper, but frames and
    /// the fusion sink's messageCache are invisible to every counter, so this is
    /// the only witness for them.
    double rssStartMiB = 0.0;
    double rssPeakMiB = 0.0;
    double rssEndMiB = 0.0;
    /// How long Stop() took: draining the queues and joining every worker.
    double stopMs = 0.0;
    /// Deepest backlog any single edge reached during the run.
    std::uint64_t edgeDepthMax = 0;
    /// Fused groups dropped because an input arrived after the fusion timeout.
    std::uint64_t fusionExpired = 0;
    /// inject only: Push/TryPush attempts and refusals (TryPush found the edge full).
    std::uint64_t attempts = 0;
    std::uint64_t refused = 0;
};

double Median(std::vector<double> values) {
    if (values.empty()) {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    return values[values.size() / 2];
}

BuiltTopology BuildTopologyFor(const Options& options, const PayloadSpec& spec, std::size_t capacity) {
    if (options.topology == "inject") {
        return BuildInjection(options.modules, capacity, options.workload);
    }
    if (options.topology == "linear") {
        return BuildLinear(options.modules, capacity, spec, options.freshPayload, options.workload);
    }
    return BuildDiamond(options.branches, capacity, spec, options.freshPayload, options.syncInputs, options.workload,
                        options.stragglerUs);
}

logger::LogLevel LogLevelFromName(const std::string& name) {
    if (name == "trace") {
        return logger::LogLevel::TRACE;
    }
    if (name == "debug") {
        return logger::LogLevel::DEBUG;
    }
    if (name == "info") {
        return logger::LogLevel::INFO;
    }
    if (name == "err") {
        return logger::LogLevel::ERR;
    }
    if (name == "off") {
        return logger::LogLevel::OFF;
    }
    return logger::LogLevel::WARN;
}

/// Samples the process's resident set while the pipeline runs.
///
/// Only the driving thread calls this: in the active topologies the main thread
/// has nothing to do but wait, and in injection mode it is the producer, so no
/// sampling thread is needed. A 25 ms interval can miss a spike shorter than
/// that, which is why the report calls this a sampled peak.
void SampleResident(std::size_t* peak) {
    const std::size_t resident = ResidentBytes();
    if (resident > *peak) {
        *peak = resident;
    }
}

/// Feeds the passive source at whatever rate the pipeline accepts, for the run's
/// duration, and counts attempts and refusals. This is the producer an external
/// decoder or socket would be: it owns the pacing, the pipeline only consumes.
void RunInjectionWindow(const Options& options, const PayloadSpec& spec, BuiltTopology& built, std::size_t* rssPeak,
                        std::uint64_t* attempts, std::uint64_t* refused) {
    const bool blocking = options.pushMode == "block";
    const Clock::time_point started = Clock::now();
    const Clock::time_point deadline = started + std::chrono::seconds(options.durationSeconds);
    Clock::time_point nextSample = started + std::chrono::milliseconds(25);

    // One frame reused by every pushed message, unless --payload-fresh asks for
    // the decoder shape where each hand-off owns a fresh picture.
    std::shared_ptr<const BenchmarkFrame> sharedFrame;
    if (!spec.empty()) {
        sharedFrame = MakeBenchmarkFrame(spec);
    }

    while (true) {
        const Clock::time_point now = Clock::now();
        if (now >= nextSample) {
            SampleResident(rssPeak);
            nextSample = now + std::chrono::milliseconds(25);
        }
        if (now >= deadline) {
            break;
        }

        BenchmarkPayload payload;
        payload.creationTime = now;
        if (!spec.empty()) {
            payload.frame = options.freshPayload ? MakeBenchmarkFrame(spec) : sharedFrame;
        }

        const Message message = MakeMessage(payload);
        ++(*attempts);
        if (blocking) {
            // Push() waits for room, so the producer's rate is the consumer's rate.
            if (built.pipeline->Push("source", message) != PushResult::kOk) {
                --(*attempts);
                break;
            }
            continue;
        }

        bool timedOut = false;
        while (true) {
            const PushResult result = built.pipeline->TryPush("source", message);
            if (result != PushResult::kQueueFull) {
                if (result != PushResult::kOk) {
                    --(*attempts);
                }
                break;
            }
            ++(*refused);
            if (Clock::now() >= deadline) {
                --(*attempts);
                timedOut = true;
                break;
            }
            std::this_thread::yield();
        }
        if (timedOut) {
            break;
        }
    }
}

Sample RunOnce(const Options& options, const PayloadSpec& spec, std::size_t capacity) {
    BuiltTopology built = BuildTopologyFor(options, spec, capacity);
    if (built.pipeline == nullptr) {
        std::cerr << "failed to build the " << options.topology << " topology\n";
        std::exit(1);
    }
    if (built.pipeline->Init() != ErrorCode::SUCCESS) {
        std::cerr << "pipeline Init() failed\n";
        std::exit(1);
    }

    if (options.timings) {
        // Before Start(): every module reads the switch on its first message.
        built.pipeline->GetContext()->EnableModuleTimings();
    }

    const std::size_t rssStart = ResidentBytes();
    std::size_t rssPeak = rssStart;
    std::uint64_t injectionAttempts = 0;
    std::uint64_t injectionRefused = 0;
    const double cpuBefore = ProcessCpuSeconds();
    const Clock::time_point started = Clock::now();
    if (built.pipeline->Start() != ErrorCode::SUCCESS) {
        std::cerr << "pipeline Start() failed\n";
        std::exit(1);
    }
    if (options.topology == "inject") {
        // The main thread is the producer here, so it cannot also just sleep.
        RunInjectionWindow(options, spec, built, &rssPeak, &injectionAttempts, &injectionRefused);
    } else {
        const auto deadline = started + std::chrono::seconds(options.durationSeconds);
        while (Clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
            SampleResident(&rssPeak);
        }
    }

    // Stop() drains the queues and joins the workers. Take the clock after it
    // returns: wall time then covers the same interval as the counters, which
    // are only safe to read once the sink thread has exited.
    const Clock::time_point stopStarted = Clock::now();
    built.pipeline->Stop();
    const Clock::time_point finished = Clock::now();
    const double cpuAfter = ProcessCpuSeconds();
    const std::size_t rssEnd = ResidentBytes();

    // The snapshot's own view of the run: per-edge water marks and the fusion
    // drops. Taken after Stop() so the counters and water marks are final.
    const PipelineSnapshot snapshot = built.pipeline->GetSnapshot();

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
    sample.rssStartMiB = static_cast<double>(rssStart) / (1024.0 * 1024.0);
    sample.rssPeakMiB = static_cast<double>(rssPeak) / (1024.0 * 1024.0);
    sample.rssEndMiB = static_cast<double>(rssEnd) / (1024.0 * 1024.0);
    sample.stopMs = std::chrono::duration<double, std::milli>(finished - stopStarted).count();
    for (const EdgeSnapshot& edge : snapshot.edges) {
        if (static_cast<std::uint64_t>(edge.maxSize) > sample.edgeDepthMax) {
            sample.edgeDepthMax = static_cast<std::uint64_t>(edge.maxSize);
        }
    }
    for (const ModuleSnapshot& module : snapshot.modules) {
        sample.fusionExpired += module.fusionExpiredTotal;
    }
    sample.attempts = injectionAttempts;
    sample.refused = injectionRefused;

    built.pipeline->DeInit();
    return sample;
}

void PrintCsv(const Options& options, std::size_t capacity, int repetition, const Sample& sample) {
    std::cout << "csv," << options.label << ',' << Platform() << ',' << options.topology << ',' << options.modules << ','
              << options.branches << ',' << capacity << ',' << NameOf(options.payload) << ','
              << NameOf(options.workload.kind) << ',' << WorkloadParameter(options.workload) << ','
              << (options.freshPayload ? 1 : 0) << ',' << (options.syncInputs ? 1 : 0) << ',' << options.cpus << ','
              << options.durationSeconds << ',' << repetition << ',' << sample.delivered << ','
              << static_cast<std::uint64_t>(sample.throughput)
              << ',' << sample.cpuSeconds << ',' << sample.cores << ',' << sample.latencyAvgUs << ','
              << sample.latencyP50Us << ',' << sample.latencyP95Us << ',' << sample.latencyP99Us << ','
              << sample.latencyMinUs << ',' << sample.latencyMaxUs << ',' << (options.timings ? 1 : 0) << ','
              << options.logLevel << ',' << options.stragglerUs << ',' << options.pushMode << ','
              << sample.rssStartMiB << ',' << sample.rssPeakMiB << ',' << sample.rssEndMiB << ',' << sample.stopMs << ','
              << sample.edgeDepthMax << ',' << sample.fusionExpired << ',' << sample.attempts << ',' << sample.refused << '\n';
}

} // namespace

/// The Windows scheduler's default timer tick is 15.6 ms, which turns a
/// `sleep_for(100 us)` into a 15.6 ms sleep. The delay workload measures a stage
/// that waits, so ask for the finest tick while the benchmark runs and restore it
/// on exit. POSIX sleep resolution is already fine.
struct TimerResolutionGuard {
#if defined(_WIN32)
    TimerResolutionGuard() { timeBeginPeriod(1); }
    ~TimerResolutionGuard() { timeEndPeriod(1); }
#endif
};

int main(int argc, char* argv[]) {
    const Options options = ParseOptions(argc, argv);
    logger::InitializeGlobalLogger({LogLevelFromName(options.logLevel)});
    TimerResolutionGuard timerResolution;

    std::string pinError;
    if (!PinToCpus(options.cpus, &pinError)) {
        std::cerr << "pipeline_benchmark: " << pinError << "\n";
        return 2;
    }

    const PayloadSpec spec = SpecFor(options.payload);
    const std::size_t capacity = static_cast<std::size_t>(options.capacity);

    std::cout << "--- NexusFlow pipeline benchmark ---\n";
    if (options.cpus > 0) {
        std::cout << "affinity: first " << options.cpus << " of " << LogicalCpuCount() << " logical CPUs\n";
    } else {
        std::cout << "affinity: all " << LogicalCpuCount() << " logical CPUs\n";
    }
    std::cout << "topology: " << options.topology << "  modules=" << options.modules << "  branches=" << options.branches
              << "  sync_inputs=" << (options.syncInputs ? "yes" : "no") << "  timings="
              << (options.timings ? "on" : "off") << "  log=" << options.logLevel << "\n";
    std::cout << "payload: " << NameOf(options.payload) << " (" << spec.bytes << " bytes/frame, "
              << (options.freshPayload ? "fresh per message" : "one buffer reused") << ")  capacity=" << capacity
              << "  streams=" << options.repeat << " x " << options.durationSeconds << "s (+1 warm-up)\n";
    if (options.stragglerUs > 0) {
        std::cout << "straggler: branch_0 sleeps " << options.stragglerUs
                  << " us per message (others keep the workload)\n";
    }
    if (options.topology == "inject") {
        std::cout << "injection: " << options.pushMode << " push from the main thread\n";
    }
    if (!options.workload.empty()) {
        std::cout << "workload: " << NameOf(options.workload.kind) << " (parameter " << WorkloadParameter(options.workload)
                  << ") on every intermediate stage\n";
    }
    if (!spec.empty()) {
        // One queue per hop on a chain; a diamond has one queue into and one
        // out of every branch.
        const std::size_t edges = options.topology == "diamond"
                                      ? static_cast<std::size_t>(options.branches) * 2
                                      : static_cast<std::size_t>(options.modules) - 1;
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
    std::vector<double> rssPeaks;
    std::vector<double> stops;
    std::vector<double> attemptCounts;
    std::vector<double> refusedCounts;
    for (const Sample& sample : samples) {
        throughputs.push_back(sample.throughput);
        p50s.push_back(sample.latencyP50Us);
        p95s.push_back(sample.latencyP95Us);
        p99s.push_back(sample.latencyP99Us);
        mins.push_back(sample.latencyMinUs);
        maxs.push_back(sample.latencyMaxUs);
        cpus.push_back(sample.cpuSeconds);
        cores.push_back(sample.cores);
        rssPeaks.push_back(sample.rssPeakMiB);
        stops.push_back(sample.stopMs);
        attemptCounts.push_back(static_cast<double>(sample.attempts));
        refusedCounts.push_back(static_cast<double>(sample.refused));
    }
    const double medianThroughput = Median(throughputs);
    const double medianP50 = Median(p50s);
    const double medianP95 = Median(p95s);
    const double medianP99 = Median(p99s);
    const double medianMin = Median(mins);
    const double medianMax = Median(maxs);
    const double medianCpu = Median(cpus);
    const double medianCores = Median(cores);
    const double medianRssPeak = Median(rssPeaks);
    const double medianStop = Median(stops);
    const double medianAttempts = Median(attemptCounts);
    const double medianRefused = Median(refusedCounts);

    std::cout << "-------------------------\n";
    std::cout << "median throughput: " << static_cast<std::uint64_t>(medianThroughput) << " msg/s\n";
    std::cout << "median latency:    p50 " << medianP50 << " / p95 " << medianP95 << " / p99 " << medianP99 << " / min "
              << medianMin << " / max " << medianMax << " us\n";
    std::cout << "median cpu time:   " << medianCpu << " s over " << options.durationSeconds << " s wall = "
              << medianCores << " cores\n";
    std::cout << "median rss peak:   " << medianRssPeak << " MiB\n";
    std::cout << "median stop cost:  " << medianStop << " ms (drain + join)\n";
    if (options.topology == "inject") {
        std::cout << "median injection:  " << static_cast<std::uint64_t>(medianAttempts) << " attempts, "
                  << static_cast<std::uint64_t>(medianRefused) << " refused ("
                  << (medianAttempts > 0.0 ? medianRefused / medianAttempts : 0.0) << " retries per attempt while the edge was full)\n";
    }
    std::cout << "-------------------------\n";
    return 0;
}
