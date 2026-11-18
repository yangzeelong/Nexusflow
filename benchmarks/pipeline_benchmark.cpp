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
#include "ProcessMemory.hpp"
#include "RawPipeline.hpp"

#include <nexusflow/Logging.hpp>
#include <nexusflow/Module.hpp>
#include <nexusflow/Pipeline.hpp>
#include <nexusflow/PipelineContext.hpp>

#if defined(_WIN32)
#include <mmsystem.h>
#pragma comment(lib, "winmm.lib")
#elif defined(__linux__)
#include <sched.h>
#endif

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
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
    std::string topology = "diamond";  // diamond (default), linear, or inject
    std::string label = "local";
    /// `framework`, or one of the bare baselines that subtract out (HARNESS.md §3):
    /// `raw` (plain struct message, nothing allocated) and `raw-alloc` (one
    /// heap-allocated payload per message, the way the framework builds one).
    std::string baseline = "framework";
    int modules = 3;      // linear: total stages including source and sink
    int branches = 2;     // diamond: number of parallel branches
    int capacity = 0;     // 0 = derive from payload
    int durationSeconds = 10;
    int repeat = 3;
    /// Worker `maxBatchSize` on every module but the source. The raw baseline has
    /// no batching, so a like-for-like comparison is `--batch 1`.
    int batch = 4;
    bool batchSpecified = false;
    /// A config whose throughput spread across repetitions is wider than this is
    /// flagged UNSTABLE; HARNESS.md rule 2 says such a number may not be read.
    double maxSpreadPercent = 10.0;
    /// false skips the harness's own clock reads and histogram samples, so the
    /// difference to the same config is the cost of measuring it.
    bool observe = true;
    /// `--micro-observer N` runs N iterations of the harness's own per-message
    /// observer (two clock reads + one histogram sample) and exits, which is how
    /// the cost gets a number instead of hiding under run-to-run noise.
    int microObserver = 0;
    bool syncInputs = false;
    bool freshPayload = false;
    PayloadKind payload = PayloadKind::None;
    // --- extension knobs, outside the HARNESS.md ladders ---
    /// Controlled work on every intermediate stage: none, delay, compute, memory.
    WorkloadSpec workload;
    /// 0 = no affinity (all logical CPUs); > 0 pins the process to the first N.
    int cpus = 0;
    /// inject: `block` waits for room like an external decoder; `try` counts
    /// refusals and yields while an edge is full, never dropping a message.
    std::string pushMode = "block";
    /// diamond: branch_0 sleeps this many microseconds, the straggler that keeps
    /// fused groups half-complete.
    int stragglerUs = 0;
    /// Enable the module per-hop timing chain (`PipelineContext::EnableModuleTimings`).
    bool timings = false;
    std::string logLevel = "warn";
};

const char* Platform() {
#if defined(_WIN32)
    return "windows-msvc";
#else
    return "linux-gcc";
#endif
}

bool IsRawBaseline(const Options& options) { return options.baseline != "framework"; }

const char* BaselineName(const Options& options) { return options.baseline.c_str(); }

/// The batch size the run actually uses. A raw baseline hands one message over at
/// a time, so 0 means "no batching" in its rows.
int EffectiveBatch(const Options& options) { return IsRawBaseline(options) ? 0 : options.batch; }

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

/**
 * @brief Parses a whole token as an integer, refusing trailing garbage.
 *
 * `--capacity 64x` used to parse as 64 and `--capacity abc` silently fell back to
 * the default, so a typo in a matrix script produced a plausible-looking row for
 * a configuration nobody asked for.
 */
bool ToInt(const std::string& text, int& out) {
    try {
        std::size_t consumed = 0;
        const int value = std::stoi(text, &consumed);
        if (consumed != text.size()) {
            return false;
        }
        out = value;
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

/// Returns the parsed integer, or stops the run with a message naming the flag.
int NumberOrExit(const char* flag, const std::string& text) {
    int value = 0;
    if (!ToInt(text, value)) {
        std::cerr << flag << " expects an integer, got '" << text << "'\n";
        std::exit(1);
    }
    return value;
}

/// Same as ToInt(), for the one fractional knob (--max-spread).
bool ToDouble(const std::string& text, double& out) {
    try {
        std::size_t consumed = 0;
        const double value = std::stod(text, &consumed);
        if (consumed != text.size()) {
            return false;
        }
        out = value;
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

double NumberOrExitDouble(const char* flag, const std::string& text) {
    double value = 0.0;
    if (!ToDouble(text, value) || value < 0.0) {
        std::cerr << flag << " expects a non-negative number, got '" << text << "'\n";
        std::exit(1);
    }
    return value;
}

PayloadKind ParsePayload(const std::string& text) {
    if (text == "1080p-rgb") return PayloadKind::Rgb1080p;
    if (text == "1080p-yuv420") return PayloadKind::Yuv420_1080p;
    if (text == "2k-rgb") return PayloadKind::Rgb2k;
    if (text == "2k-yuv420") return PayloadKind::Yuv420_2k;
    return PayloadKind::None;
}

WorkloadKind ParseWorkload(const std::string& text) {
    if (text == "none") return WorkloadKind::None;
    if (text == "delay") return WorkloadKind::Delay;
    if (text == "compute") return WorkloadKind::Compute;
    if (text == "memory") return WorkloadKind::Memory;
    return WorkloadKind::None;
}

logger::LogLevel LogLevelFromName(const std::string& name) {
    if (name == "trace") return logger::LogLevel::TRACE;
    if (name == "debug") return logger::LogLevel::DEBUG;
    if (name == "info") return logger::LogLevel::INFO;
    if (name == "err") return logger::LogLevel::ERR;
    if (name == "off") return logger::LogLevel::OFF;
    return logger::LogLevel::WARN;
}

int LogicalCpuCount() {
    const unsigned int count = std::thread::hardware_concurrency();
    return count > 0 ? static_cast<int>(count) : 1;
}

/// Pins the process to the first `count` logical CPUs; 0 means no affinity.
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

Options ParseOptions(int argc, char* argv[]) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        std::string value;
        if (TakeValue(arg, "--topology", argc, argv, i, value)) {
            options.topology = value;
        } else if (TakeValue(arg, "--label", argc, argv, i, value)) {
            // The label is written verbatim into the CSV, so a comma there would
            // shift every column after it.
            if (value.find(',') != std::string::npos) {
                std::cerr << "--label must not contain a comma: it is written into the CSV verbatim\n";
                std::exit(1);
            }
            options.label = value;
        } else if (TakeValue(arg, "--payload", argc, argv, i, value)) {
            options.payload = ParsePayload(value);
            if (NameOf(options.payload) != value) {
                std::cerr << "unknown --payload '" << value
                          << "': expected none, 1080p-rgb, 1080p-yuv420, 2k-rgb or 2k-yuv420\n";
                std::exit(1);
            }
        } else if (TakeValue(arg, "--modules", argc, argv, i, value)) {
            options.modules = NumberOrExit("--modules", value);
        } else if (TakeValue(arg, "--branches", argc, argv, i, value)) {
            options.branches = NumberOrExit("--branches", value);
        } else if (TakeValue(arg, "--baseline", argc, argv, i, value)) {
            if (value != "framework" && value != "raw" && value != "raw-alloc" && value != "raw-message") {
                std::cerr << "unknown --baseline '" << value
                          << "': expected framework, raw, raw-alloc or raw-message\n";
                std::exit(1);
            }
            options.baseline = value;
        } else if (TakeValue(arg, "--batch", argc, argv, i, value)) {
            options.batch = NumberOrExit("--batch", value);
            options.batchSpecified = true;
        } else if (TakeValue(arg, "--observe", argc, argv, i, value)) {
            if (value != "full" && value != "none") {
                std::cerr << "unknown --observe '" << value << "': expected full or none\n";
                std::exit(1);
            }
            options.observe = value == "full";
        } else if (TakeValue(arg, "--micro-observer", argc, argv, i, value)) {
            options.microObserver = NumberOrExit("--micro-observer", value);
        } else if (TakeValue(arg, "--capacity", argc, argv, i, value)) {
            options.capacity = NumberOrExit("--capacity", value);
        } else if (TakeValue(arg, "--duration", argc, argv, i, value)) {
            options.durationSeconds = NumberOrExit("--duration", value);
        } else if (TakeValue(arg, "--repeat", argc, argv, i, value)) {
            options.repeat = NumberOrExit("--repeat", value);
        } else if (TakeValue(arg, "--max-spread", argc, argv, i, value)) {
            options.maxSpreadPercent = NumberOrExitDouble("--max-spread", value);
        } else if (TakeValue(arg, "--workload", argc, argv, i, value)) {
            options.workload.kind = ParseWorkload(value);
            if (NameOf(options.workload.kind) != value) {
                std::cerr << "unknown --workload '" << value << "': expected none, delay, compute or memory\n";
                std::exit(1);
            }
        } else if (TakeValue(arg, "--delay-us", argc, argv, i, value)) {
            options.workload.delayUs = NumberOrExit("--delay-us", value);
        } else if (TakeValue(arg, "--compute-iters", argc, argv, i, value)) {
            options.workload.computeIterations = NumberOrExit("--compute-iters", value);
        } else if (TakeValue(arg, "--memory-bytes", argc, argv, i, value)) {
            const int bytes = NumberOrExit("--memory-bytes", value);
            if (bytes < 1) {
                std::cerr << "--memory-bytes " << bytes << ": a copy of zero bytes measures nothing\n";
                std::exit(1);
            }
            options.workload.memoryBytes = static_cast<std::size_t>(bytes);
        } else if (TakeValue(arg, "--cpus", argc, argv, i, value)) {
            options.cpus = NumberOrExit("--cpus", value);
        } else if (TakeValue(arg, "--straggler-us", argc, argv, i, value)) {
            options.stragglerUs = NumberOrExit("--straggler-us", value);
        } else if (TakeValue(arg, "--push-mode", argc, argv, i, value)) {
            if (value != "block" && value != "try") {
                std::cerr << "unknown --push-mode '" << value << "': expected block or try\n";
                std::exit(1);
            }
            options.pushMode = value;
        } else if (TakeValue(arg, "--log-level", argc, argv, i, value)) {
            if (value != "trace" && value != "debug" && value != "info" && value != "warn" && value != "err" &&
                value != "off") {
                std::cerr << "unknown --log-level '" << value << "': expected trace, debug, info, warn, err or off\n";
                std::exit(1);
            }
            options.logLevel = value;
        } else if (arg == "--timings") {
            options.timings = true;
        } else if (arg == "--sync-inputs") {
            options.syncInputs = true;
        } else if (arg == "--payload-fresh") {
            options.freshPayload = true;
        } else if (arg == "--help" || arg == "-h") {
            std::cout
                << "usage: pipeline_benchmark [--topology diamond|linear|inject] [--baseline framework|raw|raw-alloc|raw-message]\n"
                   "       [--modules N] [--branches N] [--capacity N] [--batch N] [--duration S] [--repeat R]\n"
                   "       [--payload none|1080p-rgb|1080p-yuv420|2k-rgb|2k-yuv420] [--payload-fresh]\n"
                   "       [--workload none|delay|compute|memory] [--delay-us N] [--compute-iters N] [--memory-bytes N]\n"
                   "       [--cpus N] [--push-mode block|try] [--straggler-us N] [--sync-inputs] [--timings]\n"
                   "       [--log-level trace|debug|info|warn|err|off] [--max-spread P] [--label TEXT]\n";
            std::exit(0);
        } else {
            std::cerr << "unknown argument '" << arg << "': try --help\n";
            std::exit(1);
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

/**
 * Where one run's messages actually went, straight from Pipeline::GetSnapshot().
 *
 * The sink's own count says how many messages it saw; it cannot say whether every
 * message the source produced got there. Two snapshots are needed because a
 * handful of pushes race the stop and are counted as dropped there: the mid-run
 * snapshot is the one that has to be clean, the after-stop one closes the books.
 */
struct Coverage {
    /// Mid-run (taken while the pipeline still runs, before Stop()).
    std::uint64_t midDropped = 0;
    std::uint64_t midFailed = 0;
    std::uint64_t midFusionExpired = 0;
    /// After Stop(): the whole run, including the shutdown tail.
    std::uint64_t pushed = 0;
    std::uint64_t popped = 0;
    std::uint64_t discarded = 0;  ///< destroyed by a cancel, never delivered
    std::uint64_t residual = 0;   ///< still queued after Stop(); must be 0
    std::uint64_t sinkPopped = 0; ///< pops from the edges that feed the sink
    std::uint64_t processed = 0;
    std::uint64_t dropped = 0;  ///< a module could not hand a message downstream
    std::uint64_t failed = 0;
    std::uint64_t fusionExpired = 0;
    std::uint64_t stopMs = 0;
    bool ok = true;
};

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
    Coverage coverage;
    /// Whole-process resident memory, sampled every 25 ms during the run, in MiB.
    /// Edge memory is `capacity * frame * edges` on paper, but frames and the
    /// fusion sink's messageCache are invisible to every counter, so this is the
    /// only witness for them.
    double rssStartMiB = 0.0;
    double rssPeakMiB = 0.0;
    double rssEndMiB = 0.0;
    /// Deepest backlog any single edge reached during the run.
    std::uint64_t edgeDepthMax = 0;
    /// How long Stop() took: draining the queues and joining every worker.
    double stopMs = 0.0;
    /// inject only: Push/TryPush attempts and refusals (TryPush found the edge full).
    std::uint64_t attempts = 0;
    std::uint64_t refused = 0;
};

/// min / median / max of one metric over the measured runs.
struct Stat {
    double min = 0.0;
    double median = 0.0;
    double max = 0.0;

    /// (max - min) / median * 100: the number HARNESS.md rule 2 compares against.
    double SpreadPercent() const { return median > 0.0 ? (max - min) / median * 100.0 : 0.0; }
};

Stat Summarize(std::vector<double> values) {
    Stat stat;
    if (values.empty()) {
        return stat;
    }
    std::sort(values.begin(), values.end());
    stat.min = values.front();
    stat.max = values.back();
    stat.median = values[values.size() / 2];
    return stat;
}

/**
 * @brief Reads the pipeline's counters and checks that nothing went missing.
 *
 * `pushed == popped + discarded + residual` is the identity Stats.hpp promises;
 * the sink-side check ties the sink's own count back to the graph: the sink is
 * handed one message per logical frame, except on the fusion path, where it is
 * handed one fused message per branch group.
 */
Coverage CoverageOf(const nexusflow::PipelineSnapshot& midRun, const nexusflow::Pipeline& pipeline,
                    const TopologyShape& shape, bool syncInputs, std::uint64_t delivered) {
    const nexusflow::PipelineSnapshot final = pipeline.GetSnapshot();
    Coverage coverage;

    for (const auto& module : midRun.modules) {
        coverage.midDropped += module.droppedTotal;
        coverage.midFailed += module.failedTotal;
        coverage.midFusionExpired += module.fusionExpiredTotal;
    }
    for (const auto& edge : final.edges) {
        coverage.pushed += edge.pushedTotal;
        coverage.popped += edge.poppedTotal;
        coverage.discarded += edge.discardedTotal;
        coverage.residual += edge.size;
        if (edge.to == "sink") {
            coverage.sinkPopped += edge.poppedTotal;
        }
    }
    for (const auto& module : final.modules) {
        coverage.processed += module.processedTotal;
        coverage.dropped += module.droppedTotal;
        coverage.failed += module.failedTotal;
        coverage.fusionExpired += module.fusionExpiredTotal;
    }
    coverage.stopMs = final.stopPipelineMs;

    const std::uint64_t messagesPerFrame =
        (syncInputs && !shape.IsLinear() && shape.branches > 0) ? static_cast<std::uint64_t>(shape.branches) : 1u;
    // While the pipeline runs nothing may be lost. The sink-side count is exact on
    // every path except fusion: there a frame normally costs `branches` pops, but
    // a group that drains incomplete costs fewer, so it is checked as a range.
    bool sinkMatches = false;
    if (messagesPerFrame == 1) {
        sinkMatches = coverage.sinkPopped == delivered;
    } else {
        // Fusion: a frame normally costs `branches` pops, an incomplete group at
        // shutdown costs fewer, and the messages a half-finished group left in
        // the worker cache were popped but never delivered -- those have to show
        // up as drops at stop, so the excess is only trusted up to that count.
        const std::uint64_t expected = delivered * messagesPerFrame;
        const std::uint64_t extra = coverage.sinkPopped > expected ? coverage.sinkPopped - expected : 0;
        sinkMatches = coverage.sinkPopped >= delivered && extra <= coverage.dropped;
    }
    coverage.ok = coverage.midDropped == 0 && coverage.midFailed == 0 && coverage.midFusionExpired == 0 &&
                  coverage.pushed == coverage.popped + coverage.discarded + coverage.residual && sinkMatches;
    return coverage;
}

/// Samples the process's resident set while the pipeline runs. Only the driving
/// thread calls this: in the active topologies the main thread has nothing to do
/// but wait, and in injection mode it is the producer, so no sampling thread is
/// needed. A 25 ms interval can miss a spike shorter than that, which is why the
/// report calls this a sampled peak.
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
        if (options.observe) {
            payload.creationTime = now;
        }
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

template <typename Factory>
Sample RunRawChain(const Options& options, const TopologyShape& shape, const PayloadSpec& spec,
                   std::size_t capacity) {
    RawChain<Factory> chain(shape, capacity, Factory(spec, options.freshPayload, options.observe), options.observe);

    const std::size_t rssStart = ResidentBytes();
    std::size_t rssPeak = rssStart;
    const double cpuBefore = ProcessCpuSeconds();
    const Clock::time_point started = Clock::now();
    chain.Start();
    const Clock::time_point deadline = started + std::chrono::seconds(options.durationSeconds);
    while (Clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
        SampleResident(&rssPeak);
    }
    const Clock::time_point finished = Clock::now();
    const Clock::time_point stopStarted = Clock::now();
    chain.Stop();
    const std::size_t rssEnd = ResidentBytes();
    const double stopMs =
        static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - stopStarted).count()) /
        1000.0;
    const double cpuAfter = ProcessCpuSeconds();

    const RawCounters counters = chain.TakeSnapshot();
    const LatencyHistogram& latency = chain.Latency();

    Sample sample;
    sample.wallSeconds = std::chrono::duration<double>(finished - started).count();
    sample.cpuSeconds = cpuAfter - cpuBefore;
    sample.delivered = counters.delivered;
    sample.throughput =
        sample.wallSeconds > 0.0 ? static_cast<double>(counters.delivered) / sample.wallSeconds : 0.0;
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
    sample.stopMs = stopMs;

    // The raw baseline has no drop channel: a push either succeeds or the queue
    // was already closed, so the only bookkeeping is that every push was popped
    // (drained by Stop()) and that the sink saw every pop.
    sample.coverage.pushed = counters.pushed;
    sample.coverage.popped = counters.popped;
    sample.coverage.residual = counters.residual;
    sample.coverage.sinkPopped = counters.sinkPopped;
    sample.coverage.processed = counters.popped;
    sample.coverage.stopMs = static_cast<std::uint64_t>(stopMs);
    sample.coverage.ok = counters.pushed == counters.popped + counters.residual &&
                         counters.sinkPopped == counters.delivered;
    return sample;
}

Sample RunOnceRaw(const Options& options, const TopologyShape& shape, const PayloadSpec& spec,
                  std::size_t capacity) {
    if (options.baseline == "raw-message") {
        return RunRawChain<RawMessageFactory>(options, shape, spec, capacity);
    }
    if (options.baseline == "raw-alloc") {
        return RunRawChain<RawBoxedFactory>(options, shape, spec, capacity);
    }
    return RunRawChain<RawInlineFactory>(options, shape, spec, capacity);
}

Sample RunOnce(const Options& options, const TopologyShape& shape, const PayloadSpec& spec, std::size_t capacity) {
    if (IsRawBaseline(options)) {
        return RunOnceRaw(options, shape, spec, capacity);
    }
    const bool injection = options.topology == "inject";
    BuiltTopology built =
        injection
            ? BuildInjection(options.modules, capacity, options.batch, options.observe, options.workload)
            : (shape.IsLinear()
                   ? BuildLinear(shape, capacity, spec, options.freshPayload, options.batch, options.observe,
                                 options.workload)
                   : BuildDiamond(shape, capacity, spec, options.freshPayload, options.syncInputs, options.batch,
                                  options.observe, options.workload, options.stragglerUs));
    if (built.pipeline == nullptr) {
        std::cerr << "failed to build the " << options.topology << " topology\n";
        std::exit(1);
    }
    if (built.pipeline->Init() != ErrorCode::SUCCESS) {
        std::cerr << "pipeline Init() failed\n";
        std::exit(1);
    }

    if (options.timings) {
        // Before Start(): every worker reads the switch on its first message.
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
    if (injection) {
        // The main thread is the producer here, so it cannot also just sleep.
        RunInjectionWindow(options, spec, built, &rssPeak, &injectionAttempts, &injectionRefused);
    } else {
        const Clock::time_point deadline = started + std::chrono::seconds(options.durationSeconds);
        while (Clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
            SampleResident(&rssPeak);
        }
    }
    const Clock::time_point finished = Clock::now();

    // One snapshot while the pipeline still runs: this is the state a healthy
    // saturated run has to be in (nothing dropped, nothing cancelled). It costs
    // one pass over the counters per run, not per message.
    const nexusflow::PipelineSnapshot midRun = built.pipeline->GetSnapshot();

    // Capture the counters after Stop(): the sink's histogram is written by its
    // worker thread, so reading it while the pipeline still runs would race.
    const Clock::time_point stopStarted = Clock::now();
    built.pipeline->Stop();
    const Clock::time_point stopped = Clock::now();
    const double cpuAfter = ProcessCpuSeconds();
    const std::size_t rssEnd = ResidentBytes();
    const nexusflow::PipelineSnapshot finalRun = built.pipeline->GetSnapshot();

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
    sample.stopMs = std::chrono::duration<double, std::milli>(stopped - stopStarted).count();
    for (const nexusflow::EdgeSnapshot& edge : finalRun.edges) {
        if (static_cast<std::uint64_t>(edge.maxSize) > sample.edgeDepthMax) {
            sample.edgeDepthMax = static_cast<std::uint64_t>(edge.maxSize);
        }
    }
    sample.attempts = injectionAttempts;
    sample.refused = injectionRefused;
    sample.coverage = CoverageOf(midRun, *built.pipeline, shape, options.syncInputs, delivered);

    built.pipeline->DeInit();
    return sample;
}

void PrintCsv(const Options& options, const TopologyShape& shape, std::size_t capacity, int repetition,
              const Sample& sample) {
    // The shape columns are what the pipeline actually is, not what the command
    // line asked for: a diamond run has branches+2 stages and a chain has no
    // branches at all, so echoing the options made every row claim otherwise.
    std::cout << "csv," << options.label << ',' << Platform() << ',' << BaselineName(options) << ','
              << EffectiveBatch(options) << ',' << (options.observe ? 1 : 0) << ',' << options.topology << ',' << shape.modules << ','
              << shape.branches << ',' << capacity << ',' << NameOf(options.payload) << ','
              << (options.freshPayload ? 1 : 0) << ',' << (options.syncInputs ? 1 : 0) << ',' << options.durationSeconds
              << ',' << repetition << ',' << sample.delivered << ',' << static_cast<std::uint64_t>(sample.throughput)
              << ',' << sample.cpuSeconds << ',' << sample.cores << ',' << sample.latencyAvgUs << ','
              << sample.latencyP50Us << ',' << sample.latencyP95Us << ',' << sample.latencyP99Us << ','
              << sample.latencyMinUs << ',' << sample.latencyMaxUs << ',' << options.cpus << ','
              << (options.timings ? 1 : 0) << ',' << NameOf(options.workload.kind) << ','
              << WorkloadParameter(options.workload) << ',' << options.pushMode << ',' << sample.attempts << ','
              << sample.refused << ',' << sample.rssStartMiB << ',' << sample.rssPeakMiB << ',' << sample.rssEndMiB
              << ',' << sample.edgeDepthMax << ',' << sample.stopMs << '\n';
}

/**
 * @brief One `cov,` line per repetition: where the messages went.
 *
 * Prefixed `cov` rather than `csv` so a `grep '^csv,'` matrix collection keeps
 * working unchanged, while a mismatch still lands on stderr and is impossible to
 * miss in the output.
 */
void PrintCoverage(const Options& options, const TopologyShape& shape, std::size_t capacity, int repetition,
                   const Sample& sample) {
    const Coverage& coverage = sample.coverage;
    std::cout << "cov," << options.label << ',' << BaselineName(options) << ',' << shape.modules << ','
              << shape.branches << ',' << capacity << ',' << repetition << ',' << coverage.pushed << ','
              << coverage.popped << ',' << coverage.discarded << ',' << coverage.residual << ','
              << coverage.sinkPopped << ',' << sample.delivered << ',' << coverage.processed << ','
              << coverage.dropped << ',' << coverage.failed << ',' << coverage.fusionExpired << ','
              << coverage.midDropped << ',' << coverage.midFusionExpired << ',' << coverage.stopMs << ','
              << (coverage.ok ? 1 : 0) << '\n';
    if (!coverage.ok) {
        std::cerr << "COVERAGE MISMATCH (repetition " << repetition << "): pushed=" << coverage.pushed
                  << " popped=" << coverage.popped << " discarded=" << coverage.discarded
                  << " residual=" << coverage.residual << " sinkPopped=" << coverage.sinkPopped
                  << " droppedWhileRunning=" << coverage.midDropped
                  << " fusionExpiredWhileRunning=" << coverage.midFusionExpired
                  << " droppedAtStop=" << coverage.dropped << " failed=" << coverage.failed << '\n';
    }
}

/**
 * @brief One `stat,` line per configuration: median [min .. max] over the runs.
 *
 * This is what a ladder table is built from: the median is the value, the range
 * is how much it moved, and `unstable` says the range is wider than --max-spread
 * (HARNESS.md rule 2).
 */
void PrintStat(const Options& options, const TopologyShape& shape, std::size_t capacity, int repetitions,
               const Stat& throughput, const Stat& cores, const Stat& cpuPerMessage, const Stat& p50, const Stat& p95,
               const Stat& p99, bool unstable) {
    std::cout << "stat," << options.label << ',' << Platform() << ',' << BaselineName(options) << ','
              << EffectiveBatch(options) << ',' << (options.observe ? 1 : 0) << ',' << options.topology << ',' << shape.modules << ',' << shape.branches
              << ',' << shape.edges << ',' << capacity << ',' << NameOf(options.payload) << ','
              << (options.freshPayload ? 1 : 0) << ',' << (options.syncInputs ? 1 : 0) << ',' << repetitions << ','
              << static_cast<std::uint64_t>(throughput.median) << ','
              << static_cast<std::uint64_t>(throughput.min) << ',' << static_cast<std::uint64_t>(throughput.max) << ','
              << throughput.SpreadPercent() << ',' << cores.median << ',' << cores.min << ',' << cores.max << ','
              << p50.median << ',' << p95.median << ',' << p99.median << ',' << cpuPerMessage.median << ','
              << (unstable ? 1 : 0) << '\n';
}

} // namespace

/// Ask for the 1 ms Windows timer tick while the benchmark runs and restore it
/// on exit. Windows may stop honouring this for `Sleep` after a few seconds, so
/// the delay workload waits on a high-resolution waitable timer instead of
/// relying on this guard; shorter waits elsewhere still benefit during the
/// first seconds of a run. POSIX sleep resolution is already fine.
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

    // --micro-observer: what the harness's own per-message observer costs. Run
    // separately, because inside a real pipeline the difference between
    // --observe full and none disappears under the run-to-run noise.
    if (options.microObserver > 0) {
        LatencyHistogram histogram;
        volatile std::uint64_t keepAlive = 0;
        const int iterations = options.microObserver;
        const Clock::time_point started = Clock::now();
        for (int i = 0; i < iterations; ++i) {
            const Clock::time_point sourceRead = Clock::now();
            const std::uint64_t value = static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - sourceRead).count());
            histogram.Record(value);
            keepAlive += histogram.Count();
        }
        const double elapsedNs =
            static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - started).count());
        std::cout << "observer micro: " << (elapsedNs / static_cast<double>(iterations))
                  << " ns per message (2 clock reads + 1 histogram sample), " << iterations << " iterations\n";
        std::cout << "  keep-alive: " << keepAlive << '\n';
        return 0;
    }

    // Reject a topology the tool cannot build instead of silently falling back to
    // the diamond, and reject knobs that would change the shape behind the
    // caller's back: an impossible value must fail loudly, never be corrected
    // quietly -- a silently clamped point poisons a ladder (HARNESS.md §7).
    const bool injection = options.topology == "inject";
    const bool linear = options.topology == "linear";
    if (!linear && !injection && options.topology != "diamond") {
        std::cerr << "unknown --topology '" << options.topology << "': expected linear, diamond or inject\n";
        return 1;
    }
    if (injection && options.baseline != "framework") {
        std::cerr << "--topology inject needs --baseline framework: there is no raw pipeline to push into\n";
        return 1;
    }
    if (injection && options.syncInputs) {
        std::cerr << "--topology inject is a chain: it has no fusion path, drop --sync-inputs\n";
        return 1;
    }
    if (linear && options.modules < 2) {
        std::cerr << "--modules " << options.modules << ": a chain has at least 2 stages (source -> sink)\n";
        return 1;
    }
    if (!linear && !injection && options.branches < 1) {
        std::cerr << "--branches " << options.branches << ": a fan-out has at least 1 branch\n";
        return 1;
    }
    if (linear && options.syncInputs) {
        std::cerr << "--sync-inputs only applies to --topology diamond: a chain has one input\n";
        return 1;
    }
    if (IsRawBaseline(options) && !options.workload.empty()) {
        std::cerr << "--workload only applies to the framework pipeline: a raw chain has no stages to run it\n";
        return 1;
    }
    if (options.capacity < 0) {
        std::cerr << "--capacity " << options.capacity << ": omit it to derive the depth from the payload\n";
        return 1;
    }
    if (options.batch < 1) {
        std::cerr << "--batch " << options.batch << ": a batch has at least 1 message\n";
        return 1;
    }
    if (IsRawBaseline(options) && options.batchSpecified) {
        std::cerr << "--baseline " << options.baseline << " hands over one message at a time: drop --batch\n";
        return 1;
    }
    if (IsRawBaseline(options) && options.syncInputs) {
        std::cerr << "--baseline " << options.baseline << " has no fusion path: drop --sync-inputs\n";
        return 1;
    }
    TopologyShape shape;
    if (injection) {
        shape.modules = options.modules < 2 ? 2 : options.modules;
        shape.branches = 0;
        shape.edges = shape.modules - 1;
    } else {
        shape = ShapeOf(linear, options.modules, options.branches);
    }

    std::string pinError;
    if (!PinToCpus(options.cpus, &pinError)) {
        std::cerr << "pipeline_benchmark: " << pinError << "\n";
        return 2;
    }

    const PayloadSpec spec = SpecFor(options.payload);
    // Queue depth times payload size is the memory each edge holds: with 1080p
    // RGB a capacity of 64 would park ~380 MiB per edge, so payload runs default
    // to a shallow queue unless the caller asks for a specific one.
    const std::size_t capacity =
        options.capacity > 0 ? static_cast<std::size_t>(options.capacity) : (spec.empty() ? 64u : 4u);

    std::cout << "--- NexusFlow pipeline benchmark ---\n";
    if (options.cpus > 0) {
        std::cout << "affinity: first " << options.cpus << " of " << LogicalCpuCount() << " logical CPUs\n";
    } else {
        std::cout << "affinity: all " << LogicalCpuCount() << " logical CPUs\n";
    }
    std::cout << "topology: " << options.topology << "  modules=" << shape.modules << "  edges=" << shape.edges;
    if (shape.branches > 0) {
        std::cout << "  branches=" << shape.branches;
    }
    std::cout << "  sync_inputs=" << (options.syncInputs ? "yes" : "no") << "\n";
    std::cout << "baseline: " << options.baseline;
    if (IsRawBaseline(options)) {
        std::cout << " (std::thread + SpscQueue)  batch=none  observe=" << (options.observe ? "full" : "none") << "\n";
    } else {
        std::cout << "  batch=" << options.batch << "  observe=" << (options.observe ? "full" : "none") << "\n";
    }
    if (injection) {
        std::cout << "injection: " << options.pushMode << " push from the main thread\n";
    }
    if (!options.workload.empty()) {
        std::cout << "workload: " << NameOf(options.workload.kind) << " (parameter "
                  << WorkloadParameter(options.workload) << ") on every intermediate stage\n";
    }
    if (options.stragglerUs > 0) {
        std::cout << "straggler: branch_0 runs the delay workload with " << options.stragglerUs << " us\n";
    }
    if (options.timings) {
        std::cout << "module timings: on\n";
    }
    std::cout << "payload: " << NameOf(options.payload) << " (" << spec.bytes << " bytes/frame, "
              << (options.freshPayload ? "fresh per message" : "one buffer reused") << ")  capacity=" << capacity
              << "  streams=" << options.repeat << " x " << options.durationSeconds << "s (+1 warm-up)\n";
    if (!spec.empty()) {
        const std::size_t edges = static_cast<std::size_t>(shape.edges);
        std::cout << "edge memory at full depth: ~" << (capacity * spec.bytes * edges) / (1024 * 1024) << " MiB\n";
    }

    // Warm-up: first run pays page faults, thread start-up and cache warming.
    const Sample warmUp = RunOnce(options, shape, spec, capacity);
    std::cout << "warm-up: " << static_cast<std::uint64_t>(warmUp.throughput) << " msg/s (discarded)\n";

    std::vector<Sample> samples;
    bool coverageOk = true;
    for (int repetition = 1; repetition <= options.repeat; ++repetition) {
        const Sample sample = RunOnce(options, shape, spec, capacity);
        samples.push_back(sample);
        PrintCsv(options, shape, capacity, repetition, sample);
        PrintCoverage(options, shape, capacity, repetition, sample);
        coverageOk = coverageOk && sample.coverage.ok;
    }

    std::vector<double> throughputs;
    std::vector<double> p50s;
    std::vector<double> p95s;
    std::vector<double> p99s;
    std::vector<double> cores;
    std::vector<double> cpuPerMessage;
    for (const Sample& sample : samples) {
        throughputs.push_back(sample.throughput);
        p50s.push_back(sample.latencyP50Us);
        p95s.push_back(sample.latencyP95Us);
        p99s.push_back(sample.latencyP99Us);
        cores.push_back(sample.cores);
        cpuPerMessage.push_back(sample.delivered > 0
                                    ? sample.cpuSeconds / static_cast<double>(sample.delivered) * 1.0e6
                                    : 0.0);
    }
    const Stat throughputStat = Summarize(throughputs);
    const Stat coresStat = Summarize(cores);
    const Stat cpuPerMessageStat = Summarize(cpuPerMessage);
    const Stat p50Stat = Summarize(p50s);
    const Stat p95Stat = Summarize(p95s);
    const Stat p99Stat = Summarize(p99s);

    // HARNESS.md rule 2: a config whose spread is wider than --max-spread may not
    // be read, so it is flagged instead of being reported as a value.
    const bool unstable = throughputStat.SpreadPercent() > options.maxSpreadPercent;

    auto printRange = [](const char* name, const Stat& stat, bool asInteger) {
        std::cout << name;
        if (asInteger) {
            std::cout << static_cast<std::uint64_t>(stat.median) << "  [" << static_cast<std::uint64_t>(stat.min)
                      << " .. " << static_cast<std::uint64_t>(stat.max) << ']';
        } else {
            std::cout << stat.median << "  [" << stat.min << " .. " << stat.max << ']';
        }
        std::cout << "   spread " << stat.SpreadPercent() << "%\n";
    };

    std::cout << "----- " << options.repeat << " runs: median [min .. max] -----\n";
    printRange("throughput  msg/s : ", throughputStat, true);
    printRange("cores             : ", coresStat, false);
    printRange("cpu per msg  us   : ", cpuPerMessageStat, false);
    printRange("latency p50  us   : ", p50Stat, false);
    printRange("latency p95  us   : ", p95Stat, false);
    printRange("latency p99  us   : ", p99Stat, false);
    std::cout << "coverage          : " << (coverageOk ? "ok" : "MISMATCH") << '\n';
    std::cout << "-------------------------------\n";

    PrintStat(options, shape, capacity, options.repeat, throughputStat, coresStat, cpuPerMessageStat, p50Stat, p95Stat,
              p99Stat, unstable);

    if (unstable) {
        std::cout << "UNSTABLE: spread " << throughputStat.SpreadPercent() << "% > --max-spread "
                  << options.maxSpreadPercent << "%: rerun before trusting this point\n";
    }
    return coverageOk ? 0 : 1;
}
