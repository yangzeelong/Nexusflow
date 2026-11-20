#ifndef NEXUSFLOW_BENCHMARKS_BENCHMARK_NODES_HPP
#define NEXUSFLOW_BENCHMARKS_BENCHMARK_NODES_HPP

#include "LatencyHistogram.hpp"

#include <nexusflow/Message.hpp>
#include <nexusflow/Module.hpp>

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#endif

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace nexusflow_bench {

using Clock = std::chrono::steady_clock;

/**
 * @brief The image payloads this harness simulates.
 *
 * The shape follows the AI-Football extension's `VideoFrame`: the bytes are owned
 * by one immutable frame object and every downstream message shares it, so a
 * message copy never touches pixels. Sizes are the real ones for the formats we
 * actually decode; YUV is NV12/I420-style 4:2:0 at 8 bits (half a byte per pixel
 * less than RGB).
 */
enum class PayloadKind {
    None,
    Rgb1080p,    ///< 1920x1080 RGB24, 5.93 MiB
    Yuv420_1080p,///< 1920x1080 YUV420, 2.97 MiB
    Rgb2k,       ///< 2560x1440 RGB24 (QHD), 10.547 MiB
    Yuv420_2k    ///< 2560x1440 YUV420, 5.27 MiB
};

struct PayloadSpec {
    std::uint32_t width = 0;
    std::uint32_t height = 0;
    std::uint32_t channels = 0;
    std::size_t bytes = 0;

    bool empty() const { return bytes == 0; }
};

inline PayloadSpec SpecFor(PayloadKind kind) {
    switch (kind) {
    case PayloadKind::None:
        return PayloadSpec{};
    case PayloadKind::Rgb1080p:
        return PayloadSpec{1920, 1080, 3, static_cast<std::size_t>(1920) * 1080 * 3};
    case PayloadKind::Yuv420_1080p:
        return PayloadSpec{1920, 1080, 3, static_cast<std::size_t>(1920) * 1080 * 3 / 2};
    case PayloadKind::Rgb2k:
        return PayloadSpec{2560, 1440, 3, static_cast<std::size_t>(2560) * 1440 * 3};
    case PayloadKind::Yuv420_2k:
        return PayloadSpec{2560, 1440, 3, static_cast<std::size_t>(2560) * 1440 * 3 / 2};
    }
    return PayloadSpec{};
}

inline const char* NameOf(PayloadKind kind) {
    switch (kind) {
    case PayloadKind::None: return "none";
    case PayloadKind::Rgb1080p: return "1080p-rgb";
    case PayloadKind::Yuv420_1080p: return "1080p-yuv420";
    case PayloadKind::Rgb2k: return "2k-rgb";
    case PayloadKind::Yuv420_2k: return "2k-yuv420";
    }
    return "?";
}

/// One immutable frame. `bytes` is what a real decoder would own and fill.
struct BenchmarkFrame {
    std::uint32_t width = 0;
    std::uint32_t height = 0;
    std::uint32_t channels = 0;
    std::shared_ptr<const std::vector<std::uint8_t>> bytes;
};

/// What the source puts into each message.
struct BenchmarkPayload {
    Clock::time_point creationTime;
    std::shared_ptr<const BenchmarkFrame> frame;  // null when --payload none
};

/// Allocates one frame of the requested size. Shared by the active source and by
/// the external producer (--topology inject), so both spend the same allocation
/// when --payload-fresh asks for a fresh picture per message.
inline std::shared_ptr<const BenchmarkFrame> MakeBenchmarkFrame(PayloadSpec spec) {
    auto frame = std::make_shared<BenchmarkFrame>();
    frame->width = spec.width;
    frame->height = spec.height;
    frame->channels = spec.channels;
    // The zero fill stands in for the decoder writing the picture.
    frame->bytes = std::make_shared<const std::vector<std::uint8_t>>(spec.bytes, static_cast<std::uint8_t>(0));
    return frame;
}

/**
 * @class SourceModule
 * @brief Generates one message per invocation, optionally with a frame.
 *
 * Two payload modes, because they answer different questions:
 *
 *  - reuse (default): one frame buffer is allocated once and shared by every
 *    message. This is what proves the framework's claim that copying a Message
 *    copies a pointer, not pixels -- with 1080p payloads the throughput should
 *    stay close to the empty-payload run.
 *  - fresh (`--payload-fresh`): a new buffer per message, the way a decoder hands
 *    over a fresh picture. This measures allocation and fill cost instead of
 *    framework cost, and is dominated by page faults on any real size.
 */
class SourceModule : public nexusflow::Module {
public:
    SourceModule(std::string name, PayloadSpec spec, bool freshPerMessage, bool measureLatency = true)
        : nexusflow::Module(std::move(name)), m_spec(spec), m_freshPerMessage(freshPerMessage),
          m_measureLatency(measureLatency) {}

protected:
    void Process(nexusflow::Message&) override {
        BenchmarkPayload payload;
        // `--observe none` drops this read: it is the harness's own cost, and the
        // point of that mode is to measure what the harness costs.
        if (m_measureLatency) {
            payload.creationTime = Clock::now();
        }
        if (!m_spec.empty()) {
            if (m_freshPerMessage) {
                payload.frame = MakeFrame();
            } else {
                if (!m_sharedFrame) {
                    m_sharedFrame = MakeFrame();
                }
                payload.frame = m_sharedFrame;
            }
        }
        Broadcast(std::move(nexusflow::MakeMessage(payload)));
    }

private:
    std::shared_ptr<const BenchmarkFrame> MakeFrame() { return MakeBenchmarkFrame(m_spec); }

    PayloadSpec m_spec;
    bool m_freshPerMessage = false;
    bool m_measureLatency = true;
    std::shared_ptr<const BenchmarkFrame> m_sharedFrame;
};

/**
 * @class PassiveSourceModule
 * @brief A source that never produces on its own: the caller is the producer.
 *
 * Pipeline::Push()/TryPush() writes straight into this module's output edges,
 * which is the shape an external producer has (a decoder, a socket, a camera
 * callback). Its worker still runs but finds nothing to do, so a run measures
 * the injection path instead of a source loop.
 */
class PassiveSourceModule : public nexusflow::Module {
public:
    explicit PassiveSourceModule(std::string name) : nexusflow::Module(std::move(name)) {}

    bool IsPassiveSource() const override { return true; }

protected:
    void Process(nexusflow::Message&) override {}
};

/// Forwards whatever it receives. The zero-work stage that isolates framework cost.
class PassThroughModule : public nexusflow::Module {
public:
    explicit PassThroughModule(std::string name) : nexusflow::Module(std::move(name)) {}

protected:
    void Process(nexusflow::Message& message) override { Broadcast(message); }
};

/**
 * @brief What the intermediate stages spend their time on.
 *
 * The pass-through stage isolates framework cost, but a real pipeline never only
 * copies pointers. These three loads put one controlled cost per message in the
 * middle of the graph, so the question changes from "how fast is the graph" to
 * "when does real work stop being hidden by the pipeline":
 *
 *  - Delay    : sleeps, the shape of an I/O or inference wait.
 *  - Compute  : a serial sin/cos chain, CPU work with no allocation.
 *  - Memory   : memcpy over a private buffer, the bandwidth-bound shape.
 */
enum class WorkloadKind {
    None,
    Delay,
    Compute,
    Memory
};

inline const char* NameOf(WorkloadKind kind) {
    switch (kind) {
    case WorkloadKind::None: return "none";
    case WorkloadKind::Delay: return "delay";
    case WorkloadKind::Compute: return "compute";
    case WorkloadKind::Memory: return "memory";
    }
    return "?";
}

struct WorkloadSpec {
    WorkloadKind kind = WorkloadKind::None;
    int delayUs = 100;            ///< Delay: sleep per message
    int computeIterations = 5000; ///< Compute: sin/cos iterations per message
    std::size_t memoryBytes = 65536; ///< Memory: bytes copied per message

    bool empty() const { return kind == WorkloadKind::None; }
};

inline std::size_t WorkloadParameter(const WorkloadSpec& spec) {
    switch (spec.kind) {
    case WorkloadKind::Delay: return static_cast<std::size_t>(spec.delayUs);
    case WorkloadKind::Compute: return static_cast<std::size_t>(spec.computeIterations);
    case WorkloadKind::Memory: return spec.memoryBytes;
    case WorkloadKind::None: break;
    }
    return 0;
}

/// A pass-through stage that first burns the configured amount of time/CPU/memory.
class WorkloadModule : public nexusflow::Module {
public:
    WorkloadModule(std::string name, WorkloadSpec spec)
        : nexusflow::Module(std::move(name)),
          m_spec(spec),
          m_buffer(spec.memoryBytes, 0),
          m_scratch(spec.memoryBytes, 0) {
#if defined(_WIN32)
        if (m_spec.kind == WorkloadKind::Delay) {
            m_delayTimer =
                CreateWaitableTimerExW(nullptr, nullptr, CREATE_WAITABLE_TIMER_HIGH_RESOLUTION, TIMER_ALL_ACCESS);
        }
#endif
    }

    ~WorkloadModule() override {
#if defined(_WIN32)
        if (m_delayTimer != nullptr) {
            CloseHandle(m_delayTimer);
        }
#endif
    }

protected:
    void Process(nexusflow::Message& message) override {
        switch (m_spec.kind) {
        case WorkloadKind::None:
            break;
        case WorkloadKind::Delay:
            WaitForDelay();
            break;
        case WorkloadKind::Compute:
            RunCompute();
            break;
        case WorkloadKind::Memory:
            RunMemory();
            break;
        }
        Broadcast(message);
    }

private:
    /// A plain sleep_for on Windows is coalesced to the 15.6 ms default tick
    /// once the process has run for a few seconds: the OS stops honouring the
    /// process-wide timeBeginPeriod request for Sleep even though
    /// NtQueryTimerResolution still reports 1 ms. A high-resolution waitable
    /// timer is not affected, so the delay workload waits on one and falls back
    /// to sleep_for only when the timer cannot be created.
    void WaitForDelay() {
#if defined(_WIN32)
        if (m_delayTimer != nullptr) {
            LARGE_INTEGER due;
            due.QuadPart = -static_cast<LONGLONG>(m_spec.delayUs) * 10;  // relative, 100 ns units
            if (SetWaitableTimer(m_delayTimer, &due, 0, nullptr, nullptr, FALSE) != 0) {
                WaitForSingleObject(m_delayTimer, INFINITE);
                return;
            }
        }
#endif
        std::this_thread::sleep_for(std::chrono::microseconds(m_spec.delayUs));
    }

    void RunCompute() {
        // The member write keeps the chain observable, so the compiler cannot
        // fold the loop away; the result itself is meaningless.
        double value = m_computeGuard;
        for (int i = 0; i < m_spec.computeIterations; ++i) {
            const double x = static_cast<double>(i) * 0.001;
            value += std::sin(x) * std::cos(x);
        }
        m_computeGuard = value;
    }

    void RunMemory() {
        if (m_buffer.empty()) {
            return;
        }
        // Move the second half to the front and the first half to the back, so
        // both halves are read and written once per message.
        const std::size_t half = m_buffer.size() / 2;
        const std::size_t rest = m_buffer.size() - half;
        std::memcpy(m_scratch.data(), m_buffer.data(), half);
        std::memcpy(m_buffer.data(), m_buffer.data() + half, rest);
        std::memcpy(m_buffer.data() + rest, m_scratch.data(), half);
    }

    WorkloadSpec m_spec;
    double m_computeGuard = 0.0;
    std::vector<std::uint8_t> m_buffer;
    std::vector<std::uint8_t> m_scratch;
#if defined(_WIN32)
    HANDLE m_delayTimer = nullptr;
#endif
};

/**
 * @class SinkModule
 * @brief Terminal stage: records end-to-end latency and counts what it received.
 *
 * Handles both shapes a sink can be handed:
 *  - a plain BenchmarkPayload, when the sink reads its inputs without join;
 *  - a joined map of Messages, when the pipeline runs it with `syncInputs`, which
 *    is what turns several branches back into one logical frame.
 */
class SinkModule : public nexusflow::Module {
public:
    SinkModule(std::string name, bool measureLatency = true)
        : nexusflow::Module(std::move(name)), m_measureLatency(measureLatency) {}

    std::uint64_t Count() const { return m_count; }
    const LatencyHistogram& Latency() const { return m_latency; }

protected:
    void Process(nexusflow::Message& message) override {
        ++m_count;
        if (const BenchmarkPayload* payload = message.BorrowPtr<BenchmarkPayload>()) {
            Record(*payload);
            return;
        }
        // Joined shape: the framework hands over every input for one message id.
        if (const auto* joined = message.BorrowPtr<std::unordered_map<std::string, nexusflow::Message>>()) {
            for (const auto& entry : *joined) {
                if (const BenchmarkPayload* payload = entry.second.BorrowPtr<BenchmarkPayload>()) {
                    Record(*payload);
                    return;
                }
            }
        }
    }

private:
    void Record(const BenchmarkPayload& payload) {
        // `--observe none`: the second clock read and the histogram sample are the
        // harness's own cost, so they are skipped rather than counted as the
        // pipeline's.
        if (!m_measureLatency) {
            return;
        }
        const auto latency = Clock::now() - payload.creationTime;
        const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(latency).count();
        m_latency.Record(ns > 0 ? static_cast<std::uint64_t>(ns) : 0);
    }

    bool m_measureLatency = true;
    std::uint64_t m_count = 0;
    LatencyHistogram m_latency;
};

} // namespace nexusflow_bench

#endif // NEXUSFLOW_BENCHMARKS_BENCHMARK_NODES_HPP
