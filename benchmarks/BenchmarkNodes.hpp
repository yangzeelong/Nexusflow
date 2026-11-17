#ifndef NEXUSFLOW_BENCHMARKS_BENCHMARK_NODES_HPP
#define NEXUSFLOW_BENCHMARKS_BENCHMARK_NODES_HPP

#include "LatencyHistogram.hpp"

#include <nexusflow/Message.hpp>
#include <nexusflow/Module.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
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

/**
 * @class SourceModule
 * @brief Generates one message per invocation, optionally with a frame.
 *
 * Two payload modes, because they answer different questions:
 *
 *  - reuse (default): one frame buffer is allocated once and shared by every
 *    message. This is what proves the framework's claim that copying a Message
 *    copies a pointer, not pixels -- with 05720p payloads the throughput should
 *    stay close to the empty-payload run.
 *  - fresh (`--payload-fresh`): a new buffer per message, the way a decoder hands
 *    over a fresh picture. This measures allocation and fill cost instead of
 *    framework cost, and is dominated by page faults on any real size.
 */
class SourceModule : public nexusflow::Module {
public:
    SourceModule(std::string name, PayloadSpec spec, bool freshPerMessage)
        : nexusflow::Module(std::move(name)), m_spec(spec), m_freshPerMessage(freshPerMessage) {}

protected:
    void Process(nexusflow::Message&) override {
        BenchmarkPayload payload;
        payload.creationTime = Clock::now();
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
    std::shared_ptr<const BenchmarkFrame> MakeFrame() {
        auto frame = std::make_shared<BenchmarkFrame>();
        frame->width = m_spec.width;
        frame->height = m_spec.height;
        frame->channels = m_spec.channels;
        // The zero fill stands in for the decoder writing the picture.
        frame->bytes = std::make_shared<const std::vector<std::uint8_t>>(m_spec.bytes, static_cast<std::uint8_t>(0));
        return frame;
    }

    PayloadSpec m_spec;
    bool m_freshPerMessage = false;
    std::shared_ptr<const BenchmarkFrame> m_sharedFrame;
};

/// Forwards whatever it receives. The zero-work stage that isolates framework cost.
class PassThroughModule : public nexusflow::Module {
public:
    explicit PassThroughModule(std::string name) : nexusflow::Module(std::move(name)) {}

protected:
    void Process(nexusflow::Message& message) override { Broadcast(message); }
};

/**
 * @class SinkModule
 * @brief Terminal stage: records end-to-end latency and counts what it received.
 *
 * Handles both shapes a sink can be handed:
 *  - a plain BenchmarkPayload, when the sink reads its inputs without fusion;
 *  - a fused map of Messages, when the pipeline runs it with `syncInputs`, which
 *    is what turns several branches back into one logical frame.
 */
class SinkModule : public nexusflow::Module {
public:
    explicit SinkModule(std::string name) : nexusflow::Module(std::move(name)) {}

    std::uint64_t Count() const { return m_count; }
    const LatencyHistogram& Latency() const { return m_latency; }
    std::uint64_t FrameCount() const { return m_frameCount; }

protected:
    void Process(nexusflow::Message& message) override {
        ++m_count;
        if (const BenchmarkPayload* payload = message.BorrowPtr<BenchmarkPayload>()) {
            Record(*payload);
            return;
        }
        // Fused shape: the framework hands over every input for one message id.
        if (const auto* fused = message.BorrowPtr<std::unordered_map<std::string, nexusflow::Message>>()) {
            for (const auto& entry : *fused) {
                if (const BenchmarkPayload* payload = entry.second.BorrowPtr<BenchmarkPayload>()) {
                    Record(*payload);
                    ++m_frameCount;
                    return;
                }
            }
        }
    }

private:
    void Record(const BenchmarkPayload& payload) {
        const auto latency = Clock::now() - payload.creationTime;
        const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(latency).count();
        m_latency.Record(ns > 0 ? static_cast<std::uint64_t>(ns) : 0);
    }

    std::uint64_t m_count = 0;
    std::uint64_t m_frameCount = 0;
    LatencyHistogram m_latency;
};

} // namespace nexusflow_bench

#endif // NEXUSFLOW_BENCHMARKS_BENCHMARK_NODES_HPP
