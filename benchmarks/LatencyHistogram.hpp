#ifndef NEXUSFLOW_BENCHMARKS_LATENCY_HISTOGRAM_HPP
#define NEXUSFLOW_BENCHMARKS_LATENCY_HISTOGRAM_HPP

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

namespace nexusflow_bench {

/**
 * @class LatencyHistogram
 * @brief Fixed-size latency histogram good enough for P50/P99 on the hot path.
 *
 * Why not store the samples: the harness measures a frame pipeline, so a single
 * run produces tens of millions of samples (3.5M msg/s * 10 s). Keeping them
 * would cost hundreds of megabytes; this histogram is fixed at ~12 KiB and pays
 * one shift and one multiply per sample.
 *
 * Layout: geometric sub-buckets, 64 per power of two, starting at 1 us and
 * covering up to 2^24 us (~16.7 s). A sample is charged to the sub-bucket that
 * contains it, and a percentile reports that bucket's geometric midpoint, so the
 * reported value is within ~1.6% of the true one. That is why the report says
 * "P50/P99 (histogram)" rather than pretending to be exact.
 *
 * Single writer: Record() is called by the sink module's worker thread only, so
 * no atomics are needed. Reading is only safe after the pipeline has stopped.
 */
class LatencyHistogram {
public:
    /// Sub-buckets per power of two. 64 keeps the relative error under 1.6%.
    static constexpr int kSubBucketBits = 6;
    static constexpr int kSubBuckets = 1 << kSubBucketBits;
    /// Lowest representable value, in nanoseconds (1 us).
    static constexpr std::uint64_t kMinNs = 1000;
    /// Number of powers of two above kMinNs: 24 covers about 16.7 s.
    static constexpr int kOctaves = 24;
    static constexpr std::size_t kTotalBuckets = static_cast<std::size_t>(kOctaves) * kSubBuckets;

    LatencyHistogram() : m_counts(kTotalBuckets, 0) {}

    /// Records one sample. Values below kMinNs land in the first bucket.
    void Record(std::uint64_t valueNs) {
        const std::size_t index = IndexFor(valueNs);
        ++m_counts[index];
        ++m_count;
        m_sumNs += valueNs;
        m_maxNs = std::max(m_maxNs, valueNs);
        m_minNs = std::min(m_minNs, valueNs);
    }

    std::uint64_t Count() const { return m_count; }
    std::uint64_t MaxNs() const { return m_maxNs; }
    std::uint64_t MinNs() const { return m_minNs == std::numeric_limits<std::uint64_t>::max() ? 0 : m_minNs; }

    /// Arithmetic mean in nanoseconds (exact; the sum is kept separately).
    double MeanNs() const { return m_count == 0 ? 0.0 : static_cast<double>(m_sumNs) / static_cast<double>(m_count); }

    /// Nearest-rank percentile, reported as a bucket midpoint. 0 <= p <= 100.
    double PercentileNs(double p) const {
        if (m_count == 0) {
            return 0.0;
        }
        const double clamped = std::min(100.0, std::max(0.0, p));
        const std::uint64_t rank = static_cast<std::uint64_t>(std::ceil(clamped / 100.0 * static_cast<double>(m_count)));
        const std::uint64_t target = rank == 0 ? 1 : rank;
        std::uint64_t seen = 0;
        for (std::size_t i = 0; i < m_counts.size(); ++i) {
            seen += m_counts[i];
            if (seen >= target) {
                return RepresentativeNs(i);
            }
        }
        return static_cast<double>(m_maxNs);
    }

private:
    static int HighestSetBit(std::uint64_t value) {
#if defined(_MSC_VER)
        unsigned long index = 0;
        _BitScanReverse64(&index, value);
        return static_cast<int>(index);
#else
        return 63 - __builtin_clzll(value);
#endif
    }

    static std::size_t IndexFor(std::uint64_t valueNs) {
        // Normalise to multiples of kMinNs first: the buckets are geometric on
        // that scale, so the exponent and the sub-bucket both come from it.
        // Using raw nanoseconds here would report every value kMinNs times too
        // large, because RepresentativeNs multiplies by kMinNs.
        const std::uint64_t scaled = std::max(valueNs / kMinNs, static_cast<std::uint64_t>(1));
        const int exponent = HighestSetBit(scaled);
        const std::uint64_t base = static_cast<std::uint64_t>(1) << exponent;
        const std::uint64_t offset = (scaled - base) * static_cast<std::uint64_t>(kSubBuckets);
        int sub = static_cast<int>(offset / base);
        if (sub >= kSubBuckets) {
            sub = kSubBuckets - 1;
        }
        std::size_t index = static_cast<std::size_t>(exponent) * kSubBuckets + static_cast<std::size_t>(sub);
        if (index >= kTotalBuckets) {
            index = kTotalBuckets - 1;
        }
        return index;
    }

    static double RepresentativeNs(std::size_t index) {
        const int exponent = static_cast<int>(index / kSubBuckets);
        const int sub = static_cast<int>(index % kSubBuckets);
        const double base = static_cast<double>(kMinNs) * static_cast<double>(static_cast<std::uint64_t>(1) << exponent);
        return base * (1.0 + (static_cast<double>(sub) + 0.5) / static_cast<double>(kSubBuckets));
    }

    std::vector<std::uint64_t> m_counts;
    std::uint64_t m_count = 0;
    std::uint64_t m_sumNs = 0;
    std::uint64_t m_maxNs = 0;
    std::uint64_t m_minNs = std::numeric_limits<std::uint64_t>::max();
};

} // namespace nexusflow_bench

#endif // NEXUSFLOW_BENCHMARKS_LATENCY_HISTOGRAM_HPP
