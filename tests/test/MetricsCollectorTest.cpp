#include "nexusflow/MetricsCollector.hpp"

#include "gtest/gtest.h"

#include <chrono>
#include <thread>
#include <vector>

using nexusflow::profiling::TimerPrintPolicy;
using nexusflow::profiling::MetricsCollector;
using nexusflow::profiling::MetricSamples;

TEST(MetricsCollectorTest, CountsOneItemPerMeasurementByDefault) {
    auto& registry = MetricsCollector::Instance();
    registry.Reset();

    registry.AddItems("test.aggregate", 10.0);
    registry.AddItems("test.aggregate", 20.0);

    MetricSamples stats;
    ASSERT_TRUE(registry.GetStats("test.aggregate", stats));
    EXPECT_EQ(stats.items, 2U);
    EXPECT_DOUBLE_EQ(stats.sumMs, 30.0);
    // Every duration is per item, so a two-item timer reports both samples.
    EXPECT_DOUBLE_EQ(stats.AvgItemMs(), 15.0);
    // No throughput field: a rate needs a wall-clock span, which cumulative
    // samples do not carry. Two snapshots give it back — see README.
    EXPECT_DOUBLE_EQ(stats.minMs, 10.0);
    EXPECT_DOUBLE_EQ(stats.maxMs, 20.0);
    // Nearest rank over both items: 0.95 * (2 - 1) rounds to the maximum.
    EXPECT_DOUBLE_EQ(stats.p95Ms, 20.0);
}

TEST(MetricsCollectorTest, DeclaredItemsNormalizeEveryDuration) {
    auto& registry = MetricsCollector::Instance();
    registry.Reset();

    // One measurement covering four items: the reported numbers describe a
    // single item, both in the average and in the extremes.
    registry.AddItems("test.batch", 40.0, 4);
    registry.AddItems("test.batch", 80.0, 8);

    MetricSamples stats;
    ASSERT_TRUE(registry.GetStats("test.batch", stats));
    EXPECT_EQ(stats.items, 12U);
    EXPECT_DOUBLE_EQ(stats.sumMs, 120.0);
    EXPECT_DOUBLE_EQ(stats.AvgItemMs(), 10.0);
    EXPECT_DOUBLE_EQ(stats.minMs, 10.0);
    EXPECT_DOUBLE_EQ(stats.maxMs, 10.0);
    EXPECT_DOUBLE_EQ(stats.p95Ms, 10.0);
}

TEST(MetricsCollectorTest, SupportsScopedAndIncrementedItems) {
    auto& registry = MetricsCollector::Instance();
    registry.Reset();

    registry.StartAverage("test.scoped", TimerPrintPolicy::EveryMs(1000));
    registry.IncrementAverage("test.scoped", 5);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    const double elapsedMs = registry.EndAverage("test.scoped");

    EXPECT_GT(elapsedMs, 0.0);
    MetricSamples stats;
    ASSERT_TRUE(registry.GetStats("test.scoped", stats));
    EXPECT_EQ(stats.items, 5U);
    EXPECT_DOUBLE_EQ(stats.sumMs, elapsedMs);
    EXPECT_DOUBLE_EQ(stats.AvgItemMs(), elapsedMs / 5.0);
    // The single call is normalized by the declared count.
    EXPECT_DOUBLE_EQ(stats.maxMs, elapsedMs / 5.0);

    {
        auto timer = registry.Scope("test.raii", TimerPrintPolicy::EveryMs(1000));
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(registry.GetStats("test.raii", stats));
    EXPECT_EQ(stats.items, 1U);
    EXPECT_GT(stats.sumMs, 0.0);

    {
        auto timer =
            registry.Scope("test.every-call", TimerPrintPolicy::EachCall());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(registry.GetStats("test.every-call", stats));
    EXPECT_EQ(stats.items, 1U);

    // Two measurements that declare nothing count two items.
    registry.StartAverage("test.two-calls", TimerPrintPolicy::EveryMs(1000));
    registry.EndAverage("test.two-calls");
    registry.StartAverage("test.two-calls", TimerPrintPolicy::EveryMs(1000));
    registry.EndAverage("test.two-calls");
    ASSERT_TRUE(registry.GetStats("test.two-calls", stats));
    EXPECT_EQ(stats.items, 2U);
}

TEST(MetricsCollectorTest, ReportingPoliciesDoNotAffectAggregation) {
    auto& registry = MetricsCollector::Instance();
    registry.Reset();

    // Windowed reporting resets its window every N measurements, and the
    // cumulative numbers have to survive that.
    for (int call = 0; call < 2; ++call) {
        registry.StartAverage("test.interval", TimerPrintPolicy::Interval(2));
        registry.IncrementAverage("test.interval", 4);
        registry.EndAverage("test.interval");
    }
    registry.StartAverage("test.interval", TimerPrintPolicy::EveryCalls(2));
    registry.EndAverage("test.interval");

    MetricSamples stats;
    ASSERT_TRUE(registry.GetStats("test.interval", stats));
    EXPECT_EQ(stats.items, 9U);
    EXPECT_GT(stats.sumMs, 0.0);
}

TEST(MetricsCollectorTest, AggregationIsThreadSafe) {
    auto& registry = MetricsCollector::Instance();
    registry.Reset();

    constexpr int kThreadCount = 4;
    constexpr int kItemsPerThread = 100;
    std::vector<std::thread> workers;
    workers.reserve(kThreadCount);
    for (int thread = 0; thread < kThreadCount; ++thread) {
        workers.emplace_back([&registry, kItemsPerThread]() {
            for (int item = 0; item < kItemsPerThread; ++item) {
                registry.AddItems("test.concurrent", 2.0, 3);
            }
        });
    }
    for (auto& worker : workers) worker.join();

    MetricSamples stats;
    ASSERT_TRUE(registry.GetStats("test.concurrent", stats));
    EXPECT_EQ(stats.items, static_cast<uint64_t>(kThreadCount * kItemsPerThread * 3));
    EXPECT_DOUBLE_EQ(stats.sumMs,
                     static_cast<double>(kThreadCount * kItemsPerThread * 2));
    // 2 ms per measurement spread over 3 items.
    EXPECT_DOUBLE_EQ(stats.AvgItemMs(), 2.0 / 3.0);
    EXPECT_DOUBLE_EQ(stats.maxMs, 2.0 / 3.0);
}

TEST(MetricsCollectorTest, InternedHandleAddressesTheSameTimerAsTheName) {
    auto& registry = MetricsCollector::Instance();
    registry.Reset();

    const auto handle = registry.Intern("test.handle");
    ASSERT_NE(handle, 0U);
    // Interning is idempotent, and an empty name never becomes a timer.
    EXPECT_EQ(handle, registry.Intern("test.handle"));
    EXPECT_NE(handle, registry.Intern("test.handle.other"));
    EXPECT_EQ(registry.Intern(""), 0U);

    // The two families feed the same aggregate: one measurement by handle, one
    // by name.
    registry.AddItems(handle, 10.0);
    registry.AddItems("test.handle", 20.0);

    MetricSamples byName;
    MetricSamples byHandle;
    ASSERT_TRUE(registry.GetStats("test.handle", byName));
    ASSERT_TRUE(registry.GetStats(handle, byHandle));
    EXPECT_EQ(byName.items, 2U);
    EXPECT_EQ(byHandle.items, 2U);
    EXPECT_DOUBLE_EQ(byHandle.sumMs, 30.0);
    EXPECT_EQ(byHandle.name, "test.handle");
    EXPECT_EQ(byHandle.minMs, 10.0);
    EXPECT_EQ(byHandle.maxMs, 20.0);
}

TEST(MetricsCollectorTest, HandleFormsMeasureAndScopeLikeTheNameForms) {
    auto& registry = MetricsCollector::Instance();
    registry.Reset();

    const auto handle = registry.Intern("test.handle.scope");
    {
        auto scope = registry.Scope(handle, TimerPrintPolicy::EveryCalls(1000));
        (void)scope;
    }

    MetricSamples stats;
    ASSERT_TRUE(registry.GetStats(handle, stats));
    EXPECT_EQ(stats.items, 1U);
    EXPECT_GT(stats.sumMs, 0.0);

    // A handle keeps its meaning across a reset, and the next measurement
    // re-captures the call site exactly as a fresh name would.
    registry.Reset();
    EXPECT_FALSE(registry.GetStats(handle, stats));
    EXPECT_TRUE(registry.Snapshot().empty());

    registry.StartAverage(handle, TimerPrintPolicy::EveryCalls(1000), __FILE__, __LINE__);
    registry.IncrementAverage(handle, 4);
    registry.IncrementAverage(handle, 6);
    registry.EndAverage(handle);
    ASSERT_TRUE(registry.GetStats(handle, stats));
    EXPECT_EQ(stats.items, 10U);
    EXPECT_EQ(stats.file, __FILE__);
}

TEST(MetricsCollectorTest, InvalidHandleIsANoOp) {
    auto& registry = MetricsCollector::Instance();
    registry.Reset();

    // 0 is what Intern() returns for an uninterned name; every handle form has
    // to tolerate it so callers can treat a missing timer as "measure nothing".
    registry.StartAverage(0U, TimerPrintPolicy::EveryCalls(1));
    registry.IncrementAverage(0U, 5);
    EXPECT_DOUBLE_EQ(registry.EndAverage(0U), 0.0);
    registry.AddItems(0U, 10.0);

    MetricSamples stats;
    EXPECT_FALSE(registry.GetStats(0U, stats));
    EXPECT_TRUE(registry.Snapshot().empty());
}

