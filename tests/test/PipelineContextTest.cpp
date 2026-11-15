#include <nexusflow/ErrorCode.hpp>
#include <nexusflow/Logging.hpp>
#include <nexusflow/Message.hpp>
#include <nexusflow/Module.hpp>
#include <nexusflow/Pipeline.hpp>
#include <nexusflow/PipelineBuilder.hpp>
#include <nexusflow/PipelineContext.hpp>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace {

using namespace nexusflow;

// Collects the records a sink receives so a test can inspect them.
class MemorySink final : public log::LogSink {
public:
    void Write(const log::LogRecord& record) override {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_records.push_back(record);
    }

    std::size_t Size() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_records.size();
    }

    std::vector<log::LogRecord> Records() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_records;
    }

private:
    mutable std::mutex m_mutex;
    std::vector<log::LogRecord> m_records;
};

// Swallows records so a test does not print the framework's own logs.
class NullSink final : public log::LogSink {
public:
    void Write(const log::LogRecord&) override {}
};

class ScopedGlobalSink {
public:
    explicit ScopedGlobalSink(std::shared_ptr<log::LogSink> sink) : m_previous(log::GetSink()) {
        log::SetSink(std::move(sink));
    }
    ~ScopedGlobalSink() { log::SetSink(m_previous); }

private:
    std::shared_ptr<log::LogSink> m_previous;
};

class ScopedLogLevel {
public:
    explicit ScopedLogLevel(log::Level level) : m_previous(log::GetLogLevel()) { log::SetLogLevel(level); }
    ~ScopedLogLevel() { log::SetLogLevel(m_previous); }

private:
    log::Level m_previous;
};

class ContextSourceModule : public Module {
public:
    explicit ContextSourceModule(const std::string& name) : Module(name) {}

    std::shared_ptr<PipelineContext> context;

protected:
    ErrorCode Init() override {
        context = GetContext();
        if (context) {
            // Metric names carry the module name: the context is shared by the
            // whole pipeline, so a bare "init" would collide across modules.
            m_initMetric = context->Intern(GetModuleName() + ".init");
            m_framesMetric = context->Intern(GetModuleName() + ".frames");
            context->Record(m_initMetric, 1.0);
        }
        return ErrorCode::SUCCESS;
    }

    void Process(Message& inputMessage) override {
        (void)inputMessage;
        if (context) {
            context->Record(m_framesMetric, 947.0);
        }
        LOG_DEBUG("[context-test] processed a frame");
        Broadcast(std::move(MakeMessage(1)));
    }

private:
    PipelineContext::MetricHandle m_initMetric = 0;
    PipelineContext::MetricHandle m_framesMetric = 0;
};

class ContextSinkModule : public Module {
public:
    explicit ContextSinkModule(const std::string& name) : Module(name) {}

    std::atomic<int> received{0};

protected:
    void Process(Message& inputMessage) override {
        (void)inputMessage;
        received.fetch_add(1, std::memory_order_relaxed);
    }
};

std::unique_ptr<Pipeline> BuildContextPipeline(const std::shared_ptr<ContextSourceModule>& source,
                                               const std::shared_ptr<ContextSinkModule>& sink) {
    return PipelineBuilder()
        .AddModule(source)
        .AddModule(sink)
        .Connect(source->GetModuleName(), sink->GetModuleName())
        .Build();
}

bool HasMarker(const std::vector<log::LogRecord>& records, const std::string& marker) {
    for (const auto& record : records) {
        if (record.message.find(marker) != std::string::npos) {
            return true;
        }
    }
    return false;
}

} // namespace

TEST(PipelineContextTest, ModulesReceiveThePipelineContext) {
    ScopedGlobalSink quiet(std::make_shared<NullSink>());
    auto source = std::make_shared<ContextSourceModule>("context-source");
    auto sink = std::make_shared<ContextSinkModule>("context-sink");
    auto pipeline = BuildContextPipeline(source, sink);
    ASSERT_NE(pipeline, nullptr);

    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    const auto context = pipeline->GetContext();
    ASSERT_NE(context, nullptr);
    EXPECT_FALSE(context->GetName().empty());
    EXPECT_EQ(source->context, context);
    EXPECT_FALSE(context->StopRequested());

    ASSERT_EQ(pipeline->DeInit(), ErrorCode::SUCCESS);
}

TEST(PipelineContextTest, ContextSinkReceivesWorkerLogs) {
    ScopedLogLevel level(log::Level::DEBUG);
    auto global = std::make_shared<MemorySink>();
    ScopedGlobalSink quiet(global);
    auto contextSink = std::make_shared<MemorySink>();

    auto source = std::make_shared<ContextSourceModule>("context-source");
    auto sink = std::make_shared<ContextSinkModule>("context-sink");
    auto pipeline = BuildContextPipeline(source, sink);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);

    auto context = pipeline->GetContext();
    ASSERT_NE(context, nullptr);
    context->SetSink(contextSink);

    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
    ASSERT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->DeInit(), ErrorCode::SUCCESS);

    // The worker thread's records go to the pipeline sink, not the process sink.
    EXPECT_TRUE(HasMarker(contextSink->Records(), "[context-test]"));
    EXPECT_FALSE(HasMarker(global->Records(), "[context-test]"));
}

TEST(PipelineContextTest, ReportedMetricsAreAccumulated) {
    ScopedGlobalSink quiet(std::make_shared<NullSink>());
    auto source = std::make_shared<ContextSourceModule>("context-source");
    auto sink = std::make_shared<ContextSinkModule>("context-sink");
    auto pipeline = BuildContextPipeline(source, sink);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);

    auto context = pipeline->GetContext();
    ASSERT_NE(context, nullptr);
    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
    ASSERT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);

    bool sawInit = false;
    bool sawFrames = false;
    for (const auto& metric : context->GetReportedMetrics()) {
        if (metric.name == "context-source.init") {
            sawInit = true;
            EXPECT_EQ(metric.samples, 1u);
            EXPECT_DOUBLE_EQ(metric.last, 1.0);
        }
        if (metric.name == "context-source.frames") {
            sawFrames = true;
            EXPECT_GT(metric.samples, 0u);
            EXPECT_GT(metric.Average(), 0.0);
        }
    }
    EXPECT_TRUE(sawInit);
    EXPECT_TRUE(sawFrames);

    ASSERT_EQ(pipeline->DeInit(), ErrorCode::SUCCESS);
}

// Interning is idempotent: the same name resolves to the same handle, and a
// handle keeps accumulating under its slot instead of creating a second entry.
TEST(PipelineContextTest, InternedMetricHandleIsReusedForTheSameName) {
    PipelineContext context("intern-test");

    const PipelineContext::MetricHandle first = context.Intern("mod.latency");
    const PipelineContext::MetricHandle again = context.Intern("mod.latency");
    ASSERT_NE(first, PipelineContext::MetricHandle(0));
    EXPECT_EQ(first, again);

    context.Record(first, 2.0);
    context.Record(first, 4.0);
    context.Record("mod.latency", 6.0);

    const std::vector<PipelineContext::Metric> metrics = context.GetReportedMetrics();
    ASSERT_EQ(metrics.size(), std::size_t(1));
    EXPECT_EQ(metrics[0].name, "mod.latency");
    EXPECT_EQ(metrics[0].samples, 3u);
    EXPECT_DOUBLE_EQ(metrics[0].last, 6.0);
    EXPECT_DOUBLE_EQ(metrics[0].Average(), 4.0);
}

// A handle from nowhere must be ignored rather than index out of bounds.
TEST(PipelineContextTest, UnknownMetricHandleIsIgnored) {
    PipelineContext context("bad-handle-test");
    context.Record(PipelineContext::MetricHandle(0), 1.0);
    context.Record(PipelineContext::MetricHandle(99), 1.0);
    EXPECT_TRUE(context.GetReportedMetrics().empty());
}

// The read side has three shapes: copy everything, ask how many, or visit each
// one under a single lock. All three must agree, including after a metric was
// added while a handle was already handed out.
TEST(PipelineContextTest, MetricReadersAgreeWithEachOther) {
    PipelineContext context("reader-test");
    const PipelineContext::MetricHandle first = context.Intern("mod.first");
    context.Record(first, 1.0);
    context.Record(first, 3.0);
    const PipelineContext::MetricHandle second = context.Intern("mod.second");
    context.Record(second, 5.0);

    EXPECT_EQ(context.GetReportedMetricCount(), std::size_t(2));

    const std::vector<PipelineContext::Metric> copied = context.GetReportedMetrics();
    ASSERT_EQ(copied.size(), std::size_t(2));

    std::size_t visited = 0;
    context.ForEachMetric([&](const PipelineContext::Metric& metric) {
        ASSERT_LT(visited, copied.size());
        EXPECT_EQ(metric.name, copied[visited].name);
        EXPECT_EQ(metric.samples, copied[visited].samples);
        EXPECT_DOUBLE_EQ(metric.total, copied[visited].total);
        ++visited;
    });
    EXPECT_EQ(visited, copied.size());

    const PipelineContext::Metric* byHandle = context.FindMetric(first);
    ASSERT_NE(byHandle, nullptr);
    EXPECT_EQ(byHandle->name, "mod.first");
    EXPECT_DOUBLE_EQ(byHandle->Average(), 2.0);
    EXPECT_EQ(context.FindMetric(PipelineContext::MetricHandle(0)), nullptr);
    EXPECT_EQ(context.FindMetric(PipelineContext::MetricHandle(99)), nullptr);
}

TEST(PipelineContextTest, StopRequestsTheStopToken) {
    ScopedGlobalSink quiet(std::make_shared<NullSink>());
    auto source = std::make_shared<ContextSourceModule>("context-source");    auto sink = std::make_shared<ContextSinkModule>("context-sink");
    auto pipeline = BuildContextPipeline(source, sink);
    ASSERT_NE(pipeline, nullptr);
    ASSERT_EQ(pipeline->Init(), ErrorCode::SUCCESS);
    auto context = pipeline->GetContext();
    ASSERT_NE(context, nullptr);
    EXPECT_FALSE(context->StopRequested());

    ASSERT_EQ(pipeline->Start(), ErrorCode::SUCCESS);
    ASSERT_EQ(pipeline->Stop(), ErrorCode::SUCCESS);
    EXPECT_TRUE(context->StopRequested());

    ASSERT_EQ(pipeline->DeInit(), ErrorCode::SUCCESS);
}

TEST(PipelineContextTest, ContextCanBeDetachedWithoutBreakingTheModule) {
    auto source = std::make_shared<ContextSourceModule>("standalone-source");
    EXPECT_EQ(source->GetContext(), nullptr);
    source->AttachContext(nullptr);
    EXPECT_EQ(source->GetContext(), nullptr);
}
