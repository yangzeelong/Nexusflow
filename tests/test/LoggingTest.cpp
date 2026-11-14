#include <nexusflow/Logging.hpp>

#include <gtest/gtest.h>

#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace {

using namespace nexusflow;

class MemorySink final : public log::LogSink {
public:
    void Write(const log::LogRecord& record) override {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_records.push_back(record);
    }

    std::vector<log::LogRecord> Records() {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_records;
    }

private:
    std::mutex m_mutex;
    std::vector<log::LogRecord> m_records;
};

// Installs a memory sink for the duration of a test and restores the previous
// level and sink afterwards so tests do not leak global logging state.
class ScopedLogCapture {
public:
    explicit ScopedLogCapture(std::shared_ptr<MemorySink> sink)
        : m_previousLevel(log::GetLogLevel()), m_previousSink(log::GetSink()) {
        log::SetLogLevel(log::Level::TRACE);
        log::SetSink(std::move(sink));
    }

    ~ScopedLogCapture() {
        log::SetSink(m_previousSink);
        log::SetLogLevel(m_previousLevel);
    }

    ScopedLogCapture(const ScopedLogCapture&) = delete;
    ScopedLogCapture& operator=(const ScopedLogCapture&) = delete;

private:
    log::Level m_previousLevel;
    std::shared_ptr<log::LogSink> m_previousSink;
};

} // namespace

TEST(LoggingTest, CapturesStructuredRecord) {
    auto sink = std::make_shared<MemorySink>();
    ScopedLogCapture capture(sink);

    LOG_INFO("hello {}", 42);

    const std::vector<log::LogRecord> records = sink->Records();
    ASSERT_EQ(records.size(), 1u);
    EXPECT_EQ(records[0].level, log::Level::INFO);
    EXPECT_EQ(records[0].message, "hello 42");
    EXPECT_EQ(std::string(records[0].file), "LoggingTest.cpp");
    EXPECT_GT(records[0].line, 0);
    EXPECT_FALSE(std::string(records[0].function).empty());
    EXPECT_NE(records[0].timestamp.find('.'), std::string::npos);
}

TEST(LoggingTest, FormatsPlaceholdersAndEscapedBraces) {
    auto sink = std::make_shared<MemorySink>();
    ScopedLogCapture capture(sink);

    LOG_WARN("{} {:.2f} {:.0f} {{literal}} {}", "a", 3.14159, 2.7, true);

    const std::vector<log::LogRecord> records = sink->Records();
    ASSERT_EQ(records.size(), 1u);
    EXPECT_EQ(records[0].message, "a 3.14 3 {literal} true");
}

TEST(LoggingTest, FiltersRecordsByLevel) {
    auto sink = std::make_shared<MemorySink>();
    ScopedLogCapture capture(sink);

    log::SetLogLevel(log::Level::WARN);
    LOG_INFO("hidden");
    LOG_WARN("shown");

    const std::vector<log::LogRecord> records = sink->Records();
    ASSERT_EQ(records.size(), 1u);
    EXPECT_EQ(records[0].message, "shown");
}

TEST(LoggingTest, OffDisablesEveryLevel) {
    auto sink = std::make_shared<MemorySink>();
    ScopedLogCapture capture(sink);

    log::SetLogLevel(log::Level::OFF);
    EXPECT_FALSE(log::IsLevelEnabled(log::Level::CRITICAL));
    LOG_CRITICAL("not captured");
    EXPECT_TRUE(sink->Records().empty());
}

TEST(LoggingTest, NullSinkRestoresTheDefaultSink) {
    auto sink = std::make_shared<MemorySink>();
    ScopedLogCapture capture(sink);

    log::SetSink(nullptr);
    LOG_WARN("not captured after reset");
    EXPECT_TRUE(sink->Records().empty());
}

TEST(LoggingTest, StreamStyleLoggingIsCaptured) {
    auto sink = std::make_shared<MemorySink>();
    ScopedLogCapture capture(sink);

    LOG_STREAM(WARN) << "streamed " << 7;

    const std::vector<log::LogRecord> records = sink->Records();
    ASSERT_EQ(records.size(), 1u);
    EXPECT_EQ(records[0].level, log::Level::WARN);
    EXPECT_EQ(records[0].message, "streamed 7");
}
