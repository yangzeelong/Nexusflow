#include <nexusflow/Config.hpp>
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

TEST(ConfigTest, MissingKeyReturnsTheDefaultWithoutWarning) {
    auto sink = std::make_shared<MemorySink>();
    ScopedLogCapture capture(sink);

    Config config;
    EXPECT_EQ(config.GetValueOrDefault<int>("missing", 7), 7);
    EXPECT_TRUE(config.GetConfigMap().empty());
    EXPECT_TRUE(sink->Records().empty());
}

TEST(ConfigTest, MatchingTypeReturnsTheStoredValue) {
    auto sink = std::make_shared<MemorySink>();
    ScopedLogCapture capture(sink);

    Config config;
    config.Add<int>("maxBatchSize", 16);

    EXPECT_EQ(config.GetValueOrDefault<int>("maxBatchSize", 4), 16);
    EXPECT_TRUE(sink->Records().empty());
}

TEST(ConfigTest, TypeMismatchWarnsAndReturnsTheDefault) {
    auto sink = std::make_shared<MemorySink>();
    ScopedLogCapture capture(sink);

    Config config;
    config.Add<std::string>("maxBatchSize", "16"); // A typo the old code ignored.

    EXPECT_EQ(config.GetValueOrDefault<int>("maxBatchSize", 4), 4);

    const std::vector<log::LogRecord> records = sink->Records();
    ASSERT_EQ(records.size(), 1u);
    EXPECT_EQ(records[0].level, log::Level::WARN);
    EXPECT_NE(records[0].message.find("maxBatchSize"), std::string::npos);
}
