#include <nexusflow/Logging.hpp>

#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <ctime>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace nexusflow {
namespace log {

namespace {

std::atomic<Level>& CurrentLevel() {
    static std::atomic<Level> level{Level::DEBUG};
    return level;
}

spdlog::level::level_enum ToSpdlogLevel(Level level) {
    switch (level) {
        case Level::TRACE:
            return spdlog::level::trace;
        case Level::DEBUG:
            return spdlog::level::debug;
        case Level::INFO:
            return spdlog::level::info;
        case Level::WARN:
            return spdlog::level::warn;
        case Level::ERR:
            return spdlog::level::err;
        case Level::CRITICAL:
            return spdlog::level::critical;
        case Level::OFF:
            break;
    }
    return spdlog::level::off;
}

std::string FormatTimestamp() {
    const std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    const std::time_t seconds = std::chrono::system_clock::to_time_t(now);
    const long long millis =
        static_cast<long long>(std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() % 1000);

    std::tm localTime{};
#ifdef _WIN32
    localtime_s(&localTime, &seconds);
#else
    localtime_r(&seconds, &localTime);
#endif

    char buffer[40];
    std::snprintf(buffer, sizeof(buffer), "%04d-%02d-%02d %02d:%02d:%02d.%03lld", localTime.tm_year + 1900,
                  localTime.tm_mon + 1, localTime.tm_mday, localTime.tm_hour, localTime.tm_min, localTime.tm_sec,
                  millis);
    return std::string(buffer);
}

// Default sink: forwards records to the current spdlog default logger.
class SpdlogSink final : public LogSink {
public:
    void Write(const LogRecord& record) override {
        const std::shared_ptr<spdlog::logger> logger = spdlog::default_logger();
        if (!logger) {
            return;
        }
        logger->log(spdlog::source_loc{record.file, record.line, record.function}, ToSpdlogLevel(record.level), "{}",
                    record.message);
    }
};

std::mutex& SinkMutex() {
    static std::mutex mutex;
    return mutex;
}

std::shared_ptr<LogSink>& CurrentSink() {
    static std::shared_ptr<LogSink> sink = std::make_shared<SpdlogSink>();
    return sink;
}

char PathSeparator() {
#ifdef _WIN32
    return '\\';
#else
    return '/';
#endif
}

}  // namespace

void SetLogLevel(Level level) { CurrentLevel().store(level, std::memory_order_relaxed); }

Level GetLogLevel() { return CurrentLevel().load(std::memory_order_relaxed); }

bool IsLevelEnabled(Level level) {
    if (level == Level::OFF) {
        return false;
    }
    return static_cast<int>(level) >= static_cast<int>(GetLogLevel());
}

void SetSink(std::shared_ptr<LogSink> sink) {
    std::lock_guard<std::mutex> lock(SinkMutex());
    if (sink) {
        CurrentSink() = std::move(sink);
    } else {
        CurrentSink() = std::make_shared<SpdlogSink>();
    }
}

std::shared_ptr<LogSink> GetSink() {
    std::lock_guard<std::mutex> lock(SinkMutex());
    return CurrentSink();
}

void InitializeGlobalLogger(LoggerParam param) {
    std::vector<spdlog::sink_ptr> sinks;
    sinks.push_back(std::make_shared<spdlog::sinks::stdout_color_sink_mt>());

    const std::string name = param.logName.empty() ? std::string("nexusflow") : param.logName;

    // If logDir is not empty, add a daily rotating file sink.
    if (!param.logDir.empty()) {
        const std::string path = param.logDir + PathSeparator() + name;
        sinks.push_back(std::make_shared<spdlog::sinks::daily_file_sink_mt>(path, 0, 0, false,
                                                                           param.maxRetentionDays));
    }

    std::shared_ptr<spdlog::logger> logger = std::make_shared<spdlog::logger>(name, sinks.begin(), sinks.end());
    logger->set_level(ToSpdlogLevel(param.logLevel));
    // log_level, datetime, thread_id, file_name, line_num, message
    // eg: D25-0627 01:08:45.513 T44956 TextureManager.cpp:43] Cleaning up textures
    logger->set_pattern("%^%L%$%C-%m%d %T.%e T%t %s:%#] %v");

    spdlog::set_default_logger(logger);
    spdlog::flush_on(ToSpdlogLevel(param.logLevel));
    spdlog::flush_every(std::chrono::seconds(1));

    SetLogLevel(param.logLevel);
    SetSink(std::make_shared<SpdlogSink>());
}

namespace detail {

void Write(Level level, const char* file, int line, const char* function, const std::string& message) {
    if (!IsLevelEnabled(level)) {
        return;
    }

    LogRecord record;
    record.level = level;
    record.timestamp = FormatTimestamp();
    record.threadId = static_cast<std::uint64_t>(std::hash<std::thread::id>()(std::this_thread::get_id()));
    record.file = file != nullptr ? FileName(file) : "";
    record.line = line;
    record.function = function != nullptr ? function : "";
    record.message = message;

    std::shared_ptr<LogSink> sink;
    {
        std::lock_guard<std::mutex> lock(SinkMutex());
        sink = CurrentSink();
    }
    if (sink) {
        sink->Write(record);
    }
}

}  // namespace detail
}  // namespace log
}  // namespace nexusflow
