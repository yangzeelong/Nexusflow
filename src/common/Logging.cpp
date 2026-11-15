#include <nexusflow/Logging.hpp>

#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <unistd.h>
#if defined(__linux__)
#include <sys/syscall.h>
#endif
#endif

namespace nexusflow {
namespace log {

namespace {

constexpr std::size_t kLevelWidth = 5;

std::atomic<Level>& CurrentLevel() {
    static std::atomic<Level> level{Level::DEBUG};
    return level;
}

// Short level tags keep the line narrow: WARN instead of WARNING.
const char* LevelName(Level level) {
    switch (level) {
        case Level::TRACE:
            return "TRACE";
        case Level::DEBUG:
            return "DEBUG";
        case Level::INFO:
            return "INFO";
        case Level::WARN:
            return "WARN";
        case Level::ERR:
            return "ERROR";
        case Level::CRITICAL:
            return "CRITICAL";
        case Level::OFF:
            return "OFF";
    }
    return "";
}



// loguru's default palette: cyan / blue / bold / yellow / red / light red.
const char* LevelColor(Level level) {
    switch (level) {
        case Level::TRACE:
            return "\033[36m\033[1m";
        case Level::DEBUG:
            return "\033[34m\033[1m";
        case Level::INFO:
            return "\033[1m";  // loguru prints INFO bold, without a hue
        case Level::WARN:
            return "\033[33m\033[1m";
        case Level::ERR:
            return "\033[31m\033[1m";
        case Level::CRITICAL:
            return "\033[91m\033[1m";
        case Level::OFF:
            break;
    }
    return "";
}

const char* kReset = "\033[0m";
const char* kTimeColor = "\033[32m";      // green
const char* kLocationColor = "\033[36m";  // cyan
const char* kThreadColor = "\033[90m";    // gray

// Reading an environment variable without tripping MSVC's getenv deprecation.
std::string EnvValue(const char* name) {
#if defined(_WIN32)
    char* value = nullptr;
    std::size_t length = 0;
    if (_dupenv_s(&value, &length, name) != 0 || value == nullptr) {
        return std::string();
    }
    const std::string result(value);
    std::free(value);
    return result;
#else
    const char* value = std::getenv(name);
    return value != nullptr ? std::string(value) : std::string();
#endif
}

bool ForceColorRequested() {
    const std::string value = EnvValue("FORCE_COLOR");
    return !value.empty() && value != "0";
}

bool NoColorRequested() { return !EnvValue("NO_COLOR").empty(); }

// Colors stay off unless stdout is a terminal, so pipes and redirected files
// receive plain text. FORCE_COLOR wins, NO_COLOR disables.
bool StdoutSupportsColor() {
    if (ForceColorRequested()) {
        return true;
    }
    if (NoColorRequested()) {
        return false;
    }
#if defined(_WIN32)
    const HANDLE handle = ::GetStdHandle(STD_OUTPUT_HANDLE);
    if (handle == nullptr || handle == INVALID_HANDLE_VALUE) {
        return false;
    }
    DWORD mode = 0;
    if (::GetConsoleMode(handle, &mode) == 0) {
        return false;  // stdout is redirected, not a console
    }
    if ((mode & ENABLE_VIRTUAL_TERMINAL_PROCESSING) == 0) {
        // Enable ANSI processing; older consoles reject it and stay plain text.
        if (::SetConsoleMode(handle, mode | ENABLE_VIRTUAL_TERMINAL_PROCESSING) == 0) {
            return false;
        }
    }
    return true;
#else
    return ::isatty(STDOUT_FILENO) != 0;
#endif
}

// "YYYY-MM-DD HH:mm:ss.mmm +HH:MM", matching loguru's default time format. The
// default line drops the offset, custom sinks still get it in the record.
// "YYYY-MM-DD HH:mm:ss" is cached per thread per second: localtime() and the
// UTC offset lookup dominate the per-record cost and only change once a second.
struct TimestampCache {
    std::time_t second = 0;
    long offsetMinutes = 0;
    std::string dateTime;
};

std::string FormatTimestamp() {
    const std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    const std::time_t seconds = std::chrono::system_clock::to_time_t(now);
    const long long millis = static_cast<long long>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() % 1000);

    thread_local TimestampCache cache;
    if (cache.dateTime.empty() || cache.second != seconds) {
        std::tm localTime{};
#ifdef _WIN32
        localtime_s(&localTime, &seconds);
#else
        localtime_r(&seconds, &localTime);
#endif

        // UTC offset of that instant, including DST. mktime() reads the broken
        // down time as local time, the *mkgmtime family reads it as UTC.
        std::tm localForOffset = localTime;
        std::tm utcForOffset = localTime;
        const std::time_t asLocal = std::mktime(&localForOffset);
#ifdef _WIN32
        const std::time_t asUtc = _mkgmtime(&utcForOffset);
#else
        const std::time_t asUtc = timegm(&utcForOffset);
#endif
        long offsetMinutes = 0;
        if (asLocal != static_cast<std::time_t>(-1) && asUtc != static_cast<std::time_t>(-1)) {
            offsetMinutes = static_cast<long>((asLocal - asUtc) / 60);
        }

        char buffer[32];
        std::snprintf(buffer, sizeof(buffer), "%04d-%02d-%02d %02d:%02d:%02d", localTime.tm_year + 1900,
                      localTime.tm_mon + 1, localTime.tm_mday, localTime.tm_hour, localTime.tm_min, localTime.tm_sec);
        cache.dateTime.assign(buffer);
        cache.offsetMinutes = offsetMinutes;
        cache.second = seconds;
    }

    char suffix[16];
    std::snprintf(suffix, sizeof(suffix), ".%03lld %+03ld:%02ld", millis, cache.offsetMinutes / 60,
                  std::labs(cache.offsetMinutes % 60));
    std::string timestamp;
    timestamp.reserve(cache.dateTime.size() + 16);
    timestamp.assign(cache.dateTime);
    timestamp += suffix;
    return timestamp;
}

char PathSeparator() {
#ifdef _WIN32
    return '\\';
#else
    return '/';
#endif
}

// Native thread id: short numbers on Windows and Linux, which keeps the thread
// column readable (std::hash<thread::id> values are unsightly long).
std::uint64_t NativeThreadId() {
#if defined(_WIN32)
    return static_cast<std::uint64_t>(::GetCurrentThreadId());
#elif defined(__linux__)
    return static_cast<std::uint64_t>(::syscall(SYS_gettid));
#else
    return static_cast<std::uint64_t>(std::hash<std::thread::id>()(std::this_thread::get_id()));
#endif
}

// "YYYY-MM-DD HH:mm:ss.mmm +HH:MM" -> "YYYY-MM-DD HH:mm:ss.mmm", appended
// straight into the output buffer.
void AppendDateTime(std::string& out, const LogRecord& record) {
    const std::size_t length = record.timestamp.size() >= 23 ? 23 : record.timestamp.size();
    out.append(record.timestamp.data(), length);
}

void AppendThread(std::string& out, const LogRecord& record) {
    char buffer[32];
    const int length =
        std::snprintf(buffer, sizeof(buffer), "T%llu", static_cast<unsigned long long>(record.threadId));
    if (length > 0) {
        out.append(buffer, static_cast<std::size_t>(length));
    }
}

void AppendLocation(std::string& out, const LogRecord& record) {
    out.append(record.file != nullptr ? record.file : "");
    char buffer[24];
    const int length = std::snprintf(buffer, sizeof(buffer), ":%d", record.line);
    if (length > 0) {
        out.append(buffer, static_cast<std::size_t>(length));
    }
}

void AppendLevel(std::string& out, Level level) {
    char buffer[16];
    const int length =
        std::snprintf(buffer, sizeof(buffer), "%-*s", static_cast<int>(kLevelWidth), LevelName(level));
    if (length > 0) {
        out.append(buffer, static_cast<std::size_t>(length));
    }
}

// Plain layout (console without a terminal, and the log file):
//   2026-11-14 22:13:01.084 | INFO  | T19088 | main.cpp:47 - message
void AppendPlainLine(std::string& line, const LogRecord& record) {
    AppendDateTime(line, record);
    line += " | ";
    AppendLevel(line, record.level);
    line += " | ";
    AppendThread(line, record);
    line += " | ";
    AppendLocation(line, record);
    line += " - ";
    line += record.message;
}

// Same layout with loguru's colors: green time, level colored tag and message,
// gray thread id, cyan location.
void AppendColoredLine(std::string& line, const LogRecord& record) {
    const char* levelColor = LevelColor(record.level);
    line += kTimeColor;
    AppendDateTime(line, record);
    line += kReset;
    line += " | ";
    line += levelColor;
    AppendLevel(line, record.level);
    line += kReset;
    line += " | ";
    line += kThreadColor;
    AppendThread(line, record);
    line += kReset;
    line += " | ";
    line += kLocationColor;
    AppendLocation(line, record);
    line += kReset;
    line += " - ";
    line += levelColor;
    line += record.message;
    line += kReset;
}

std::mutex& ParamMutex() {
    static std::mutex mutex;
    return mutex;
}

LoggerParam& DefaultParam() {
    static LoggerParam param;
    return param;
}

std::mutex& SinkMutex() {
    static std::mutex mutex;
    return mutex;
}

// File backend stays on spdlog (daily rotation, retention); the payload is
// already a complete plain-text line, so the pattern is just the message.
std::shared_ptr<spdlog::logger> MakeFileLogger(const LoggerParam& param) {
    const std::string name = param.logName.empty() ? std::string("nexusflow") : param.logName;
    std::string path = param.logDir + PathSeparator() + name;
    // daily_file_sink appends "_YYYY-MM-DD" before the extension; give the file an
    // explicit .log extension when the caller did not provide one.
    if (name.find('.') == std::string::npos) {
        path += ".log";
    }
    auto sink = std::make_shared<spdlog::sinks::daily_file_sink_mt>(path, 0, 0, false, param.maxRetentionDays);
    auto fileLogger = std::make_shared<spdlog::logger>(name, std::move(sink));
    fileLogger->set_pattern("%v");
    fileLogger->set_level(spdlog::level::trace);
    // Errors reach the file immediately; routine lines are flushed at most once
    // per second in DefaultSink::Write, which keeps the hot path out of a flush
    // syscall per record.
    fileLogger->flush_on(spdlog::level::err);
    return fileLogger;
}

// A bad log directory must not take the process down, so a failed file backend
// degrades to console-only output.
std::shared_ptr<spdlog::logger> SafeMakeFileLogger(const LoggerParam& param) {
    try {
        return MakeFileLogger(param);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "nexusflow: cannot open the log file in '%s': %s\n", param.logDir.c_str(), e.what());
        return nullptr;
    }
}

// Built-in sink: loguru-like console output plus an optional daily rot file.
class DefaultSink final : public LogSink {
public:
    explicit DefaultSink(const LoggerParam& param)
        : m_consoleColors(StdoutSupportsColor()),
          m_fileLogger(param.logDir.empty() ? nullptr : SafeMakeFileLogger(param)) {
    }

    void Write(const LogRecord& record) override {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::string text;
        text.reserve(192 + record.message.size());
        if (m_consoleColors) {
            AppendColoredLine(text, record);
        } else {
            AppendPlainLine(text, record);
        }
        text += '\n';
        std::fwrite(text.data(), 1, text.size(), stdout);
        // A terminal wants the line immediately; redirected output is batched by
        // stdio, so only errors jump the queue there.
        if (m_consoleColors || record.level >= Level::ERR) {
            std::fflush(stdout);
        }
        if (m_fileLogger) {
            std::string plain;
            plain.reserve(192 + record.message.size());
            AppendPlainLine(plain, record);
            m_fileLogger->trace("{}", plain);
            const auto now = std::chrono::steady_clock::now();
            if (record.level >= Level::ERR || now - m_lastFileFlush >= std::chrono::seconds(1)) {
                m_fileLogger->flush();
                m_lastFileFlush = now;
            }
        }
    }

private:
    std::mutex m_mutex;
    bool m_consoleColors;
    std::shared_ptr<spdlog::logger> m_fileLogger;
    std::chrono::steady_clock::time_point m_lastFileFlush = std::chrono::steady_clock::now();
};

std::shared_ptr<LogSink> MakeDefaultSink() {
    LoggerParam param;
    {
        std::lock_guard<std::mutex> lock(ParamMutex());
        param = DefaultParam();
    }
    return std::make_shared<DefaultSink>(param);
}

std::shared_ptr<LogSink>& CurrentSink() {
    static std::shared_ptr<LogSink> sink = MakeDefaultSink();
    return sink;
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

void InitializeGlobalLogger(LoggerParam param) {
    {
        std::lock_guard<std::mutex> lock(ParamMutex());
        DefaultParam() = param;
    }
    SetLogLevel(param.logLevel);
    // Rebuild the built-in sink with the new configuration (console + optional file).
    SetSink(nullptr);
}

void SetSink(std::shared_ptr<LogSink> sink) {
    std::lock_guard<std::mutex> lock(SinkMutex());
    if (sink) {
        CurrentSink() = std::move(sink);
    } else {
        CurrentSink() = MakeDefaultSink();
    }
}

std::shared_ptr<LogSink> GetSink() {
    std::lock_guard<std::mutex> lock(SinkMutex());
    return CurrentSink();
}

namespace detail {

void Write(Level level, const char* file, int line, const char* function, std::string message) {
    if (!IsLevelEnabled(level)) {
        return;
    }

    LogRecord record;
    record.level = level;
    record.timestamp = FormatTimestamp();
    record.threadId = NativeThreadId();
    record.file = file != nullptr ? FileName(file) : "";
    record.line = line;
    record.function = function != nullptr ? function : "";
    record.message = std::move(message);

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
