#pragma once

// Public logging facade.
//
// This header owns the logging API: level, parameters, record layout and the
// sink hook. It deliberately does not include any third-party logging header:
// the default backend (spdlog) lives in src/common/Logging.cpp and can be
// replaced at runtime through log::SetSink().
//
// For debug-only logs (DLOG), compile with -DNDEBUG for release builds.

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <type_traits>

// Compatibility macro kept for code that used it to strip paths from __FILE__.
// FILENAME_ below handles both separators directly, so this is only a leftover.
#ifndef OS_SEP
#ifdef _MSC_VER
#define OS_SEP '\\'
#else
#define OS_SEP '/'
#endif
#endif

namespace nexusflow {
namespace log {

// Severity, ordered from least to most severe. OFF disables logging entirely.
enum class Level : std::int8_t { TRACE = 0, DEBUG, INFO, WARN, ERR, CRITICAL, OFF };

// Parameters of the default logger. Field names are kept stable because they are
// part of the public API used by the applications.
struct LoggerParam {
    Level logLevel = Level::DEBUG;  // default log level
    std::string logDir;             // if empty, no file logger will be created
    std::string logName;            // if empty, a default name will be used
    std::size_t maxRetentionDays = 0;  // if 0, no retention limit
};

// One formatted log line, handed to a LogSink.
struct LogRecord {
    Level level = Level::INFO;
    std::string timestamp;  // local time with offset, "YYYY-MM-DD HH:MM:SS.mmm +HH:MM"
    std::uint64_t threadId = 0;  // native thread id (TID on Windows/Linux)
    const char* file = "";  // basename only
    int line = 0;
    const char* function = "";
    std::string message;  // already formatted
};

// A sink receives every enabled log record. Implement it to forward records to
// a custom backend (collector, test buffer, rotating file writer, ...).
class LogSink {
public:
    virtual ~LogSink() = default;
    virtual void Write(const LogRecord& record) = 0;
};

// Level control. The default level is DEBUG.
void SetLogLevel(Level level);
Level GetLogLevel();
bool IsLevelEnabled(Level level);

// Installs the built-in logger: the console plus an optional daily rotating
// file, and replaces the current sink with it.
//
// Default layout (console colored by level, file plain text; one file per day:
// <logName>_YYYY-MM-DD.log):
//   2026-11-14 22:13:01.084 | INFO  | T19088 | main.cpp:47 - message
// The console uses loguru's palette (green time, level-colored tag and message,
// gray thread id, cyan location) when stdout is a terminal; NO_COLOR disables it
// and FORCE_COLOR=1 forces it on. Implement a LogSink to take over formatting.
void InitializeGlobalLogger(LoggerParam param = LoggerParam());

// Replaces the sink. Passing nullptr restores the built-in default sink.
void SetSink(std::shared_ptr<LogSink> sink);
std::shared_ptr<LogSink> GetSink();

namespace detail {

// Strips directories from a __FILE__ path (handles '/' and '\\').
inline const char* FileName(const char* path) {
    if (path == nullptr) {
        return "";
    }
    const char* base = path;
    for (const char* cursor = path; *cursor != '\0'; ++cursor) {
        if (*cursor == '/' || *cursor == '\\') {
            base = cursor + 1;
        }
    }
    return base;
}

// Parses the precision of floating-point placeholders such as "{:.2f}".
inline bool ParseFixedSpec(const char* begin, const char* end, int& precision) {
    if (begin == end || *begin != '.') {
        return false;
    }
    const char* cursor = begin + 1;
    int value = 0;
    bool hasDigit = false;
    while (cursor != end && *cursor >= '0' && *cursor <= '9') {
        value = value * 10 + (*cursor - '0');
        ++cursor;
        hasDigit = true;
    }
    if (!hasDigit) {
        return false;
    }
    if (cursor != end && *cursor == 'f') {
        ++cursor;
    }
    if (cursor != end) {
        return false;
    }
    precision = value;
    return true;
}

// Appends one argument. The common cases avoid std::ostringstream: numbers go
// through snprintf into a stack buffer and strings are appended directly, so a
// log call stays allocation-light. Unusual types fall back to a stream.
inline void AppendValue(std::string& out, const std::string& value, const char*, const char*) {
    out += value;
}

inline void AppendValue(std::string& out, const char* value, const char*, const char*) {
    out += (value != nullptr) ? value : "(null)";
}

inline void AppendValue(std::string& out, char* value, const char* specBegin, const char* specEnd) {
    AppendValue(out, static_cast<const char*>(value), specBegin, specEnd);
}

template <std::size_t N>
inline void AppendValue(std::string& out, const char (&value)[N], const char* specBegin, const char* specEnd) {
    AppendValue(out, static_cast<const char*>(value), specBegin, specEnd);
}

template <std::size_t N>
inline void AppendValue(std::string& out, char (&value)[N], const char* specBegin, const char* specEnd) {
    AppendValue(out, static_cast<const char*>(value), specBegin, specEnd);
}

inline void AppendValue(std::string& out, bool value, const char*, const char*) {
    out += value ? "true" : "false";
}

inline void AppendValue(std::string& out, char value, const char*, const char*) {
    out += value;
}

template <typename T>
inline typename std::enable_if<std::is_integral<T>::value && !std::is_same<T, bool>::value &&
                                   !std::is_same<T, char>::value,
                               void>::type
AppendValue(std::string& out, T value, const char*, const char*) {
    char buffer[32];
    int written = 0;
    if (std::is_signed<T>::value) {
        written = std::snprintf(buffer, sizeof(buffer), "%lld", static_cast<long long>(value));
    } else {
        written = std::snprintf(buffer, sizeof(buffer), "%llu", static_cast<unsigned long long>(value));
    }
    if (written > 0) {
        out.append(buffer, static_cast<std::size_t>(written));
    }
}

template <typename T>
inline typename std::enable_if<std::is_floating_point<T>::value, void>::type
AppendValue(std::string& out, T value, const char* specBegin, const char* specEnd) {
    char buffer[64];
    int precision = 0;
    int written = 0;
    if (ParseFixedSpec(specBegin, specEnd, precision)) {
        written = std::snprintf(buffer, sizeof(buffer), "%.*f", precision, static_cast<double>(value));
    } else {
        written = std::snprintf(buffer, sizeof(buffer), "%.6g", static_cast<double>(value));
    }
    if (written > 0) {
        out.append(buffer, static_cast<std::size_t>(written));
    }
}

template <typename T>
inline typename std::enable_if<std::is_enum<T>::value, void>::type
AppendValue(std::string& out, T value, const char* specBegin, const char* specEnd) {
    AppendValue(out, static_cast<typename std::underlying_type<T>::type>(value), specBegin, specEnd);
}

// Fallback for user-defined types.
template <typename T>
inline typename std::enable_if<!std::is_integral<T>::value && !std::is_floating_point<T>::value &&
                                   !std::is_enum<T>::value,
                               void>::type
AppendValue(std::string& out, const T& value, const char* specBegin, const char* specEnd) {
    std::ostringstream stream;
    int precision = 0;
    if (ParseFixedSpec(specBegin, specEnd, precision)) {
        stream << std::fixed << std::setprecision(precision);
    }
    stream << value;
    out += stream.str();
}

// Terminal case: append what is left of the format string.
inline void FormatInto(std::string& out, const char* format) {
    if (format != nullptr) {
        out.append(format, std::strlen(format));
    }
}

// Consumes one argument per "{}" / "{:spec}" placeholder and appends literals.
template <typename First, typename... Rest>
void FormatInto(std::string& out, const char* format, const First& first, const Rest&... rest) {
    const char* cursor = format;
    while (cursor != nullptr && *cursor != '\0') {
        if (*cursor == '{') {
            if (cursor[1] == '{') {
                out += '{';
                cursor += 2;
                continue;
            }
            const char* close = cursor + 1;
            while (*close != '\0' && *close != '}') {
                ++close;
            }
            const char* specBegin = cursor + 1;
            const char* specEnd = close;
            if (specBegin != specEnd && *specBegin == ':') {
                ++specBegin;
            }
            AppendValue(out, first, specBegin, specEnd);
            cursor = (*close == '}') ? close + 1 : close;
            FormatInto(out, cursor, rest...);
            return;
        }
        if (*cursor == '}' && cursor[1] == '}') {
            out += '}';
            cursor += 2;
            continue;
        }
        out += *cursor;
        ++cursor;
    }
    // More arguments than placeholders: keep the extra values visible.
    out += ' ';
    AppendValue(out, first, nullptr, nullptr);
    FormatInto(out, cursor, rest...);
}

// Minimal replacement for fmt: "{}" and "{:.Nf}" are the only supported
// placeholders (that is all the framework uses). Braces can be escaped with
// "{{" / "}}". Values are written into one buffer instead of a stream per
// placeholder, which is what keeps LOG_* cheap enough for hot loops.
template <typename... Args>
std::string Format(const char* format, const Args&... args) {
    std::string out;
    if (format == nullptr) {
        return out;
    }
    out.reserve(std::strlen(format) + 16 * sizeof...(Args));
    FormatInto(out, format, args...);
    return out;
}

// Call sites also pass a runtime-built std::string as the format string (e.g.
// LOG_INFO(GetModuleName() + ": ...", value)). Accept it explicitly so the
// facade keeps the same call syntax as the previous spdlog-based macros.
template <typename... Args>
std::string Format(const std::string& format, const Args&... args) {
    return Format(format.c_str(), args...);
}

// Implemented in src/common/Logging.cpp. 'message' is taken by value so
// the temporary produced by Format() can be moved into the record.
void Write(Level level, const char* file, int line, const char* function, std::string message);

// Helper for time-based logging state.
struct LogEveryTState {
    std::mutex mutex;
    std::chrono::steady_clock::time_point lastLog{};
};

inline bool ShouldLogEveryT(LogEveryTState& state, double seconds) {
    const std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(state.mutex);
    const std::chrono::steady_clock::duration interval =
        std::chrono::duration_cast<std::chrono::steady_clock::duration>(std::chrono::duration<double>(seconds));
    if (now - state.lastLog > interval) {
        state.lastLog = now;
        return true;
    }
    return false;
}

// A NullStream does nothing; used to disable logging statements at compile time.
class NullStream {
public:
    template <typename T>
    NullStream& operator<<(const T&) {
        return *this;
    }
    NullStream& stream() { return *this; }
};

// Stream-style logging: buffers with operator<< and emits on destruction.
class StreamLogger {
public:
    StreamLogger(Level level, const char* file, int line, const char* function)
        : m_level(level),
          m_file(file),
          m_line(line),
          m_function(function),
          m_enabled(IsLevelEnabled(level)) {}

    virtual ~StreamLogger() {
        if (m_writeOnDestroy) {
            WriteBuffered();
        }
    }

    template <typename T>
    StreamLogger& operator<<(const T& value) {
        if (m_enabled) {
            m_stream << value;
        }
        return *this;
    }

    std::stringstream& stream() { return m_stream; }

protected:
    void WriteBuffered() {
        if (m_enabled) {
            Write(m_level, m_file, m_line, m_function, m_stream.str());
            m_enabled = false;
        }
    }

    std::stringstream m_stream;
    Level m_level;
    const char* m_file;
    int m_line;
    const char* m_function;
    bool m_enabled;
    bool m_writeOnDestroy = true;
};

// Stream-style fatal check: writes the buffer and terminates the process.
class FatalStream : public StreamLogger {
public:
    using StreamLogger::StreamLogger;

    ~FatalStream() override {
        WriteBuffered();
        std::abort();
    }
};

}  // namespace detail
}  // namespace log
}  // namespace nexusflow

// Basename of the current source file, without directories.
#define FILENAME_ (::nexusflow::log::detail::FileName(__FILE__))

//--------------------------------------------------------------------------------//
// Core logging macros
//--------------------------------------------------------------------------------//

#define NEXUSFLOW_LOG_AT(level, ...)                                                        \
    do {                                                                                    \
        if (::nexusflow::log::IsLevelEnabled(level)) {                                      \
            ::nexusflow::log::detail::Write(level, FILENAME_, __LINE__, __FUNCTION__,       \
                                            ::nexusflow::log::detail::Format(__VA_ARGS__)); \
        }                                                                                   \
    } while (0)

// Kept for compatibility with call sites that spell the helper name directly.
#define HELPER_LOG_TRACE(...) NEXUSFLOW_LOG_AT(::nexusflow::log::Level::TRACE, __VA_ARGS__)
#define HELPER_LOG_DEBUG(...) NEXUSFLOW_LOG_AT(::nexusflow::log::Level::DEBUG, __VA_ARGS__)
#define HELPER_LOG_INFO(...) NEXUSFLOW_LOG_AT(::nexusflow::log::Level::INFO, __VA_ARGS__)
#define HELPER_LOG_WARN(...) NEXUSFLOW_LOG_AT(::nexusflow::log::Level::WARN, __VA_ARGS__)
#define HELPER_LOG_ERROR(...) NEXUSFLOW_LOG_AT(::nexusflow::log::Level::ERR, __VA_ARGS__)
#define HELPER_LOG_CRITICAL(...) NEXUSFLOW_LOG_AT(::nexusflow::log::Level::CRITICAL, __VA_ARGS__)

// Helper for CHECK_XX comparison macros.
#define HELPER_CHECK_OP_FMT(val1, val2, op_str, op)                                           \
    do {                                                                                      \
        auto v1 = (val1);                                                                     \
        auto v2 = (val2);                                                                     \
        if (!(v1 op v2)) {                                                                    \
            LOG(CRITICAL, "Check failed: {} {} {} ({} vs {})", #val1, op_str, #val2, v1, v2); \
            std::abort();                                                                     \
        }                                                                                     \
    } while (0)

// --- 1. FMT-style logging ---

#define LOG(level, ...) HELPER_LOG_##level(__VA_ARGS__)
#define LOG_IF(level, condition, ...) \
    if (condition) {                  \
        LOG(level, __VA_ARGS__);      \
    }
#define LOG_EVERY_N(level, n, ...)                                                   \
    do {                                                                             \
        static std::atomic<size_t> counter(0);                                       \
        if ((n) > 0 && counter.fetch_add(1, std::memory_order_relaxed) % (n) == 0) { \
            LOG(level, __VA_ARGS__);                                                 \
        }                                                                            \
    } while (0)
#define LOG_FIRST_N(level, n, ...)                                                      \
    do {                                                                                \
        static std::atomic<size_t> counter(0);                                          \
        if (counter.fetch_add(1, std::memory_order_relaxed) < static_cast<size_t>(n)) { \
            LOG(level, __VA_ARGS__);                                                    \
        }                                                                               \
    } while (0)
#define LOG_EVERY_T(level, seconds, ...)                                    \
    do {                                                                    \
        static ::nexusflow::log::detail::LogEveryTState state;              \
        if (::nexusflow::log::detail::ShouldLogEveryT(state, seconds)) {    \
            LOG(level, __VA_ARGS__);                                        \
        }                                                                   \
    } while (0)
#define LOG_IF_EVERY_N(level, condition, n, ...) \
    if (condition) {                             \
        LOG_EVERY_N(level, n, __VA_ARGS__);      \
    }

// --- 2. Stream-style logging ---

#define LOG_STREAM(level) \
    ::nexusflow::log::detail::StreamLogger(::nexusflow::log::Level::level, FILENAME_, __LINE__, __FUNCTION__)
#define LOG_STREAM_IF(level, condition) \
    if (condition) LOG_STREAM(level) else ::nexusflow::log::detail::NullStream()
#define LOG_STREAM_EVERY_N(level, n)                                         \
    (((n) > 0 &&                                                             \
      [] {                                                                   \
          static std::atomic<size_t> counter(0);                             \
          return counter.fetch_add(1, std::memory_order_relaxed) % (n) == 0; \
      }())                                                                   \
         ? LOG_STREAM(level)                                                 \
         : ::nexusflow::log::detail::NullStream())

// --- 3. Simplified level-only macros ---

#define LOG_TRACE(...) HELPER_LOG_TRACE(__VA_ARGS__)
#define LOG_DEBUG(...) HELPER_LOG_DEBUG(__VA_ARGS__)
#define LOG_INFO(...) HELPER_LOG_INFO(__VA_ARGS__)
#define LOG_WARN(...) HELPER_LOG_WARN(__VA_ARGS__)
#define LOG_ERROR(...) HELPER_LOG_ERROR(__VA_ARGS__)
#define LOG_CRITICAL(...) HELPER_LOG_CRITICAL(__VA_ARGS__)

// --- 4. Debug-only logging (DLOG - compiled out if NDEBUG is defined) ---

#ifdef NDEBUG
#define DLOG(level, ...) (void)0
#define DLOG_IF(level, ...) (void)0
#define DLOG_STREAM(level) ::nexusflow::log::detail::NullStream()
#define DLOG_STREAM_IF(level, condition) ::nexusflow::log::detail::NullStream()
#else
#define DLOG(level, ...) LOG(level, __VA_ARGS__)
#define DLOG_IF(level, condition, ...) LOG_IF(level, condition, __VA_ARGS__)
#define DLOG_STREAM(level) LOG_STREAM(level)
#define DLOG_STREAM_IF(level, condition) LOG_STREAM_IF(level, condition)
#endif

// --- 5. Fatal assertion checks (CHECK - ALWAYS ON) ---

// `fmt` style check
#define CHECK(condition, ...)                                            \
    do {                                                                 \
        if (!(condition)) {                                              \
            LOG(CRITICAL, "Check failed: " #condition ". " __VA_ARGS__); \
            std::abort();                                                \
        }                                                                \
    } while (0)
#define CHECK_EQ(v1, v2) HELPER_CHECK_OP_FMT(v1, v2, "==", ==)
#define CHECK_NE(v1, v2) HELPER_CHECK_OP_FMT(v1, v2, "!=", !=)
#define CHECK_LT(v1, v2) HELPER_CHECK_OP_FMT(v1, v2, "<", <)
#define CHECK_LE(v1, v2) HELPER_CHECK_OP_FMT(v1, v2, "<=", <=)
#define CHECK_GT(v1, v2) HELPER_CHECK_OP_FMT(v1, v2, ">", >)
#define CHECK_GE(v1, v2) HELPER_CHECK_OP_FMT(v1, v2, ">=", >=)

// Stream style check
#define CHECK_STREAM(condition)                                                                     \
    if (condition)                                                                                  \
        (void)0;                                                                                    \
    else                                                                                            \
        ::nexusflow::log::detail::FatalStream(::nexusflow::log::Level::CRITICAL, FILENAME_, __LINE__, \
                                              __FUNCTION__)                                         \
                .stream()                                                                           \
            << "Check failed: " #condition ". "

//--------------------------------------------------------------------------------//
// Backwards-compatible spelling: nexusflow::logger
//--------------------------------------------------------------------------------//

namespace nexusflow {
namespace logger {

using LogLevel = ::nexusflow::log::Level;
using LoggerParam = ::nexusflow::log::LoggerParam;

using ::nexusflow::log::GetLogLevel;
using ::nexusflow::log::InitializeGlobalLogger;
using ::nexusflow::log::IsLevelEnabled;
using ::nexusflow::log::SetLogLevel;

}  // namespace logger
}  // namespace nexusflow
