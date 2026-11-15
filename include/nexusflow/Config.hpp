#ifndef NEXUSFLOW_CONFIG_HPP
#define NEXUSFLOW_CONFIG_HPP

#include <nexusflow/Logging.hpp>
#include <nexusflow/common/Any.hpp>
#include <string>
#include <unordered_map>
namespace nexusflow {

class Config {
public:
    void Reset(std::unordered_map<std::string, Any> cfgMap) { m_cfgMap = std::move(cfgMap); }

    template <typename T>
    void Add(const std::string& key, const T& value) {
        m_cfgMap[key] = value;
    }

    /**
     * @brief Reads a value, or @p defaultValue when the key is absent.
     *
     * A key that is present with a different type also yields @p defaultValue,
     * but it is reported as a warning: silently ignoring the mismatch turned
     * typos such as `maxBatchSize: "4"` into unexplained default behaviour.
     */
    template <typename T>
    T GetValueOrDefault(const std::string& key, const T& defaultValue) const {
        auto it = m_cfgMap.find(key);
        if (it == m_cfgMap.end()) {
            return defaultValue;
        }

        auto* res = it->second.get<T>();
        if (res != nullptr) {
            return *res;
        }

        LOG_WARN("Config key '{}' has a different type than requested; using the default value.", key);
        return defaultValue;
    }

    const std::unordered_map<std::string, Any>& GetConfigMap() const { return m_cfgMap; }

private:
    std::unordered_map<std::string, Any> m_cfgMap;
};

} // namespace nexusflow

#endif