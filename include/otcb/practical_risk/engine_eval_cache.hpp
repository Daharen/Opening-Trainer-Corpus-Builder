#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <unordered_map>

struct sqlite3;

namespace otcb {

class EngineEvalCache {
public:
    explicit EngineEvalCache(const std::filesystem::path& sqlite_path);
    ~EngineEvalCache();

    EngineEvalCache(const EngineEvalCache&) = delete;
    EngineEvalCache& operator=(const EngineEvalCache&) = delete;

    std::optional<double> get(const std::string& key);
    void put(const std::string& key,
             const std::string& position_key,
             const std::string& move_uci,
             const std::string& engine_id,
             int movetime_ms,
             const std::string& policy_hash,
             double score_cp);

private:
    sqlite3* db_ = nullptr;
    std::unordered_map<std::string, double> memory_;
};

}  // namespace otcb
