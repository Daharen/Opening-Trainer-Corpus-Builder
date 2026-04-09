#include "otcb/practical_risk/engine_eval_cache.hpp"

#include <cmath>
#include <stdexcept>

#include <sqlite3.h>

namespace otcb {

EngineEvalCache::EngineEvalCache(const std::filesystem::path& sqlite_path) {
    if (sqlite3_open(sqlite_path.string().c_str(), &db_) != SQLITE_OK) {
        throw std::runtime_error("unable to open engine eval cache sqlite");
    }
    const char* ddl =
        "CREATE TABLE IF NOT EXISTS eval_cache ("
        "cache_key TEXT PRIMARY KEY,"
        "position_key TEXT NOT NULL,"
        "move_uci TEXT NOT NULL,"
        "engine_id TEXT NOT NULL,"
        "movetime_ms INTEGER NOT NULL,"
        "policy_hash TEXT NOT NULL,"
        "score_cp INTEGER NOT NULL"
        ");";
    char* err = nullptr;
    if (sqlite3_exec(db_, ddl, nullptr, nullptr, &err) != SQLITE_OK) {
        const std::string msg = err ? err : "sqlite error";
        sqlite3_free(err);
        throw std::runtime_error("sqlite create table failed: " + msg);
    }
}

EngineEvalCache::~EngineEvalCache() {
    if (db_) sqlite3_close(db_);
}

std::optional<double> EngineEvalCache::get(const std::string& key) {
    auto it = memory_.find(key);
    if (it != memory_.end()) return it->second;
    const char* sql = "SELECT score_cp FROM eval_cache WHERE cache_key=?1";
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) != SQLITE_OK) return std::nullopt;
    sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_TRANSIENT);
    std::optional<double> out;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        out = static_cast<double>(sqlite3_column_int(stmt, 0));
        memory_.emplace(key, *out);
    }
    sqlite3_finalize(stmt);
    return out;
}

void EngineEvalCache::put(const std::string& key,
                          const std::string& position_key,
                          const std::string& move_uci,
                          const std::string& engine_id,
                          int movetime_ms,
                          const std::string& policy_hash,
                          double score_cp) {
    memory_[key] = score_cp;
    const char* sql =
        "INSERT OR REPLACE INTO eval_cache(cache_key, position_key, move_uci, engine_id, movetime_ms, policy_hash, score_cp)"
        "VALUES(?1,?2,?3,?4,?5,?6,?7)";
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) != SQLITE_OK) return;
    sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, position_key.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, move_uci.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 4, engine_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 5, movetime_ms);
    sqlite3_bind_text(stmt, 6, policy_hash.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 7, static_cast<int>(std::lround(score_cp)));
    (void)sqlite3_step(stmt);
    sqlite3_finalize(stmt);
}

}  // namespace otcb
