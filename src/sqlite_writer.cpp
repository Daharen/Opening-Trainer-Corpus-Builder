#include "otcb/sqlite_writer.hpp"

#include <sqlite3.h>

#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace otcb {
namespace {

class SqliteDb {
   public:
    explicit SqliteDb(const std::filesystem::path& path) {
        if (sqlite3_open(path.string().c_str(), &db_) != SQLITE_OK) {
            const std::string error = db_ ? sqlite3_errmsg(db_) : "unknown sqlite open error";
            if (db_) {
                sqlite3_close(db_);
                db_ = nullptr;
            }
            throw std::runtime_error("Failed to open sqlite database: " + error);
        }
    }

    ~SqliteDb() {
        if (db_) {
            sqlite3_close(db_);
        }
    }

    sqlite3* get() const { return db_; }

   private:
    sqlite3* db_ = nullptr;
};

class Statement {
   public:
    Statement(sqlite3* db, const char* sql) {
        if (sqlite3_prepare_v2(db, sql, -1, &stmt_, nullptr) != SQLITE_OK) {
            throw std::runtime_error("Failed to prepare sqlite statement");
        }
    }

    ~Statement() {
        if (stmt_) {
            sqlite3_finalize(stmt_);
        }
    }

    sqlite3_stmt* get() const { return stmt_; }

   private:
    sqlite3_stmt* stmt_ = nullptr;
};

void exec_sql(sqlite3* db, const char* sql) {
    char* error_message = nullptr;
    if (sqlite3_exec(db, sql, nullptr, nullptr, &error_message) != SQLITE_OK) {
        const std::string error = error_message ? error_message : "unknown sqlite exec error";
        sqlite3_free(error_message);
        throw std::runtime_error("SQLite exec failed: " + error);
    }
}

void bind_text(sqlite3_stmt* stmt, int index, const std::string& value) {
    if (sqlite3_bind_text(stmt, index, value.c_str(), -1, SQLITE_TRANSIENT) != SQLITE_OK) {
        throw std::runtime_error("Failed to bind sqlite text parameter");
    }
}

void bind_int(sqlite3_stmt* stmt, int index, int value) {
    if (sqlite3_bind_int(stmt, index, value) != SQLITE_OK) {
        throw std::runtime_error("Failed to bind sqlite int parameter");
    }
}

void reset_statement(sqlite3_stmt* stmt) {
    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);
}

void step_expect_done(sqlite3* db, sqlite3_stmt* stmt) {
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        throw std::runtime_error("SQLite step failed: " + std::string(sqlite3_errmsg(db)));
    }
}

}  // namespace

SqliteWriteStats write_aggregate_payload_sqlite(const std::filesystem::path& sqlite_path,
                                                const BuildConfig& config,
                                                const std::string& artifact_id,
                                                const AggregationSummary& summary,
                                                const std::vector<AggregatedPositionRecord>& positions) {
    if (std::filesystem::exists(sqlite_path)) {
        std::filesystem::remove(sqlite_path);
    }

    SqliteDb db(sqlite_path);
    exec_sql(db.get(), "PRAGMA foreign_keys = ON;");
    exec_sql(db.get(), "BEGIN TRANSACTION;");
    try {
        exec_sql(db.get(),
                 "CREATE TABLE artifact_metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);"
                 "CREATE TABLE positions("
                 "position_id INTEGER PRIMARY KEY,"
                 "position_key TEXT NOT NULL,"
                 "position_key_format TEXT NOT NULL,"
                 "side_to_move TEXT NOT NULL,"
                 "candidate_move_count INTEGER NOT NULL,"
                 "total_observations INTEGER NOT NULL"
                 ");"
                 "CREATE TABLE moves("
                 "position_id INTEGER NOT NULL,"
                 "move_key TEXT NOT NULL,"
                 "move_key_format TEXT NOT NULL,"
                 "raw_count INTEGER NOT NULL,"
                 "example_san TEXT,"
                 "PRIMARY KEY(position_id, move_key),"
                 "FOREIGN KEY(position_id) REFERENCES positions(position_id)"
                 ");"
                 "CREATE UNIQUE INDEX idx_positions_position_key_side_to_move ON positions(position_key, side_to_move);"
                 "CREATE INDEX idx_moves_position_id ON moves(position_id);");

        Statement metadata_stmt(db.get(), "INSERT INTO artifact_metadata(key, value) VALUES (?, ?);");
        Statement position_stmt(db.get(), "INSERT INTO positions(position_key, position_key_format, side_to_move, candidate_move_count, total_observations) VALUES (?, ?, ?, ?, ?);");
        Statement move_stmt(db.get(), "INSERT INTO moves(position_id, move_key, move_key_format, raw_count, example_san) VALUES (?, ?, ?, ?, ?);");

        const std::vector<std::pair<std::string, std::string>> metadata = {
            {"artifact_schema_version", "otcb_sqlite_aggregate_v1"},
            {"artifact_id", artifact_id},
            {"payload_format", to_string(config.payload_format)},
            {"source_path", summary.source_path},
            {"min_rating", std::to_string(config.min_rating)},
            {"max_rating", std::to_string(config.max_rating)},
            {"rating_policy", to_string(*config.rating_policy)},
            {"retained_ply", std::to_string(config.retained_ply)},
            {"position_key_format", to_string(*config.position_key_format)},
            {"move_key_format", to_string(*config.move_key_format)},
            {"min_position_count", std::to_string(config.min_position_count)},
            {"raw_counts_preserved", "true"},
            {"effective_weights_precomputed", "false"},
            {"total_accepted_games", std::to_string(summary.total_games_accepted_upstream)},
            {"total_emitted_positions", std::to_string(summary.total_unique_positions_emitted)},
            {"total_emitted_move_entries", std::to_string(summary.total_aggregate_move_entries_emitted)},
            {"total_raw_observations", std::to_string(summary.total_raw_observations_emitted)},
        };

        for (const auto& [key, value] : metadata) {
            bind_text(metadata_stmt.get(), 1, key);
            bind_text(metadata_stmt.get(), 2, value);
            step_expect_done(db.get(), metadata_stmt.get());
            reset_statement(metadata_stmt.get());
        }

        SqliteWriteStats stats;
        for (const auto& record : positions) {
            bind_text(position_stmt.get(), 1, record.position_key);
            bind_text(position_stmt.get(), 2, to_string(*config.position_key_format));
            bind_text(position_stmt.get(), 3, record.side_to_move);
            bind_int(position_stmt.get(), 4, record.candidate_move_count);
            bind_int(position_stmt.get(), 5, record.total_observations);
            step_expect_done(db.get(), position_stmt.get());
            reset_statement(position_stmt.get());

            const auto position_id = static_cast<int>(sqlite3_last_insert_rowid(db.get()));
            ++stats.positions_rows;
            stats.total_raw_observations += record.total_observations;

            for (const auto& move : record.candidate_moves) {
                bind_int(move_stmt.get(), 1, position_id);
                bind_text(move_stmt.get(), 2, move.move_key);
                bind_text(move_stmt.get(), 3, to_string(*config.move_key_format));
                bind_int(move_stmt.get(), 4, move.raw_count);
                if (move.example_san.empty()) {
                    if (sqlite3_bind_null(move_stmt.get(), 5) != SQLITE_OK) {
                        throw std::runtime_error("Failed to bind sqlite null parameter");
                    }
                } else {
                    bind_text(move_stmt.get(), 5, move.example_san);
                }
                step_expect_done(db.get(), move_stmt.get());
                reset_statement(move_stmt.get());
                ++stats.moves_rows;
            }
        }

        exec_sql(db.get(), "COMMIT;");
        return stats;
    } catch (...) {
        exec_sql(db.get(), "ROLLBACK;");
        throw;
    }
}

}  // namespace otcb
