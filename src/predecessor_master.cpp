#include "otcb/predecessor_master.hpp"

#include <sqlite3.h>

#include <chrono>
#include <ctime>
#include <fstream>
#include <optional>
#include <stdexcept>
#include <string>

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
        if (db_) sqlite3_close(db_);
    }

    sqlite3* get() const { return db_; }

   private:
    sqlite3* db_ = nullptr;
};

class Statement {
   public:
    Statement(sqlite3* db, const char* sql) {
        if (sqlite3_prepare_v2(db, sql, -1, &stmt_, nullptr) != SQLITE_OK) {
            throw std::runtime_error("Failed to prepare sqlite statement: " + std::string(sqlite3_errmsg(db)));
        }
    }
    ~Statement() {
        if (stmt_) sqlite3_finalize(stmt_);
    }
    sqlite3_stmt* get() const { return stmt_; }

   private:
    sqlite3_stmt* stmt_ = nullptr;
};

void exec_sql(sqlite3* db, const std::string& sql) {
    char* error_message = nullptr;
    if (sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &error_message) != SQLITE_OK) {
        const std::string error = error_message ? error_message : "unknown sqlite exec error";
        sqlite3_free(error_message);
        throw std::runtime_error("SQLite exec failed: " + error);
    }
}

void exec_sql_allow_error(sqlite3* db, const std::string& sql) {
    char* error_message = nullptr;
    sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &error_message);
    if (error_message) {
        sqlite3_free(error_message);
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

int select_int(sqlite3* db, const std::string& sql) {
    Statement stmt(db, sql.c_str());
    const int rc = sqlite3_step(stmt.get());
    if (rc != SQLITE_ROW) {
        throw std::runtime_error("Expected integer result row for SQL: " + sql);
    }
    return sqlite3_column_int(stmt.get(), 0);
}

std::string utc_now_iso8601() {
    const std::time_t t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::tm utc_tm{};
#if defined(_WIN32)
    gmtime_s(&utc_tm, &t);
#else
    gmtime_r(&t, &utc_tm);
#endif
    char buffer[32];
    if (std::strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%SZ", &utc_tm) == 0) {
        throw std::runtime_error("Failed to format UTC timestamp");
    }
    return buffer;
}

std::filesystem::path resolve_source_sqlite_path(const std::filesystem::path& source_path) {
    if (!std::filesystem::exists(source_path)) {
        throw std::runtime_error("Source predecessor path does not exist: " + source_path.string());
    }
    if (std::filesystem::is_regular_file(source_path)) {
        return source_path;
    }
    if (std::filesystem::is_directory(source_path)) {
        const auto candidate = source_path / "data" / "canonical_predecessor_edges.sqlite";
        if (std::filesystem::exists(candidate) && std::filesystem::is_regular_file(candidate)) {
            return candidate;
        }
        throw std::runtime_error("Source directory is missing data/canonical_predecessor_edges.sqlite: " + source_path.string());
    }
    throw std::runtime_error("Unsupported source predecessor path type: " + source_path.string());
}

bool safe_to_delete_source(const std::filesystem::path& original_source_path) {
    if (std::filesystem::is_regular_file(original_source_path)) {
        return original_source_path.filename() == "canonical_predecessor_edges.sqlite";
    }
    if (std::filesystem::is_directory(original_source_path)) {
        return std::filesystem::exists(original_source_path / "manifest.json") &&
               std::filesystem::exists(original_source_path / "data" / "canonical_predecessor_edges.sqlite");
    }
    return false;
}

void ensure_master_schema(sqlite3* master_db) {
    exec_sql(master_db,
             "PRAGMA foreign_keys = ON;"
             "CREATE TABLE IF NOT EXISTS master_metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);"
             "CREATE TABLE IF NOT EXISTS merge_sources("
             "merge_order INTEGER PRIMARY KEY,"
             "source_path TEXT NOT NULL,"
             "source_artifact_id TEXT,"
             "source_min_rating TEXT,"
             "source_max_rating TEXT,"
             "source_rating_policy TEXT,"
             "source_time_control_id TEXT,"
             "inserted_positions INTEGER NOT NULL,"
             "skipped_existing_positions INTEGER NOT NULL,"
             "merge_started_utc TEXT NOT NULL,"
             "merge_completed_utc TEXT NOT NULL,"
             "source_deleted_after_merge INTEGER NOT NULL"
             ");"
             "CREATE TABLE IF NOT EXISTS master_positions("
             "position_id INTEGER PRIMARY KEY,"
             "position_key TEXT NOT NULL UNIQUE,"
             "parent_position_key TEXT,"
             "incoming_move_uci TEXT,"
             "depth_from_root INTEGER NOT NULL,"
             "edge_support_count INTEGER NOT NULL,"
             "selection_policy_version TEXT NOT NULL,"
             "source_artifact_id TEXT,"
             "source_merge_order INTEGER NOT NULL"
             ");");

    exec_sql(master_db,
             "INSERT INTO master_metadata(key, value) VALUES('schema_version', 'otcb_predecessor_master_v1') ON CONFLICT(key) DO NOTHING;"
             "INSERT INTO master_metadata(key, value) VALUES('build_mode', 'build-predecessor-master') ON CONFLICT(key) DO NOTHING;"
             "INSERT INTO master_metadata(key, value) VALUES('selection_policy', 'first_seen_child_position_wins_in_operator_input_order') ON CONFLICT(key) DO NOTHING;"
             "INSERT INTO master_metadata(key, value) VALUES('first_win_policy', 'true') ON CONFLICT(key) DO NOTHING;");
}

void validate_attached_source(sqlite3* master_db) {
    if (select_int(master_db, "SELECT EXISTS(SELECT 1 FROM src.sqlite_master WHERE type='table' AND name='artifact_metadata');") != 1 ||
        select_int(master_db, "SELECT EXISTS(SELECT 1 FROM src.sqlite_master WHERE type='table' AND name='positions');") != 1 ||
        select_int(master_db, "SELECT EXISTS(SELECT 1 FROM src.sqlite_master WHERE type='table' AND name='canonical_predecessors');") != 1) {
        throw std::runtime_error("Source is not a recognized canonical predecessor payload");
    }
    if (select_int(master_db, "SELECT EXISTS(SELECT 1 FROM src.artifact_metadata WHERE key='artifact_schema_version' AND value='otcb_canonical_predecessors_v1');") != 1) {
        throw std::runtime_error("Source predecessor payload schema mismatch");
    }
    if (select_int(master_db, "SELECT EXISTS(SELECT 1 FROM src.artifact_metadata WHERE key='payload_type' AND value='canonical_predecessor_edges');") != 1) {
        throw std::runtime_error("Source predecessor payload type mismatch");
    }
}

}  // namespace

PredecessorMasterResult build_predecessor_master(const BuildConfig& config, ProgressReporter* reporter) {
    if (reporter != nullptr) {
        reporter->stage_started(ProgressStage::AggregateCounts, "building predecessor master database from source artifacts");
    }

    if (!config.master_output.parent_path().empty()) {
        std::filesystem::create_directories(config.master_output.parent_path());
    }

    SqliteDb master_db(config.master_output);
    ensure_master_schema(master_db.get());

    Statement updated_stmt(master_db.get(),
                           "INSERT INTO master_metadata(key, value) VALUES('last_updated_utc', ?) "
                           "ON CONFLICT(key) DO UPDATE SET value = excluded.value;");
    bind_text(updated_stmt.get(), 1, utc_now_iso8601());
    step_expect_done(master_db.get(), updated_stmt.get());

    Statement created_stmt(master_db.get(),
                           "INSERT INTO master_metadata(key, value) VALUES('created_utc', ?) ON CONFLICT(key) DO NOTHING;");
    bind_text(created_stmt.get(), 1, utc_now_iso8601());
    step_expect_done(master_db.get(), created_stmt.get());

    PredecessorMasterResult result;
    result.master_output = config.master_output;
    result.total_sources = static_cast<int>(config.source_predecessors.size());

    for (int source_index = 0; source_index < static_cast<int>(config.source_predecessors.size()); ++source_index) {
        const auto& source_path = config.source_predecessors[static_cast<std::size_t>(source_index)];
        const auto source_sqlite_path = resolve_source_sqlite_path(source_path);
        const std::string source_sqlite = source_sqlite_path.lexically_normal().generic_string();
        const std::string source_path_text = source_path.lexically_normal().generic_string();

        exec_sql_allow_error(master_db.get(), "DETACH DATABASE src;");
        exec_sql(master_db.get(), "ATTACH DATABASE '" + source_sqlite + "' AS src;");
        validate_attached_source(master_db.get());
        Statement insert_chunk_stmt(master_db.get(),
                                    "INSERT OR IGNORE INTO master_positions("
                                    "position_key, parent_position_key, incoming_move_uci, depth_from_root, edge_support_count, selection_policy_version, source_artifact_id, source_merge_order"
                                    ") "
                                    "SELECT c.position_key, p.position_key, cp.incoming_move_uci, c.depth_from_root, cp.edge_support_count, cp.selection_policy_version, "
                                    "COALESCE((SELECT value FROM src.artifact_metadata WHERE key='artifact_id' LIMIT 1), ?), ? "
                                    "FROM src.canonical_predecessors cp "
                                    "JOIN src.positions c ON c.position_id = cp.child_position_id "
                                    "LEFT JOIN src.positions p ON p.position_id = cp.parent_position_id "
                                    "WHERE c.position_id >= ? AND c.position_id < ? "
                                    "ORDER BY c.position_id;");
        Statement insert_source_row_stmt(master_db.get(),
                                         "INSERT INTO merge_sources("
                                         "merge_order, source_path, source_artifact_id, source_min_rating, source_max_rating, source_rating_policy, source_time_control_id, inserted_positions, skipped_existing_positions, merge_started_utc, merge_completed_utc, source_deleted_after_merge"
                                         ") VALUES ("
                                         "?, ?, "
                                         "(SELECT value FROM src.artifact_metadata WHERE key='artifact_id' LIMIT 1),"
                                         "(SELECT value FROM src.artifact_metadata WHERE key='min_rating' LIMIT 1),"
                                         "(SELECT value FROM src.artifact_metadata WHERE key='max_rating' LIMIT 1),"
                                         "(SELECT value FROM src.artifact_metadata WHERE key='rating_policy' LIMIT 1),"
                                         "(SELECT value FROM src.artifact_metadata WHERE key='time_control_id' LIMIT 1),"
                                         "?, ?, ?, ?, ?);");

        const int merge_order = select_int(master_db.get(), "SELECT COALESCE(MAX(merge_order), 0) + 1 FROM merge_sources;");
        const int max_position_id = select_int(master_db.get(), "SELECT COALESCE(MAX(position_id), 0) FROM src.positions;");
        const auto source_fallback_artifact_id = source_sqlite_path.stem().string();
        const auto merge_started = utc_now_iso8601();

        int source_scanned = 0;
        int source_inserted = 0;
        int source_skipped = 0;

        for (int start_id = 1; start_id <= max_position_id; start_id += config.merge_batch_size) {
            const int end_id = start_id + config.merge_batch_size;
            const int before_count = select_int(master_db.get(), "SELECT COUNT(*) FROM master_positions;");
            bind_text(insert_chunk_stmt.get(), 1, source_fallback_artifact_id);
            bind_int(insert_chunk_stmt.get(), 2, merge_order);
            bind_int(insert_chunk_stmt.get(), 3, start_id);
            bind_int(insert_chunk_stmt.get(), 4, end_id);
            step_expect_done(master_db.get(), insert_chunk_stmt.get());
            reset_statement(insert_chunk_stmt.get());
            const int after_count = select_int(master_db.get(), "SELECT COUNT(*) FROM master_positions;");

            const int scanned_chunk = select_int(master_db.get(), "SELECT COUNT(*) FROM src.positions WHERE position_id >= " + std::to_string(start_id) + " AND position_id < " + std::to_string(end_id) + ";");
            const int inserted_chunk = after_count - before_count;
            const int skipped_chunk = scanned_chunk - inserted_chunk;
            source_scanned += scanned_chunk;
            source_inserted += inserted_chunk;
            source_skipped += skipped_chunk;

            if (reporter != nullptr) {
                const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - reporter->snapshot().stage_started_at).count();
                reporter->update([&](ProgressSnapshot& snapshot) {
                    snapshot.current_range_index = source_index + 1;
                    snapshot.ranges_planned = result.total_sources;
                    snapshot.ranges_completed = source_index;
                    snapshot.games_scanned = source_scanned;
                    snapshot.games_accepted = source_inserted;
                    snapshot.games_rejected = source_skipped;
                    snapshot.throughput_per_second = elapsed > 0.0 ? std::optional<double>(source_scanned / elapsed) : std::nullopt;
                    snapshot.last_event_message = "merging source " + std::to_string(source_index + 1) + "/" + std::to_string(result.total_sources) + " " + source_path_text;
                });
            }
        }

        if (!config.skip_integrity_check) {
            exec_sql(master_db.get(), "PRAGMA quick_check;");
        }

        bool deleted = false;
        if (config.delete_source_after_merge) {
            if (!safe_to_delete_source(source_path)) {
                throw std::runtime_error("Refusing to delete ambiguous source path: " + source_path.string());
            }
            if (std::filesystem::is_directory(source_path)) {
                std::filesystem::remove_all(source_path);
            } else {
                std::filesystem::remove(source_path);
            }
            deleted = true;
        }

        bind_int(insert_source_row_stmt.get(), 1, merge_order);
        bind_text(insert_source_row_stmt.get(), 2, source_path_text);
        bind_int(insert_source_row_stmt.get(), 3, source_inserted);
        bind_int(insert_source_row_stmt.get(), 4, source_skipped);
        bind_text(insert_source_row_stmt.get(), 5, merge_started);
        bind_text(insert_source_row_stmt.get(), 6, utc_now_iso8601());
        bind_int(insert_source_row_stmt.get(), 7, deleted ? 1 : 0);
        step_expect_done(master_db.get(), insert_source_row_stmt.get());
        reset_statement(insert_source_row_stmt.get());

        result.rows_scanned += source_scanned;
        result.rows_inserted += source_inserted;
        result.rows_skipped_existing += source_skipped;

        exec_sql(master_db.get(), "DETACH DATABASE src;");
    }

    const auto metadata_path = config.master_output.parent_path() / (config.master_output.stem().string() + ".metadata.json");
    std::ofstream metadata(metadata_path, std::ios::binary);
    metadata << "{\n"
             << "  \"schema_version\": \"otcb_predecessor_master_v1\",\n"
             << "  \"build_mode\": \"build-predecessor-master\",\n"
             << "  \"master_output\": \"" << result.master_output.lexically_normal().generic_string() << "\",\n"
             << "  \"source_count\": " << result.total_sources << ",\n"
             << "  \"rows_scanned\": " << result.rows_scanned << ",\n"
             << "  \"rows_inserted\": " << result.rows_inserted << ",\n"
             << "  \"rows_skipped_existing\": " << result.rows_skipped_existing << ",\n"
             << "  \"first_seen_wins\": true,\n"
             << "  \"created_utc\": \"" << utc_now_iso8601() << "\"\n"
             << "}\n";

    if (reporter != nullptr) {
        reporter->stage_completed("predecessor master merge complete");
    }

    return result;
}

}  // namespace otcb
