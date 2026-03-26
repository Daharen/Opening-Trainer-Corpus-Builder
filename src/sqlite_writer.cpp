#include "otcb/sqlite_writer.hpp"

#include <sqlite3.h>

#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <map>

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

std::vector<std::string> split(const std::string& text, const char delimiter) {
    std::vector<std::string> out;
    std::string current;
    for (const char ch : text) {
        if (ch == delimiter) {
            out.push_back(current);
            current.clear();
        } else {
            current.push_back(ch);
        }
    }
    out.push_back(current);
    return out;
}

int piece_code(const char piece) {
    switch (piece) {
        case 'P': return 1;
        case 'N': return 2;
        case 'B': return 3;
        case 'R': return 4;
        case 'Q': return 5;
        case 'K': return 6;
        case 'p': return 7;
        case 'n': return 8;
        case 'b': return 9;
        case 'r': return 10;
        case 'q': return 11;
        case 'k': return 12;
        default: return 0;
    }
}

std::string encode_compact_position_key(const std::string& position_key) {
    const auto parts = split(position_key, ' ');
    if (parts.size() < 4) {
        throw std::runtime_error("Invalid normalized position key for compact encoding");
    }
    std::vector<int> squares;
    for (const auto& rank : split(parts[0], '/')) {
        for (const char ch : rank) {
            if (ch >= '1' && ch <= '8') {
                for (int i = 0; i < ch - '0'; ++i) {
                    squares.push_back(0);
                }
            } else {
                squares.push_back(piece_code(ch));
            }
        }
    }
    if (squares.size() != 64) {
        throw std::runtime_error("Invalid board placement for compact key");
    }
    std::vector<unsigned char> packed(34, 0);
    for (int i = 0; i < 32; ++i) {
        packed[i] = static_cast<unsigned char>((squares[2 * i] << 4) | squares[2 * i + 1]);
    }
    unsigned char flags = 0;
    if (parts[1] == "w") flags |= 0x1;
    if (parts[2].find('K') != std::string::npos) flags |= 0x2;
    if (parts[2].find('Q') != std::string::npos) flags |= 0x4;
    if (parts[2].find('k') != std::string::npos) flags |= 0x8;
    if (parts[2].find('q') != std::string::npos) flags |= 0x10;
    packed[32] = flags;
    if (parts[3] != "-" && parts[3].size() == 2) {
        const int file = parts[3][0] - 'a';
        const int rank = parts[3][1] - '1';
        packed[33] = static_cast<unsigned char>(rank * 8 + file + 1);
    } else {
        packed[33] = 0;
    }
    static const char* hex = "0123456789abcdef";
    std::string out;
    out.reserve(packed.size() * 2);
    for (unsigned char byte : packed) {
        out.push_back(hex[(byte >> 4) & 0xF]);
        out.push_back(hex[byte & 0xF]);
    }
    return out;
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
        stats.position_moves_rows = stats.moves_rows;
        return stats;
    } catch (...) {
        exec_sql(db.get(), "ROLLBACK;");
        throw;
    }
}

SqliteWriteStats write_aggregate_payload_sqlite_compact_v2(const std::filesystem::path& sqlite_path,
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
                 "position_key_compact TEXT NOT NULL,"
                 "position_key_inspect TEXT NOT NULL,"
                 "position_key_format TEXT NOT NULL,"
                 "side_to_move TEXT NOT NULL,"
                 "total_observations INTEGER NOT NULL"
                 ");"
                 "CREATE TABLE moves("
                 "move_id INTEGER PRIMARY KEY,"
                 "uci_text TEXT NOT NULL UNIQUE"
                 ");"
                 "CREATE TABLE position_moves("
                 "position_id INTEGER NOT NULL,"
                 "move_id INTEGER NOT NULL,"
                 "raw_count INTEGER NOT NULL,"
                 "PRIMARY KEY(position_id, move_id),"
                 "FOREIGN KEY(position_id) REFERENCES positions(position_id),"
                 "FOREIGN KEY(move_id) REFERENCES moves(move_id)"
                 ");"
                 "CREATE UNIQUE INDEX idx_positions_inspect ON positions(position_key_inspect, side_to_move);"
                 "CREATE INDEX idx_position_moves_position_id ON position_moves(position_id);");

        Statement metadata_stmt(db.get(), "INSERT INTO artifact_metadata(key, value) VALUES (?, ?);");
        Statement position_stmt(db.get(), "INSERT INTO positions(position_key_compact, position_key_inspect, position_key_format, side_to_move, total_observations) VALUES (?, ?, ?, ?, ?);");
        Statement move_insert_stmt(db.get(), "INSERT INTO moves(uci_text) VALUES (?);");
        Statement move_select_stmt(db.get(), "SELECT move_id FROM moves WHERE uci_text = ?;");
        Statement position_move_stmt(db.get(), "INSERT INTO position_moves(position_id, move_id, raw_count) VALUES (?, ?, ?);");

        const std::vector<std::pair<std::string, std::string>> metadata = {
            {"artifact_schema_version", "otcb_exact_sqlite_v2_compact"},
            {"artifact_id", artifact_id},
            {"payload_format", to_string(config.payload_format)},
            {"payload_version", "2"},
            {"source_path", summary.source_path},
            {"min_rating", std::to_string(config.min_rating)},
            {"max_rating", std::to_string(config.max_rating)},
            {"rating_policy", to_string(*config.rating_policy)},
            {"retained_ply", std::to_string(config.retained_ply)},
            {"raw_counts_preserved", "true"},
            {"effective_weights_precomputed", "false"},
            {"total_accepted_games", std::to_string(summary.total_games_accepted_upstream)},
            {"total_emitted_positions", std::to_string(summary.total_unique_positions_emitted)},
            {"total_emitted_move_entries", std::to_string(summary.total_aggregate_move_entries_emitted)},
            {"position_key_format_description", "packed_34_byte_binary_with_inspectable_normalized_key"},
            {"move_representation_description", "integer_move_dictionary_with_uci_lookup"},
        };
        for (const auto& [key, value] : metadata) {
            bind_text(metadata_stmt.get(), 1, key);
            bind_text(metadata_stmt.get(), 2, value);
            step_expect_done(db.get(), metadata_stmt.get());
            reset_statement(metadata_stmt.get());
        }

        SqliteWriteStats stats;
        for (const auto& record : positions) {
            const auto compact_key = encode_compact_position_key(record.position_key);
            bind_text(position_stmt.get(), 1, compact_key);
            bind_text(position_stmt.get(), 2, record.position_key);
            bind_text(position_stmt.get(), 3, to_string(*config.position_key_format));
            bind_text(position_stmt.get(), 4, record.side_to_move);
            bind_int(position_stmt.get(), 5, record.total_observations);
            step_expect_done(db.get(), position_stmt.get());
            reset_statement(position_stmt.get());
            const auto position_id = static_cast<int>(sqlite3_last_insert_rowid(db.get()));
            ++stats.positions_rows;
            stats.total_raw_observations += record.total_observations;

            for (const auto& move : record.candidate_moves) {
                int move_id = 0;
                bind_text(move_select_stmt.get(), 1, move.move_key);
                const int rc = sqlite3_step(move_select_stmt.get());
                if (rc == SQLITE_ROW) {
                    move_id = sqlite3_column_int(move_select_stmt.get(), 0);
                    reset_statement(move_select_stmt.get());
                } else {
                    reset_statement(move_select_stmt.get());
                    bind_text(move_insert_stmt.get(), 1, move.move_key);
                    step_expect_done(db.get(), move_insert_stmt.get());
                    reset_statement(move_insert_stmt.get());
                    move_id = static_cast<int>(sqlite3_last_insert_rowid(db.get()));
                    ++stats.moves_rows;
                }

                bind_int(position_move_stmt.get(), 1, position_id);
                bind_int(position_move_stmt.get(), 2, move_id);
                bind_int(position_move_stmt.get(), 3, move.raw_count);
                step_expect_done(db.get(), position_move_stmt.get());
                reset_statement(position_move_stmt.get());
                ++stats.position_moves_rows;
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
