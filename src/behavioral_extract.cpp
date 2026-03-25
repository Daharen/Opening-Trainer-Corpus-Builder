#include "otcb/behavioral_extract.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <zstd.h>

#include "otcb/chess_board.hpp"
#include "otcb/chess_types.hpp"
#include "otcb/position_key.hpp"
#include "otcb/san_replay.hpp"
#include <sqlite3.h>

namespace otcb {
namespace {

struct TimedMoveToken {
    std::string san;
    std::optional<double> clk_after_seconds;
};

struct ParsedGame {
    std::map<std::string, std::string> tags;
    std::string movetext;
    std::string source_file;
    std::string source_month;
    int source_game_index = 0;
};

struct ParsedTimeControl {
    std::string raw;
    int initial_seconds = 0;
    int increment_seconds = 0;
    std::string canonical;
};

static std::string trim(const std::string& in) {
    std::size_t s = 0;
    while (s < in.size() && std::isspace(static_cast<unsigned char>(in[s]))) ++s;
    std::size_t e = in.size();
    while (e > s && std::isspace(static_cast<unsigned char>(in[e - 1]))) --e;
    return in.substr(s, e - s);
}


static std::optional<std::string> parse_tag_name(const std::string& line) {
    if (line.size() < 5 || line.front() != '[' || line.back() != ']') return std::nullopt;
    const auto sp = line.find(' ');
    if (sp == std::string::npos) return std::nullopt;
    return line.substr(1, sp - 1);
}

static std::optional<std::pair<std::string, std::string>> parse_tag(const std::string& line) {
    if (line.size() < 5 || line.front() != '[' || line.back() != ']') return std::nullopt;
    const auto sp = line.find(' ');
    if (sp == std::string::npos || sp + 2 >= line.size()) return std::nullopt;
    if (line[sp + 1] != '"' || line[line.size() - 2] != '"') return std::nullopt;
    return std::make_pair(line.substr(1, sp - 1), line.substr(sp + 2, line.size() - sp - 4));
}

static std::optional<int> parse_int_str(const std::string& s) {
    if (s.empty()) return std::nullopt;
    for (char ch : s) {
        if (!std::isdigit(static_cast<unsigned char>(ch))) return std::nullopt;
    }
    try { return std::stoi(s); } catch (...) { return std::nullopt; }
}

static std::optional<ParsedTimeControl> parse_time_control(const std::string& raw) {
    const auto plus = raw.find('+');
    if (plus == std::string::npos || plus == 0 || plus + 1 >= raw.size()) return std::nullopt;
    const auto a = parse_int_str(raw.substr(0, plus));
    const auto b = parse_int_str(raw.substr(plus + 1));
    if (!a.has_value() || !b.has_value()) return std::nullopt;
    ParsedTimeControl tc;
    tc.raw = raw;
    tc.initial_seconds = *a;
    tc.increment_seconds = *b;
    tc.canonical = std::to_string(tc.initial_seconds) + "+" + std::to_string(tc.increment_seconds);
    return tc;
}

static std::optional<double> parse_clk_comment(const std::string& comment) {
    const auto marker = comment.find("[%clk ");
    if (marker == std::string::npos) return std::nullopt;
    auto end = comment.find(']', marker);
    if (end == std::string::npos) end = comment.size();
    std::string value = trim(comment.substr(marker + 6, end - marker - 6));
    std::vector<int> parts;
    std::stringstream ss(value);
    std::string piece;
    while (std::getline(ss, piece, ':')) {
        const auto iv = parse_int_str(piece);
        if (!iv.has_value()) return std::nullopt;
        parts.push_back(*iv);
    }
    if (parts.size() != 3) return std::nullopt;
    return static_cast<double>(parts[0] * 3600 + parts[1] * 60 + parts[2]);
}

static bool is_result_token(const std::string& token) {
    return token == "1-0" || token == "0-1" || token == "1/2-1/2" || token == "*";
}

static bool is_move_number(const std::string& token) {
    if (token.empty()) return false;
    std::size_t i = 0;
    while (i < token.size() && std::isdigit(static_cast<unsigned char>(token[i]))) ++i;
    if (i == 0) return false;
    while (i < token.size() && token[i] == '.') ++i;
    return i == token.size();
}

static std::vector<TimedMoveToken> tokenize_timed_movetext(const std::string& movetext) {
    std::vector<TimedMoveToken> out;
    std::string cur;
    bool in_comment = false;
    std::string comment;
    int variation_depth = 0;

    auto flush = [&]() {
        if (cur.empty() || variation_depth > 0) {
            cur.clear();
            return;
        }
        if (is_move_number(cur) || (!cur.empty() && cur.front() == '$')) {
            cur.clear();
            return;
        }
        std::string tok = cur;
        while (!tok.empty() && (tok.back() == '!' || tok.back() == '?' || tok.back() == '+' || tok.back() == '#')) tok.pop_back();
        cur.clear();
        if (tok.empty() || is_result_token(tok)) return;
        out.push_back({tok, std::nullopt});
    };

    for (char ch : movetext) {
        if (in_comment) {
            if (ch == '}') {
                in_comment = false;
                if (!out.empty()) {
                    const auto clk = parse_clk_comment(comment);
                    if (clk.has_value()) out.back().clk_after_seconds = clk;
                }
                comment.clear();
            } else {
                comment.push_back(ch);
            }
            continue;
        }
        if (ch == '{') {
            flush();
            in_comment = true;
            comment.clear();
            continue;
        }
        if (ch == '(') { flush(); ++variation_depth; continue; }
        if (ch == ')') { flush(); if (variation_depth > 0) --variation_depth; continue; }
        if (std::isspace(static_cast<unsigned char>(ch))) { flush(); continue; }
        if (variation_depth == 0) cur.push_back(ch);
    }
    flush();
    return out;
}

class LineReader {
public:
    explicit LineReader(const std::filesystem::path& path) : path_(path) {
        file_.open(path, std::ios::binary);
        if (!file_) {
            throw std::runtime_error("Failed to open input: " + path.string());
        }
        is_zst_ = path.extension() == ".zst";
        if (!is_zst_) return;

        std::array<unsigned char, 4> magic{};
        file_.read(reinterpret_cast<char*>(magic.data()), static_cast<std::streamsize>(magic.size()));
        file_.clear();
        file_.seekg(0, std::ios::beg);
        const bool has_zstd_magic = magic == std::array<unsigned char, 4>{0x28, 0xB5, 0x2F, 0xFD};
        if (!has_zstd_magic) {
            is_zst_ = false;
            return;
        }

        dstream_ = ZSTD_createDStream();
        if (!dstream_) throw std::runtime_error("Failed to create zstd stream.");
        const auto init = ZSTD_initDStream(dstream_);
        if (ZSTD_isError(init)) throw std::runtime_error("Failed to init zstd stream.");
    }

    ~LineReader() {
        if (dstream_) ZSTD_freeDStream(dstream_);
    }

    bool getline(std::string& line) {
        line.clear();
        while (true) {
            const auto nl = pending_.find('\n');
            if (nl != std::string::npos) {
                line = pending_.substr(0, nl);
                pending_.erase(0, nl + 1);
                if (!line.empty() && line.back() == '\r') line.pop_back();
                return true;
            }
            if (!fill_pending()) {
                if (pending_.empty()) return false;
                line = pending_;
                pending_.clear();
                if (!line.empty() && line.back() == '\r') line.pop_back();
                return true;
            }
        }
    }

private:
    bool fill_pending() {
        if (!is_zst_) {
            std::string chunk(65536, '\0');
            file_.read(chunk.data(), static_cast<std::streamsize>(chunk.size()));
            const auto got = file_.gcount();
            if (got <= 0) return false;
            chunk.resize(static_cast<std::size_t>(got));
            pending_ += chunk;
            return true;
        }

        std::array<char, 131072> inbuf{};
        std::array<char, 131072> outbuf{};
        while (true) {
            if (zin_.pos == zin_.size) {
                file_.read(inbuf.data(), static_cast<std::streamsize>(inbuf.size()));
                const auto got = file_.gcount();
                if (got <= 0) return false;
                zinput_storage_.assign(inbuf.data(), static_cast<std::size_t>(got));
                zin_ = ZSTD_inBuffer{zinput_storage_.data(), zinput_storage_.size(), 0};
            }
            ZSTD_outBuffer zout{outbuf.data(), outbuf.size(), 0};
            const auto ret = ZSTD_decompressStream(dstream_, &zout, &zin_);
            if (ZSTD_isError(ret)) throw std::runtime_error("zstd decompression failed.");
            if (zout.pos > 0) {
                pending_.append(outbuf.data(), outbuf.data() + static_cast<std::ptrdiff_t>(zout.pos));
                return true;
            }
            if (ret == 0 && zin_.pos == zin_.size) return false;
        }
    }

    std::filesystem::path path_;
    std::ifstream file_;
    bool is_zst_ = false;
    std::string pending_;
    ZSTD_DStream* dstream_ = nullptr;
    std::string zinput_storage_;
    ZSTD_inBuffer zin_{nullptr, 0, 0};
};

static std::string infer_month(const ParsedGame& game, const BehavioralExtractOptions& opts) {
    if (opts.month_override.has_value()) return *opts.month_override;
    const auto it = game.tags.find("Date");
    if (it == game.tags.end() || it->second.size() < 7) return "unknown";
    return it->second.substr(0, 7);
}

static std::string normalize_elo_band(const int elo) {
    const int lo = (elo / 200) * 200;
    const int hi = lo + 199;
    return std::to_string(lo) + "-" + std::to_string(hi);
}

static std::string clock_pressure_bucket(const double ratio) {
    if (ratio < 0.10) return "critical";
    if (ratio < 0.25) return "low";
    if (ratio < 0.50) return "medium";
    return "comfortable";
}

static std::string think_bucket(const double seconds) {
    if (seconds < 2.0) return "instant";
    if (seconds < 10.0) return "short";
    if (seconds < 30.0) return "medium";
    return "long";
}

static std::uint64_t fnv1a_64(const std::string& text) {
    std::uint64_t h = 1469598103934665603ULL;
    for (unsigned char c : text) {
        h ^= c;
        h *= 1099511628211ULL;
    }
    return h;
}

static void exec_sql(sqlite3* db, const std::string& sql) {
    char* errmsg = nullptr;
    if (sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &errmsg) != SQLITE_OK) {
        std::string msg = errmsg ? errmsg : "sqlite exec failure";
        sqlite3_free(errmsg);
        throw std::runtime_error(msg);
    }
}

static void init_schema(sqlite3* db) {
    exec_sql(db,
        "CREATE TABLE IF NOT EXISTS extract_manifest ("
        "schema_version TEXT NOT NULL, builder_version TEXT NOT NULL, creation_timestamp_utc TEXT NOT NULL,"
        "deterministic_settings TEXT NOT NULL, input_source_labels TEXT NOT NULL, banding_policy_version TEXT NOT NULL,"
        "bucketing_policy_version TEXT NOT NULL, time_control_normalization_policy_version TEXT NOT NULL);"
        "CREATE TABLE IF NOT EXISTS source_files (id INTEGER PRIMARY KEY, source_file TEXT NOT NULL UNIQUE);"
        "CREATE TABLE IF NOT EXISTS games ("
        "game_id TEXT PRIMARY KEY, source_file_id INTEGER NOT NULL, source_month TEXT NOT NULL, source_game_index INTEGER NOT NULL,"
        "white_elo INTEGER, black_elo INTEGER, time_control_raw TEXT, time_control_initial_seconds INTEGER,"
        "time_control_increment_seconds INTEGER, result TEXT, accepted INTEGER NOT NULL, rejection_reason TEXT,"
        "FOREIGN KEY(source_file_id) REFERENCES source_files(id));"
        "CREATE TABLE IF NOT EXISTS move_events ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, game_id TEXT NOT NULL, source_file TEXT NOT NULL, source_month TEXT NOT NULL,"
        "game_ply_index INTEGER NOT NULL, fullmove_number INTEGER NOT NULL, side_to_move TEXT NOT NULL, position_key TEXT NOT NULL,"
        "move_uci TEXT NOT NULL, time_control_raw TEXT NOT NULL, time_control_initial_seconds INTEGER NOT NULL,"
        "time_control_increment_seconds INTEGER NOT NULL, white_elo INTEGER, black_elo INTEGER, mover_elo INTEGER, mover_elo_band TEXT,"
        "mover_clock_before_seconds REAL NOT NULL, mover_clock_after_seconds REAL NOT NULL, think_time_seconds REAL NOT NULL,"
        "opponent_previous_think_time_seconds REAL, is_first_move_for_side INTEGER NOT NULL, termination_result TEXT,"
        "opening_ply_index INTEGER NOT NULL, mover_clock_ratio REAL NOT NULL, think_time_ratio REAL NOT NULL,"
        "clock_pressure_bucket TEXT NOT NULL, prev_opp_think_bucket TEXT, time_control_id TEXT NOT NULL, position_move_key TEXT NOT NULL);"
        "CREATE TABLE IF NOT EXISTS invalid_rows ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, source_file TEXT NOT NULL, game_id TEXT, game_ply_index INTEGER, reason TEXT NOT NULL, detail TEXT);");
    exec_sql(db,
        "CREATE INDEX IF NOT EXISTS idx_move_events_elo_band ON move_events(mover_elo_band);"
        "CREATE INDEX IF NOT EXISTS idx_move_events_tc ON move_events(time_control_id);"
        "CREATE INDEX IF NOT EXISTS idx_move_events_pos ON move_events(position_key);"
        "CREATE INDEX IF NOT EXISTS idx_move_events_move ON move_events(move_uci);"
        "CREATE INDEX IF NOT EXISTS idx_move_events_month ON move_events(source_month);");
}

static std::string quote_csv(const std::vector<std::string>& items) {
    std::ostringstream out;
    for (std::size_t i = 0; i < items.size(); ++i) {
        if (i) out << ',';
        out << items[i];
    }
    return out.str();
}

BehavioralExtractOptions parse_or_throw(int argc, char** argv) {
    BehavioralExtractOptions opts;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto need = [&](const std::string& flag) {
            if (i + 1 >= argc) throw std::runtime_error("Missing value for " + flag);
            return std::string(argv[++i]);
        };
        if (arg == "--help") { print_behavioral_extract_usage(argc > 0 ? argv[0] : "build_behavioral_training_extract"); std::exit(0); }
        if (arg == "--input") { opts.input_paths.emplace_back(need(arg)); continue; }
        if (arg == "--output") { opts.output_path = need(arg); continue; }
        if (arg == "--time-controls") { opts.time_controls.push_back(need(arg)); continue; }
        if (arg == "--elo-bands") { opts.elo_bands.push_back(need(arg)); continue; }
        if (arg == "--month") { opts.month_override = need(arg); continue; }
        if (arg == "--max-games") { opts.max_games = std::stoi(need(arg)); continue; }
        if (arg == "--resume") { opts.resume = true; continue; }
        if (arg == "--overwrite") { opts.overwrite = true; continue; }
        if (arg == "--workers") { opts.workers = std::stoi(need(arg)); continue; }
        if (arg == "--log-every") { opts.log_every = std::stoi(need(arg)); continue; }
        if (arg == "--emit-invalid-report") { opts.emit_invalid_report = true; continue; }
        if (arg == "--source-label") { opts.source_label = need(arg); continue; }
        if (arg == "--strict") { opts.strict = true; continue; }
        throw std::runtime_error("Unknown argument: " + arg);
    }
    if (opts.input_paths.empty()) throw std::runtime_error("--input is required at least once");
    if (opts.output_path.empty()) throw std::runtime_error("--output is required");
    if (opts.resume && opts.overwrite) throw std::runtime_error("--resume and --overwrite are mutually exclusive");
    return opts;
}

}  // namespace

BehavioralExtractOptions parse_behavioral_extract_cli(int argc, char** argv) {
    return parse_or_throw(argc, argv);
}

void print_behavioral_extract_usage(const std::string& program_name) {
    std::cout
        << "Usage: " << program_name << " --input <pgn|pgn.zst> [--input ...] --output <sqlite> [options]\n"
        << "Options:\n"
        << "  --time-controls <initial+increment>   Optional include filter; repeatable.\n"
        << "  --elo-bands <lo-hi>                   Optional include filter; repeatable (200-point default band policy).\n"
        << "  --month <YYYY-MM>                     Optional source month override.\n"
        << "  --max-games <n>                       Optional cap for debugging.\n"
        << "  --resume                              Reuse existing artifact deterministically.\n"
        << "  --overwrite                           Explicitly recreate output.\n"
        << "  --workers <n>                         Reserved for deterministic future parallelism (currently 1-path).\n"
        << "  --log-every <n>                       Progress cadence.\n"
        << "  --emit-invalid-report                 Persist invalid_rows entries.\n"
        << "  --source-label <label>                Optional source label persisted in manifest summary.\n"
        << "  --strict                              Fail hard on malformed timed records.\n";
}

BehavioralExtractCounters build_behavioral_training_extract(const BehavioralExtractOptions& options) {
    BehavioralExtractCounters counters;

    std::vector<std::filesystem::path> inputs = options.input_paths;
    std::sort(inputs.begin(), inputs.end());

    if (std::filesystem::exists(options.output_path)) {
        if (options.overwrite) {
            std::filesystem::remove(options.output_path);
        } else if (!options.resume) {
            throw std::runtime_error("Output exists; pass --overwrite or --resume.");
        }
    }

    sqlite3* db = nullptr;
    if (sqlite3_open(options.output_path.string().c_str(), &db) != SQLITE_OK) {
        throw std::runtime_error("Failed to open sqlite output");
    }
    std::unique_ptr<sqlite3, decltype(&sqlite3_close)> db_guard(db, sqlite3_close);
    init_schema(db);
    exec_sql(db, "BEGIN IMMEDIATE TRANSACTION;");

    exec_sql(db, "DELETE FROM extract_manifest;");
    sqlite3_stmt* manifest_stmt = nullptr;
    sqlite3_prepare_v2(db, "INSERT INTO extract_manifest(schema_version,builder_version,creation_timestamp_utc,deterministic_settings,input_source_labels,banding_policy_version,bucketing_policy_version,time_control_normalization_policy_version) VALUES(?,?,?,?,?,?,?,?);", -1, &manifest_stmt, nullptr);
    sqlite3_bind_text(manifest_stmt, 1, "1", -1, SQLITE_STATIC);
    sqlite3_bind_text(manifest_stmt, 2, "otcb_behavioral_extract_v1", -1, SQLITE_STATIC);
    sqlite3_bind_text(manifest_stmt, 3, "deterministic", -1, SQLITE_STATIC);
    sqlite3_bind_text(manifest_stmt, 4, "sorted_inputs|stable_game_order|stable_ply_order", -1, SQLITE_STATIC);
    const std::string labels = options.source_label.value_or("") + (options.source_label.has_value() ? "|" : "") + quote_csv({});
    sqlite3_bind_text(manifest_stmt, 5, labels.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(manifest_stmt, 6, "elo_band_v1_200", -1, SQLITE_STATIC);
    sqlite3_bind_text(manifest_stmt, 7, "clock_bucket_v1", -1, SQLITE_STATIC);
    sqlite3_bind_text(manifest_stmt, 8, "time_control_v1_exact_initial_plus_increment", -1, SQLITE_STATIC);
    if (sqlite3_step(manifest_stmt) != SQLITE_DONE) throw std::runtime_error("manifest insert failed");
    sqlite3_finalize(manifest_stmt);

    sqlite3_stmt* source_stmt = nullptr;
    sqlite3_stmt* game_stmt = nullptr;
    sqlite3_stmt* move_stmt = nullptr;
    sqlite3_stmt* invalid_stmt = nullptr;

    sqlite3_prepare_v2(db, "INSERT OR IGNORE INTO source_files(source_file) VALUES(?);", -1, &source_stmt, nullptr);
    sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO games(game_id,source_file_id,source_month,source_game_index,white_elo,black_elo,time_control_raw,time_control_initial_seconds,time_control_increment_seconds,result,accepted,rejection_reason) VALUES(?,?,?,?,?,?,?,?,?,?,?,?);", -1, &game_stmt, nullptr);
    sqlite3_prepare_v2(db, "INSERT INTO move_events(game_id,source_file,source_month,game_ply_index,fullmove_number,side_to_move,position_key,move_uci,time_control_raw,time_control_initial_seconds,time_control_increment_seconds,white_elo,black_elo,mover_elo,mover_elo_band,mover_clock_before_seconds,mover_clock_after_seconds,think_time_seconds,opponent_previous_think_time_seconds,is_first_move_for_side,termination_result,opening_ply_index,mover_clock_ratio,think_time_ratio,clock_pressure_bucket,prev_opp_think_bucket,time_control_id,position_move_key) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);", -1, &move_stmt, nullptr);
    sqlite3_prepare_v2(db, "INSERT INTO invalid_rows(source_file,game_id,game_ply_index,reason,detail) VALUES(?,?,?,?,?);", -1, &invalid_stmt, nullptr);

    std::set<std::string> allowed_tc(options.time_controls.begin(), options.time_controls.end());
    std::set<std::string> allowed_bands(options.elo_bands.begin(), options.elo_bands.end());

    for (const auto& input : inputs) {
        ++counters.files_processed;
        LineReader reader(input);

        int source_file_id = 0;
        sqlite3_reset(source_stmt);
        sqlite3_bind_text(source_stmt, 1, input.generic_string().c_str(), -1, SQLITE_TRANSIENT);
        if (sqlite3_step(source_stmt) != SQLITE_DONE) throw std::runtime_error("source insert failed");
        sqlite3_finalize(source_stmt);
        sqlite3_prepare_v2(db, "SELECT id FROM source_files WHERE source_file=?;", -1, &source_stmt, nullptr);
        sqlite3_bind_text(source_stmt, 1, input.generic_string().c_str(), -1, SQLITE_TRANSIENT);
        if (sqlite3_step(source_stmt) == SQLITE_ROW) source_file_id = sqlite3_column_int(source_stmt, 0);
        sqlite3_finalize(source_stmt);
        sqlite3_prepare_v2(db, "INSERT OR IGNORE INTO source_files(source_file) VALUES(?);", -1, &source_stmt, nullptr);

        std::string line;
        ParsedGame current;
        current.source_file = input.generic_string();
        bool in_headers = false;
        bool saw_headers = false;
        int game_index_for_file = 0;

        auto flush_game = [&]() {
            if (current.tags.empty() && current.movetext.empty()) return;
            ++game_index_for_file;
            ++counters.games_seen;
            current.source_game_index = game_index_for_file;
            current.source_month = infer_month(current, options);

            const auto tc_it = current.tags.find("TimeControl");
            const std::string tc_raw = tc_it == current.tags.end() ? "" : tc_it->second;
            const auto parsed_tc = parse_time_control(tc_raw);
            const auto white_elo = parse_int_str(current.tags.count("WhiteElo") ? current.tags.at("WhiteElo") : "");
            const auto black_elo = parse_int_str(current.tags.count("BlackElo") ? current.tags.at("BlackElo") : "");
            const std::string game_id = std::to_string(fnv1a_64(current.source_file + "#" + std::to_string(current.source_game_index)));

            auto reject_game = [&](const std::string& reason) {
                ++counters.games_rejected;
                sqlite3_reset(game_stmt);
                sqlite3_bind_text(game_stmt, 1, game_id.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int(game_stmt, 2, source_file_id);
                sqlite3_bind_text(game_stmt, 3, current.source_month.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int(game_stmt, 4, current.source_game_index);
                if (white_elo) sqlite3_bind_int(game_stmt, 5, *white_elo); else sqlite3_bind_null(game_stmt, 5);
                if (black_elo) sqlite3_bind_int(game_stmt, 6, *black_elo); else sqlite3_bind_null(game_stmt, 6);
                sqlite3_bind_text(game_stmt, 7, tc_raw.c_str(), -1, SQLITE_TRANSIENT);
                if (parsed_tc) sqlite3_bind_int(game_stmt, 8, parsed_tc->initial_seconds); else sqlite3_bind_null(game_stmt, 8);
                if (parsed_tc) sqlite3_bind_int(game_stmt, 9, parsed_tc->increment_seconds); else sqlite3_bind_null(game_stmt, 9);
                const std::string result = current.tags.count("Result") ? current.tags.at("Result") : "";
                sqlite3_bind_text(game_stmt, 10, result.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int(game_stmt, 11, 0);
                sqlite3_bind_text(game_stmt, 12, reason.c_str(), -1, SQLITE_TRANSIENT);
                if (sqlite3_step(game_stmt) != SQLITE_DONE) throw std::runtime_error("game reject insert failed");
            };

            if (!parsed_tc.has_value()) {
                ++counters.rows_skipped_invalid_time_control;
                if (options.strict) throw std::runtime_error("Invalid time control: " + tc_raw);
                reject_game("invalid_time_control");
                current = ParsedGame{};
                current.source_file = input.generic_string();
                return;
            }
            if (!allowed_tc.empty() && allowed_tc.count(parsed_tc->canonical) == 0) {
                reject_game("filtered_time_control");
                current = ParsedGame{};
                current.source_file = input.generic_string();
                return;
            }
            if (!white_elo.has_value() || !black_elo.has_value()) {
                reject_game("missing_elo");
                current = ParsedGame{};
                current.source_file = input.generic_string();
                return;
            }

            const auto timed = tokenize_timed_movetext(current.movetext);
            ChessBoard board;
            std::optional<double> prev_white_think;
            std::optional<double> prev_black_think;
            double white_clock = static_cast<double>(parsed_tc->initial_seconds);
            double black_clock = static_cast<double>(parsed_tc->initial_seconds);
            bool white_first = true;
            bool black_first = true;
            int emitted = 0;

            for (std::size_t idx = 0; idx < timed.size(); ++idx) {
                const auto& tm = timed[idx];
                const auto resolved = resolve_san_move(board, tm.san);
                if (!resolved.success || !resolved.move.has_value()) {
                    if (options.strict) throw std::runtime_error("SAN replay failed in timed extraction");
                    continue;
                }
                const bool white_to_move = board.side_to_move() == Color::White;
                const double before = white_to_move ? white_clock : black_clock;
                if (!tm.clk_after_seconds.has_value()) {
                    ++counters.rows_skipped_missing_clock;
                    if (options.strict) throw std::runtime_error("Missing [%clk] comment");
                    board.apply_move(*resolved.move);
                    continue;
                }
                const double after = *tm.clk_after_seconds;
                const double think = before + static_cast<double>(parsed_tc->increment_seconds) - after;
                if (think < -0.01) {
                    if (options.strict) throw std::runtime_error("Negative think time derived");
                    board.apply_move(*resolved.move);
                    continue;
                }

                const int mover_elo = white_to_move ? *white_elo : *black_elo;
                const std::string elo_band = normalize_elo_band(mover_elo);
                if (!allowed_bands.empty() && allowed_bands.count(elo_band) == 0) {
                    board.apply_move(*resolved.move);
                    if (white_to_move) white_clock = after; else black_clock = after;
                    continue;
                }

                const std::string side = white_to_move ? "white" : "black";
                const std::string position_key = make_position_key(board, PositionKeyFormat::FenNormalized);
                const std::string move_uci = move_to_uci(*resolved.move);
                const double clock_ratio = parsed_tc->initial_seconds > 0 ? before / static_cast<double>(parsed_tc->initial_seconds) : 0.0;
                const double think_ratio = parsed_tc->initial_seconds > 0 ? think / static_cast<double>(parsed_tc->initial_seconds) : 0.0;
                const auto prev_opp = white_to_move ? prev_black_think : prev_white_think;
                const bool first_for_side = white_to_move ? white_first : black_first;

                sqlite3_reset(move_stmt);
                sqlite3_bind_text(move_stmt, 1, game_id.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(move_stmt, 2, current.source_file.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(move_stmt, 3, current.source_month.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int(move_stmt, 4, static_cast<int>(idx + 1));
                sqlite3_bind_int(move_stmt, 5, static_cast<int>((idx / 2) + 1));
                sqlite3_bind_text(move_stmt, 6, side.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(move_stmt, 7, position_key.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(move_stmt, 8, move_uci.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(move_stmt, 9, tc_raw.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int(move_stmt, 10, parsed_tc->initial_seconds);
                sqlite3_bind_int(move_stmt, 11, parsed_tc->increment_seconds);
                sqlite3_bind_int(move_stmt, 12, *white_elo);
                sqlite3_bind_int(move_stmt, 13, *black_elo);
                sqlite3_bind_int(move_stmt, 14, mover_elo);
                sqlite3_bind_text(move_stmt, 15, elo_band.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_double(move_stmt, 16, before);
                sqlite3_bind_double(move_stmt, 17, after);
                sqlite3_bind_double(move_stmt, 18, think);
                if (prev_opp.has_value()) sqlite3_bind_double(move_stmt, 19, *prev_opp); else sqlite3_bind_null(move_stmt, 19);
                sqlite3_bind_int(move_stmt, 20, first_for_side ? 1 : 0);
                const std::string result = current.tags.count("Result") ? current.tags.at("Result") : "";
                sqlite3_bind_text(move_stmt, 21, result.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int(move_stmt, 22, static_cast<int>(idx + 1));
                sqlite3_bind_double(move_stmt, 23, clock_ratio);
                sqlite3_bind_double(move_stmt, 24, think_ratio);
                const std::string pressure = clock_pressure_bucket(clock_ratio);
                sqlite3_bind_text(move_stmt, 25, pressure.c_str(), -1, SQLITE_TRANSIENT);
                const std::string prev_bucket = prev_opp.has_value() ? think_bucket(*prev_opp) : "none";
                sqlite3_bind_text(move_stmt, 26, prev_bucket.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(move_stmt, 27, parsed_tc->canonical.c_str(), -1, SQLITE_TRANSIENT);
                const std::string pmk = position_key + "|" + move_uci;
                sqlite3_bind_text(move_stmt, 28, pmk.c_str(), -1, SQLITE_TRANSIENT);
                if (sqlite3_step(move_stmt) != SQLITE_DONE) throw std::runtime_error("move insert failed");

                ++counters.move_events_emitted;
                ++emitted;
                if (white_to_move) {
                    white_clock = after;
                    prev_white_think = think;
                    white_first = false;
                } else {
                    black_clock = after;
                    prev_black_think = think;
                    black_first = false;
                }
                board.apply_move(*resolved.move);
            }

            sqlite3_reset(game_stmt);
            sqlite3_bind_text(game_stmt, 1, game_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(game_stmt, 2, source_file_id);
            sqlite3_bind_text(game_stmt, 3, current.source_month.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(game_stmt, 4, current.source_game_index);
            sqlite3_bind_int(game_stmt, 5, *white_elo);
            sqlite3_bind_int(game_stmt, 6, *black_elo);
            sqlite3_bind_text(game_stmt, 7, tc_raw.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(game_stmt, 8, parsed_tc->initial_seconds);
            sqlite3_bind_int(game_stmt, 9, parsed_tc->increment_seconds);
            const std::string result = current.tags.count("Result") ? current.tags.at("Result") : "";
            sqlite3_bind_text(game_stmt, 10, result.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(game_stmt, 11, emitted > 0 ? 1 : 0);
            if (emitted > 0) sqlite3_bind_null(game_stmt, 12); else sqlite3_bind_text(game_stmt, 12, "no_valid_move_events", -1, SQLITE_STATIC);
            if (sqlite3_step(game_stmt) != SQLITE_DONE) throw std::runtime_error("game insert failed");

            if (emitted > 0) ++counters.games_accepted; else ++counters.games_rejected;
            if (options.max_games > 0 && counters.games_seen >= options.max_games) {
                // handled by caller loop condition via exception-free early exit marker
            }

            current = ParsedGame{};
                current.source_file = input.generic_string();
        };

        while (reader.getline(line)) {
            const std::string t = trim(line);
            if (t.empty()) {
                in_headers = false;
                continue;
            }
            if (t.front() == '[') {
                auto maybe_name = parse_tag_name(t);
                if (maybe_name.has_value() && *maybe_name == "Event" && saw_headers && !current.movetext.empty()) {
                    flush_game();
                }
                saw_headers = true;
                in_headers = true;
                const auto tag = parse_tag(t);
                if (tag.has_value()) current.tags[tag->first] = tag->second;
                continue;
            }
            if (!in_headers && saw_headers) {
                if (!current.movetext.empty()) current.movetext.push_back(' ');
                current.movetext += t;
            }
            if (options.max_games > 0 && counters.games_seen >= options.max_games) break;
        }
        flush_game();
        if (options.max_games > 0 && counters.games_seen >= options.max_games) break;
    }

    sqlite3_finalize(source_stmt);
    sqlite3_finalize(game_stmt);
    sqlite3_finalize(move_stmt);
    sqlite3_finalize(invalid_stmt);

    exec_sql(db, "COMMIT;");
    return counters;
}

}  // namespace otcb
