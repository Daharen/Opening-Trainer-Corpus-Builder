#include "otcb/practical_risk_screen.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/types.h>
#include <sys/wait.h>
#include <tuple>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>

#include <sqlite3.h>

#include "otcb/chess_board.hpp"
#include "otcb/chess_types.hpp"
#include "otcb/position_key.hpp"
#include "otcb/progress.hpp"
#include "otcb/rating_filter.hpp"
#include "otcb/san_replay.hpp"

namespace otcb {
namespace {

std::string trim(const std::string& in) {
    std::size_t s = 0;
    while (s < in.size() && std::isspace(static_cast<unsigned char>(in[s]))) ++s;
    std::size_t e = in.size();
    while (e > s && std::isspace(static_cast<unsigned char>(in[e - 1]))) --e;
    return in.substr(s, e - s);
}

std::string json_escape(const std::string& input) {
    std::string escaped;
    escaped.reserve(input.size());
    for (const char ch : input) {
        switch (ch) {
            case '\\': escaped += "\\\\"; break;
            case '"': escaped += "\\\""; break;
            case '\n': escaped += "\\n"; break;
            case '\r': escaped += "\\r"; break;
            case '\t': escaped += "\\t"; break;
            default: escaped += ch; break;
        }
    }
    return escaped;
}

bool is_move_number(const std::string& token) {
    if (token.empty()) return false;
    std::size_t i = 0;
    while (i < token.size() && std::isdigit(static_cast<unsigned char>(token[i]))) ++i;
    if (i == 0) return false;
    while (i < token.size() && token[i] == '.') ++i;
    return i == token.size();
}

bool is_result_token(const std::string& token) {
    return token == "1-0" || token == "0-1" || token == "1/2-1/2" || token == "*";
}

std::vector<std::string> tokenize_movetext(const std::string& movetext) {
    std::vector<std::string> out;
    std::string cur;
    bool in_comment = false;
    int variation_depth = 0;
    auto flush = [&]() {
        if (cur.empty() || variation_depth > 0) {
            cur.clear();
            return;
        }
        if (is_move_number(cur) || cur.front() == '$' || is_result_token(cur)) {
            cur.clear();
            return;
        }
        while (!cur.empty() && (cur.back() == '!' || cur.back() == '?')) cur.pop_back();
        if (!cur.empty()) out.push_back(cur);
        cur.clear();
    };
    for (char ch : movetext) {
        if (in_comment) {
            if (ch == '}') in_comment = false;
            continue;
        }
        if (ch == '{') {
            flush();
            in_comment = true;
            continue;
        }
        if (ch == '(') {
            flush();
            ++variation_depth;
            continue;
        }
        if (ch == ')') {
            flush();
            if (variation_depth > 0) --variation_depth;
            continue;
        }
        if (std::isspace(static_cast<unsigned char>(ch))) {
            flush();
            continue;
        }
        if (variation_depth == 0) cur.push_back(ch);
    }
    flush();
    return out;
}

std::optional<int> parse_int(const std::string& value) {
    if (value.empty()) return std::nullopt;
    try {
        size_t p = 0;
        int out = std::stoi(value, &p);
        if (p != value.size()) return std::nullopt;
        return out;
    } catch (...) {
        return std::nullopt;
    }
}

struct ParsedGame {
    std::map<std::string, std::string> tags;
    std::string movetext;
};

template <typename Callback>
void stream_games(const std::filesystem::path& input, ProgressReporter& progress, Callback&& callback) {
    std::ifstream in(input, std::ios::binary);
    if (!in) throw std::runtime_error("failed to open input pgn: " + input.string());
    ParsedGame current;
    bool in_moves = false;
    std::string line;
    std::uint64_t bytes = 0;
    while (std::getline(in, line)) {
        bytes += static_cast<std::uint64_t>(line.size() + 1);
        progress.update([&](ProgressSnapshot& s) { s.source_bytes_scanned = bytes; });
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (!line.empty() && line.front() == '[') {
            in_moves = false;
            const auto sp = line.find(' ');
            if (sp != std::string::npos && line.back() == ']' && sp + 2 < line.size() && line[sp + 1] == '"' && line[line.size() - 2] == '"') {
                current.tags[line.substr(1, sp - 1)] = line.substr(sp + 2, line.size() - sp - 4);
            }
            continue;
        }
        if (trim(line).empty()) {
            if (!current.movetext.empty() && !current.tags.empty()) {
                callback(current);
                current = ParsedGame{};
            }
            in_moves = true;
            continue;
        }
        if (in_moves || !current.tags.empty()) {
            if (!current.movetext.empty()) current.movetext.push_back(' ');
            current.movetext += trim(line);
            in_moves = true;
        }
    }
    if (!current.movetext.empty() && !current.tags.empty()) callback(current);
}

double score_for_side(const std::string& result, Color side) {
    if (result == "1/2-1/2") return 0.5;
    if (result == "1-0") return side == Color::White ? 1.0 : 0.0;
    if (result == "0-1") return side == Color::Black ? 1.0 : 0.0;
    return 0.5;
}

struct MoveAggregate {
    int support = 0;
    double score_sum = 0.0;
    double score_sq_sum = 0.0;
};

struct RootAggregate {
    int root_support = 0;
    std::map<std::string, MoveAggregate> moves;
};

std::pair<double, double> mean_sigma(const MoveAggregate& stats) {
    if (stats.support <= 0) return {0.0, 0.0};
    const double mu = stats.score_sum / static_cast<double>(stats.support);
    const double ex2 = stats.score_sq_sum / static_cast<double>(stats.support);
    const double var = std::max(0.0, ex2 - mu * mu);
    return {mu, std::sqrt(var)};
}

std::string key_to_fen(const std::string& key) {
    return key + " 0 1";
}

int cp_from_stockfish_score(const std::string& token_type, const int value) {
    if (token_type == "cp") return value;
    if (token_type == "mate") return value > 0 ? 100000 : -100000;
    return 0;
}

struct EvalInfo {
    int cp = 0;
    bool accepted = false;
    int loss_cp = 0;
    std::string reason;
};

class StockfishProcess {
public:
    StockfishProcess(const std::filesystem::path& engine_path, int hash_mb, int threads) {
        int to_child[2]{};
        int from_child[2]{};
        if (pipe(to_child) != 0 || pipe(from_child) != 0) {
            throw std::runtime_error("failed to create pipes for stockfish");
        }
        pid_ = fork();
        if (pid_ < 0) {
            throw std::runtime_error("failed to fork stockfish process");
        }
        if (pid_ == 0) {
            dup2(to_child[0], STDIN_FILENO);
            dup2(from_child[1], STDOUT_FILENO);
            dup2(from_child[1], STDERR_FILENO);
            close(to_child[0]); close(to_child[1]); close(from_child[0]); close(from_child[1]);
            execl(engine_path.c_str(), engine_path.c_str(), static_cast<char*>(nullptr));
            _exit(127);
        }
        close(to_child[0]);
        close(from_child[1]);
        in_ = fdopen(to_child[1], "w");
        out_ = fdopen(from_child[0], "r");
        if (!in_ || !out_) throw std::runtime_error("failed to open stockfish pipes");
        send("uci");
        wait_for("uciok");
        send("setoption name Hash value " + std::to_string(hash_mb));
        send("setoption name Threads value " + std::to_string(threads));
        send("isready");
        wait_for("readyok");
    }

    ~StockfishProcess() {
        if (in_) {
            send("quit");
            fclose(in_);
        }
        if (out_) fclose(out_);
        if (pid_ > 0) {
            int status = 0;
            waitpid(pid_, &status, 0);
        }
    }

    int evaluate_cp_for_fen(const std::string& fen, int movetime_ms) {
        send("position fen " + fen);
        return go_and_collect_cp(movetime_ms);
    }

    int evaluate_cp_for_fen_with_move(const std::string& fen, const std::string& move_uci, int movetime_ms) {
        send("position fen " + fen + " moves " + move_uci);
        return go_and_collect_cp(movetime_ms);
    }

private:
    int go_and_collect_cp(const int movetime_ms) {
        send("go movetime " + std::to_string(movetime_ms));
        int score_cp = 0;
        std::array<char, 4096> line{};
        while (fgets(line.data(), static_cast<int>(line.size()), out_) != nullptr) {
            const std::string s = trim(line.data());
            if (s.rfind("info ", 0) == 0) {
                auto cp = parse_score_from_info(s);
                if (cp.has_value()) score_cp = *cp;
            }
            if (s.rfind("bestmove ", 0) == 0) break;
        }
        return score_cp;
    }
    std::optional<int> parse_score_from_info(const std::string& info) {
        std::stringstream ss(info);
        std::string tok;
        while (ss >> tok) {
            if (tok == "score") {
                std::string t;
                int value = 0;
                if (!(ss >> t >> value)) return std::nullopt;
                return cp_from_stockfish_score(t, value);
            }
        }
        return std::nullopt;
    }

    void send(const std::string& line) {
        std::fputs((line + "\n").c_str(), in_);
        std::fflush(in_);
    }

    void wait_for(const std::string& marker) {
        std::array<char, 4096> line{};
        while (fgets(line.data(), static_cast<int>(line.size()), out_) != nullptr) {
            if (trim(line.data()) == marker) return;
        }
        throw std::runtime_error("stockfish handshake failed waiting for " + marker);
    }

    pid_t pid_ = -1;
    FILE* in_ = nullptr;
    FILE* out_ = nullptr;
};

class EngineEvalCache {
public:
    explicit EngineEvalCache(const std::filesystem::path& db_path) : db_path_(db_path) {
        if (sqlite3_open(db_path_.string().c_str(), &db_) != SQLITE_OK) {
            throw std::runtime_error("failed to open sqlite cache");
        }
        exec("CREATE TABLE IF NOT EXISTS eval_cache (cache_key TEXT PRIMARY KEY, cp INTEGER NOT NULL);");
    }

    ~EngineEvalCache() {
        if (db_) sqlite3_close(db_);
    }

    std::optional<int> get(const std::string& cache_key) {
        auto it = mem_.find(cache_key);
        if (it != mem_.end()) return it->second;
        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db_, "SELECT cp FROM eval_cache WHERE cache_key=?1;", -1, &stmt, nullptr) != SQLITE_OK) {
            return std::nullopt;
        }
        sqlite3_bind_text(stmt, 1, cache_key.c_str(), -1, SQLITE_TRANSIENT);
        std::optional<int> out;
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            out = sqlite3_column_int(stmt, 0);
            mem_[cache_key] = *out;
        }
        sqlite3_finalize(stmt);
        return out;
    }

    void put(const std::string& cache_key, int cp) {
        mem_[cache_key] = cp;
        sqlite3_stmt* stmt = nullptr;
        sqlite3_prepare_v2(db_, "INSERT OR REPLACE INTO eval_cache(cache_key, cp) VALUES(?1, ?2);", -1, &stmt, nullptr);
        sqlite3_bind_text(stmt, 1, cache_key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(stmt, 2, cp);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }

private:
    void exec(const std::string& sql) {
        char* err = nullptr;
        if (sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
            const std::string msg = err ? err : "sqlite error";
            sqlite3_free(err);
            throw std::runtime_error(msg);
        }
    }

    std::filesystem::path db_path_;
    sqlite3* db_ = nullptr;
    std::unordered_map<std::string, int> mem_;
};

std::string make_cache_key(const std::string& identity,
                           const std::string& position_key,
                           const std::string& move_uci,
                           int movetime_ms,
                           const PracticalRiskScreenOptions& opts) {
    std::ostringstream out;
    out << identity << "|" << position_key << "|" << move_uci << "|" << movetime_ms << "|"
        << opts.engine_accept_policy << "|" << opts.engine_max_loss_cp << "|" << opts.engine_reference_mode;
    return out.str();
}

struct CandidateEvalRow {
    std::string move;
    int support = 0;
    double mu = 0.0;
    double sigma = 0.0;
    double ceiling = 0.0;
    std::string engine_reason;
    bool is_engine_fail = false;
    bool ceiling_beats_baseline = false;
    bool final_pass = false;
    std::string final_reason;
};

}  // namespace

void print_practical_risk_screen_usage(const std::string& program_name) {
    std::cout
        << "Usage: " << program_name << " --input-pgn <path> --output-dir <dir> --artifact-id <id>\\n"
        << "       --min-rating <n> --max-rating <n> --rating-policy <both_in_band|average_in_band|white_in_band|black_in_band>\\n"
        << "       --retained-ply <n> --time-controls <tc1,tc2> --time-control-id <id> --initial-time-seconds <sec> --increment-seconds <sec> --time-format-label <label>\\n"
        << "       --engine-path <path> --engine-movetime-ms <n> --engine-hash-mb <n> --engine-threads <n>\\n";
}

PracticalRiskScreenOptions parse_practical_risk_screen_cli(int argc, char** argv) {
    PracticalRiskScreenOptions out;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto require_value = [&](const char* flag) -> std::string {
            if (i + 1 >= argc) throw std::runtime_error(std::string("missing value for ") + flag);
            return argv[++i];
        };
        if (arg == "--input-pgn") out.input_pgn = require_value("--input-pgn");
        else if (arg == "--output-dir") out.output_dir = require_value("--output-dir");
        else if (arg == "--artifact-id") out.artifact_id = require_value("--artifact-id");
        else if (arg == "--min-rating") out.min_rating = std::stoi(require_value("--min-rating"));
        else if (arg == "--max-rating") out.max_rating = std::stoi(require_value("--max-rating"));
        else if (arg == "--rating-policy") {
            const auto parsed = parse_rating_policy(require_value("--rating-policy"));
            if (!parsed.has_value()) throw std::runtime_error("invalid --rating-policy");
            out.rating_policy = *parsed;
        } else if (arg == "--retained-ply") out.retained_ply = std::stoi(require_value("--retained-ply"));
        else if (arg == "--time-controls") {
            std::stringstream ss(require_value("--time-controls"));
            std::string part;
            while (std::getline(ss, part, ',')) {
                part = trim(part);
                if (!part.empty()) out.time_controls.push_back(part);
            }
        } else if (arg == "--time-control-id") out.time_control_id = require_value("--time-control-id");
        else if (arg == "--initial-time-seconds") out.initial_time_seconds = std::stoi(require_value("--initial-time-seconds"));
        else if (arg == "--increment-seconds") out.increment_seconds = std::stoi(require_value("--increment-seconds"));
        else if (arg == "--time-format-label") out.time_format_label = require_value("--time-format-label");
        else if (arg == "--candidate-min-support") out.candidate_min_support = std::stoi(require_value("--candidate-min-support"));
        else if (arg == "--baseline-min-support") out.baseline_min_support = std::stoi(require_value("--baseline-min-support"));
        else if (arg == "--root-min-support") out.root_min_support = std::stoi(require_value("--root-min-support"));
        else if (arg == "--engine-path") out.engine_path = require_value("--engine-path");
        else if (arg == "--engine-movetime-ms") out.engine_movetime_ms = std::stoi(require_value("--engine-movetime-ms"));
        else if (arg == "--engine-hash-mb") out.engine_hash_mb = std::stoi(require_value("--engine-hash-mb"));
        else if (arg == "--engine-threads") out.engine_threads = std::stoi(require_value("--engine-threads"));
        else if (arg == "--baseline-prefix-limit") out.baseline_prefix_limit = std::stoi(require_value("--baseline-prefix-limit"));
        else if (arg == "--candidate-prefix-limit") out.candidate_prefix_limit = std::stoi(require_value("--candidate-prefix-limit"));
        else if (arg == "--emit-progress-log") out.emit_progress_log = true;
        else if (arg == "--emit-status-json") out.emit_status_json = true;
        else if (arg == "--heartbeat-seconds") out.heartbeat_seconds = std::stoi(require_value("--heartbeat-seconds"));
        else if (arg == "--quiet-progress") out.quiet_progress = true;
        else if (arg == "--engine-accept-policy") out.engine_accept_policy = require_value("--engine-accept-policy");
        else if (arg == "--engine-max-loss-cp") out.engine_max_loss_cp = std::stoi(require_value("--engine-max-loss-cp"));
        else if (arg == "--engine-reference-mode") out.engine_reference_mode = require_value("--engine-reference-mode");
        else if (arg == "--help" || arg == "-h") {
            print_practical_risk_screen_usage(argv[0]);
            std::exit(0);
        } else {
            throw std::runtime_error("unknown argument: " + arg);
        }
    }
    if (out.input_pgn.empty() || out.output_dir.empty() || out.artifact_id.empty() || out.engine_path.empty()) {
        throw std::runtime_error("missing required --input-pgn --output-dir --artifact-id --engine-path");
    }
    if (out.time_controls.empty()) throw std::runtime_error("--time-controls required");
    if (out.min_rating > out.max_rating) throw std::runtime_error("--min-rating must be <= --max-rating");
    return out;
}

int run_practical_risk_screen(const PracticalRiskScreenOptions& options) {
    const std::filesystem::path bundle_dir = options.output_dir / options.artifact_id;
    std::filesystem::create_directories(bundle_dir);
    std::filesystem::create_directories(bundle_dir / "audit");

    ProgressReporter progress(ProgressReporterOptions{options.quiet_progress, options.emit_progress_log, options.emit_status_json, options.heartbeat_seconds, bundle_dir});
    progress.start();

    const auto source_size = std::filesystem::exists(options.input_pgn) ? std::optional<std::uint64_t>(std::filesystem::file_size(options.input_pgn)) : std::nullopt;
    progress.stage_started(ProgressStage::Preflight, "preflight");
    progress.stage_completed("preflight complete");

    progress.stage_started(ProgressStage::ScanHeaders, "scan-headers", source_size);
    std::map<std::string, RootAggregate> roots;
    std::set<std::string> tc_allowed(options.time_controls.begin(), options.time_controls.end());
    int games_seen = 0;
    int games_scope = 0;
    int roots_observed = 0;
    int move_rows = 0;
    stream_games(options.input_pgn, progress, [&](const ParsedGame& game) {
        ++games_seen;
        progress.update([&](ProgressSnapshot& s) { s.games_scanned = games_seen; });
        const auto welo_it = game.tags.find("WhiteElo");
        const auto belo_it = game.tags.find("BlackElo");
        const auto tc_it = game.tags.find("TimeControl");
        const auto result_it = game.tags.find("Result");
        if (welo_it == game.tags.end() || belo_it == game.tags.end() || tc_it == game.tags.end() || result_it == game.tags.end()) return;
        const auto welo = parse_int(welo_it->second);
        const auto belo = parse_int(belo_it->second);
        if (!welo.has_value() || !belo.has_value()) return;
        if (!rating_policy_match(*welo, *belo, options.rating_policy, {EloRange{options.min_rating, options.max_rating}})) return;
        if (tc_allowed.find(tc_it->second) == tc_allowed.end()) return;

        ++games_scope;
        auto san_moves = tokenize_movetext(game.movetext);
        ChessBoard board;
        for (int ply = 0; ply < static_cast<int>(san_moves.size()) && ply < options.retained_ply; ++ply) {
            const std::string position_key = make_position_key(board, PositionKeyFormat::FenNormalized);
            const auto resolved = resolve_san_move(board, san_moves[static_cast<std::size_t>(ply)]);
            if (!resolved.success || !resolved.move.has_value()) break;
            const std::string move_uci = move_to_uci(*resolved.move);
            RootAggregate& root = roots[position_key];
            ++root.root_support;
            MoveAggregate& ma = root.moves[move_uci];
            ++ma.support;
            const double score = score_for_side(result_it->second, board.side_to_move());
            ma.score_sum += score;
            ma.score_sq_sum += score * score;
            if (ma.support == 1) ++move_rows;
            board.apply_move(*resolved.move);
        }
        progress.update([&](ProgressSnapshot& s) {
            s.games_accepted = games_scope;
            s.games_rejected = games_seen - games_scope;
            s.aggregated_positions = static_cast<int>(roots.size());
            s.aggregate_move_entries = move_rows;
        });
    });
    roots_observed = static_cast<int>(roots.size());
    progress.stage_completed("scan headers complete");

    progress.stage_started(ProgressStage::AggregateEmpiricalScreen, "aggregate-empirical-screen");

    std::ofstream report(bundle_dir / "root_screen_report.jsonl");
    if (!report) throw std::runtime_error("failed to create root_screen_report.jsonl");

    struct RootEvalWork {
        std::string key;
        std::vector<std::pair<std::string, MoveAggregate>> popularity;
        std::vector<std::pair<std::string, MoveAggregate>> candidate_survivors;
        std::string baseline_move;
        MoveAggregate baseline_stats;
        double baseline_ceiling = 0.0;
    };
    std::vector<RootEvalWork> engine_roots;

    int discarded_low_support = 0;
    int discarded_no_candidate_support = 0;
    int discarded_no_candidate_empirical = 0;

    for (const auto& [root_key, root] : roots) {
        std::vector<std::pair<std::string, MoveAggregate>> popularity(root.moves.begin(), root.moves.end());
        std::sort(popularity.begin(), popularity.end(), [](const auto& a, const auto& b) {
            if (a.second.support != b.second.support) return a.second.support > b.second.support;
            return a.first < b.first;
        });
        if (root.root_support < options.root_min_support) {
            ++discarded_low_support;
            report << "{\"position_key\":\"" << json_escape(root_key) << "\",\"root_support\":" << root.root_support
                   << ",\"reason\":\"discard_root_low_support\"}\n";
            continue;
        }
        if (popularity.empty() || popularity.front().second.support < options.baseline_min_support) {
            ++discarded_no_candidate_support;
            report << "{\"position_key\":\"" << json_escape(root_key) << "\",\"root_support\":" << root.root_support
                   << ",\"reason\":\"discard_no_candidate_support\"}\n";
            continue;
        }

        const auto [base_mu, base_sigma] = mean_sigma(popularity.front().second);
        const double base_ceiling = base_mu + base_sigma;

        std::vector<std::pair<std::string, MoveAggregate>> candidate_support;
        for (std::size_t i = 1; i < popularity.size(); ++i) {
            if (popularity[i].second.support >= options.candidate_min_support) candidate_support.push_back(popularity[i]);
        }
        if (candidate_support.empty()) {
            ++discarded_no_candidate_support;
            report << "{\"position_key\":\"" << json_escape(root_key) << "\",\"root_support\":" << root.root_support
                   << ",\"most_common_raw_baseline_move\":\"" << popularity.front().first
                   << "\",\"reason\":\"discard_no_candidate_support\"}\n";
            continue;
        }

        std::vector<std::pair<std::string, MoveAggregate>> survivors;
        for (const auto& cand : candidate_support) {
            const auto [mu, sigma] = mean_sigma(cand.second);
            if (mu + sigma > base_ceiling) survivors.push_back(cand);
        }
        if (survivors.empty()) {
            ++discarded_no_candidate_empirical;
            report << "{\"position_key\":\"" << json_escape(root_key) << "\",\"root_support\":" << root.root_support
                   << ",\"most_common_raw_baseline_move\":\"" << popularity.front().first
                   << "\",\"reason\":\"discard_no_candidate_empirical_survivors\"}\n";
            continue;
        }
        engine_roots.push_back(RootEvalWork{root_key, popularity, survivors, popularity.front().first, popularity.front().second, base_ceiling});
    }

    progress.update([&](ProgressSnapshot& s) {
        s.risky_positions_considered = roots_observed;
        s.risky_unresolved_rows = static_cast<int>(engine_roots.size());
        s.risky_rejected_rows = discarded_low_support + discarded_no_candidate_support + discarded_no_candidate_empirical;
    });
    progress.stage_completed("aggregate empirical screen complete");

    progress.stage_started(ProgressStage::EngineBaselineScreen, "engine-baseline-screen");
    StockfishProcess engine(options.engine_path, options.engine_hash_mb, options.engine_threads);
    EngineEvalCache cache(bundle_dir / "engine_eval_cache.sqlite");
    const std::string engine_identity = options.engine_path.string();

    int baseline_evals = 0;
    int candidate_evals = 0;
    int cache_hits = 0;
    int cache_misses = 0;
    int roots_no_baseline = 0;
    int candidates_passed = 0;
    int candidates_rejected = 0;

    std::ofstream audit(bundle_dir / "audit" / "sample_roots.jsonl");

    for (const auto& work : engine_roots) {
        progress.note_event("engine root " + work.key);
        const std::string root_fen = key_to_fen(work.key);
        const std::string root_key_best = make_cache_key(engine_identity, work.key, "__root__", options.engine_movetime_ms, options);
        int root_best_cp = 0;
        if (auto c = cache.get(root_key_best); c.has_value()) {
            root_best_cp = *c;
            ++cache_hits;
        } else {
            root_best_cp = engine.evaluate_cp_for_fen(root_fen, options.engine_movetime_ms);
            cache.put(root_key_best, root_best_cp);
            ++cache_misses;
        }

        std::optional<std::pair<std::string, MoveAggregate>> accepted_baseline;
        EvalInfo baseline_eval;
        int baseline_checked = 0;
        for (const auto& entry : work.popularity) {
            if (entry.second.support < options.baseline_min_support) continue;
            if (baseline_checked >= options.baseline_prefix_limit) break;
            ++baseline_checked;
            ++baseline_evals;
            const std::string cache_key = make_cache_key(engine_identity, work.key, entry.first, options.engine_movetime_ms, options);
            int move_cp = 0;
            if (auto c = cache.get(cache_key); c.has_value()) {
                move_cp = *c;
                ++cache_hits;
            } else {
                move_cp = -engine.evaluate_cp_for_fen_with_move(root_fen, entry.first, options.engine_movetime_ms);
                cache.put(cache_key, move_cp);
                ++cache_misses;
            }
            EvalInfo eval;
            eval.cp = move_cp;
            eval.loss_cp = root_best_cp - move_cp;
            eval.accepted = eval.loss_cp <= options.engine_max_loss_cp;
            eval.reason = eval.accepted ? "engine_accept_loss_within_threshold" : "engine_reject_loss_above_threshold";
            if (eval.accepted) {
                accepted_baseline = entry;
                baseline_eval = eval;
                break;
            }
        }

        if (!accepted_baseline.has_value()) {
            ++roots_no_baseline;
            report << "{\"position_key\":\"" << json_escape(work.key) << "\",\"root_support\":" << roots[work.key].root_support
                   << ",\"reason\":\"discard_no_engine_accepted_baseline\"}\n";
            continue;
        }

        const auto [b_mu, b_sigma] = mean_sigma(accepted_baseline->second);
        const double baseline_ceiling = b_mu + b_sigma;

        std::vector<CandidateEvalRow> rows;
        int candidate_checked = 0;
        for (const auto& cand : work.candidate_survivors) {
            if (candidate_checked >= options.candidate_prefix_limit) break;
            ++candidate_checked;
            ++candidate_evals;
            const auto [c_mu, c_sigma] = mean_sigma(cand.second);
            CandidateEvalRow row;
            row.move = cand.first;
            row.support = cand.second.support;
            row.mu = c_mu;
            row.sigma = c_sigma;
            row.ceiling = c_mu + c_sigma;
            row.ceiling_beats_baseline = row.ceiling > baseline_ceiling;

            const std::string cache_key = make_cache_key(engine_identity, work.key, cand.first, options.engine_movetime_ms, options);
            int move_cp = 0;
            if (auto c = cache.get(cache_key); c.has_value()) {
                move_cp = *c;
                ++cache_hits;
            } else {
                move_cp = -engine.evaluate_cp_for_fen_with_move(root_fen, cand.first, options.engine_movetime_ms);
                cache.put(cache_key, move_cp);
                ++cache_misses;
            }
            const int loss_cp = root_best_cp - move_cp;
            row.is_engine_fail = loss_cp > options.engine_max_loss_cp;
            row.engine_reason = row.is_engine_fail ? "engine_fail_loss_above_threshold" : "engine_not_fail_loss_within_threshold";

            if (!row.is_engine_fail) {
                row.final_reason = "discard_candidate_engine_not_fail";
                ++candidates_rejected;
            } else if (!row.ceiling_beats_baseline) {
                row.final_reason = "discard_candidate_ceiling_not_above_baseline";
                ++candidates_rejected;
            } else {
                row.final_reason = "pass_candidate_ceiling_above_engine_accepted_baseline";
                row.final_pass = true;
                ++candidates_passed;
            }
            rows.push_back(row);
        }

        std::ostringstream line;
        line << "{\"position_key\":\"" << json_escape(work.key) << "\"";
        line << ",\"root_support\":" << roots[work.key].root_support;
        line << ",\"all_observed_moves\":[";
        bool first = true;
        for (const auto& m : work.popularity) {
            const auto [mu, sigma] = mean_sigma(m.second);
            if (!first) line << ',';
            first = false;
            line << "{\"move\":\"" << m.first << "\",\"support\":" << m.second.support
                 << ",\"mu_empirical\":" << std::fixed << std::setprecision(6) << mu
                 << ",\"sigma_empirical\":" << sigma
                 << ",\"ceiling_empirical\":" << (mu + sigma) << "}";
        }
        line << "]";
        const auto [raw_mu, raw_sigma] = mean_sigma(work.baseline_stats);
        line << ",\"most_common_raw_baseline_move\":\"" << work.baseline_move << "\"";
        line << ",\"most_common_raw_baseline_support\":" << work.baseline_stats.support;
        line << ",\"most_common_raw_baseline_ceiling\":" << (raw_mu + raw_sigma);
        line << ",\"surviving_candidates_after_empirical_filter\":[";
        for (std::size_t i = 0; i < work.candidate_survivors.size(); ++i) {
            if (i) line << ',';
            line << '"' << work.candidate_survivors[i].first << '"';
        }
        line << "]";
        line << ",\"accepted_baseline_move\":\"" << accepted_baseline->first << "\"";
        line << ",\"accepted_baseline_support\":" << accepted_baseline->second.support;
        line << ",\"accepted_baseline_engine_reason\":\"" << baseline_eval.reason << "\"";
        line << ",\"accepted_baseline_mu_empirical\":" << b_mu;
        line << ",\"accepted_baseline_sigma_empirical\":" << b_sigma;
        line << ",\"accepted_baseline_ceiling_empirical\":" << baseline_ceiling;
        line << ",\"candidate_engine_rows\":[";
        for (std::size_t i = 0; i < rows.size(); ++i) {
            const auto& row = rows[i];
            if (i) line << ',';
            line << "{\"candidate_move\":\"" << row.move << "\",\"candidate_support\":" << row.support
                 << ",\"candidate_mu_empirical\":" << row.mu
                 << ",\"candidate_sigma_empirical\":" << row.sigma
                 << ",\"candidate_ceiling_empirical\":" << row.ceiling
                 << ",\"candidate_engine_reason\":\"" << row.engine_reason
                 << "\",\"candidate_is_engine_fail\":" << (row.is_engine_fail ? "true" : "false")
                 << ",\"baseline_ceiling_empirical\":" << baseline_ceiling
                 << ",\"candidate_ceiling_beats_baseline\":" << (row.ceiling_beats_baseline ? "true" : "false")
                 << ",\"final_pass\":" << (row.final_pass ? "true" : "false")
                 << ",\"final_reason_code\":\"" << row.final_reason << "\"}";
        }
        line << "]}";
        report << line.str() << "\n";
        audit << line.str() << "\n";

        progress.update([&](ProgressSnapshot& s) {
            s.risky_candidates_evaluated = candidate_evals;
            s.risky_admitted_rows = candidates_passed;
            s.risky_rejected_rows = candidates_rejected;
            s.risky_unresolved_rows = static_cast<int>(engine_roots.size()) - roots_no_baseline;
            s.risky_candidate_fails_considered = baseline_evals;
        });
    }
    progress.stage_completed("engine baseline screen complete");

    progress.stage_started(ProgressStage::WriteArtifacts, "write-artifacts");
    {
        std::ofstream summary(bundle_dir / "build_summary.txt");
        summary << "practical risk screening complete\n";
        summary << "roots_observed=" << roots_observed << "\n";
        summary << "roots_engine_stage=" << engine_roots.size() << "\n";
        summary << "baseline_evals=" << baseline_evals << "\n";
        summary << "candidate_evals=" << candidate_evals << "\n";
        summary << "cache_hits=" << cache_hits << "\n";
        summary << "cache_misses=" << cache_misses << "\n";
    }
    {
        std::ofstream summary(bundle_dir / "screen_summary.json");
        summary << "{\n";
        summary << "  \"artifact_role\": \"practical_risk_screening\",\n";
        summary << "  \"roots_observed\": " << roots_observed << ",\n";
        summary << "  \"roots_surviving_empirical\": " << engine_roots.size() << ",\n";
        summary << "  \"roots_discarded_no_engine_accepted_baseline\": " << roots_no_baseline << ",\n";
        summary << "  \"candidates_passed\": " << candidates_passed << ",\n";
        summary << "  \"candidates_rejected\": " << candidates_rejected << ",\n";
        summary << "  \"baseline_engine_evaluations\": " << baseline_evals << ",\n";
        summary << "  \"candidate_engine_evaluations\": " << candidate_evals << ",\n";
        summary << "  \"cache_hits\": " << cache_hits << ",\n";
        summary << "  \"cache_misses\": " << cache_misses << "\n";
        summary << "}\n";
    }
    {
        std::ofstream manifest(bundle_dir / "manifest.json");
        manifest << "{\n";
        manifest << "  \"artifact_id\": \"" << json_escape(options.artifact_id) << "\",\n";
        manifest << "  \"artifact_role\": \"practical_risk_screening\",\n";
        manifest << "  \"stockfish_used\": true,\n";
        manifest << "  \"external_book_dependency_used\": false,\n";
        manifest << "  \"mean_penalty_veto_used\": false,\n";
        manifest << "  \"raw_empirical_ceiling_rule_used\": true,\n";
        manifest << "  \"evaluation_policy\": {\n";
        manifest << "    \"engine_accept_policy\": \"" << json_escape(options.engine_accept_policy) << "\",\n";
        manifest << "    \"engine_max_loss_cp\": " << options.engine_max_loss_cp << ",\n";
        manifest << "    \"engine_reference_mode\": \"" << json_escape(options.engine_reference_mode) << "\"\n";
        manifest << "  },\n";
        manifest << "  \"thresholds\": {\n";
        manifest << "    \"candidate_min_support\": " << options.candidate_min_support << ",\n";
        manifest << "    \"baseline_min_support\": " << options.baseline_min_support << ",\n";
        manifest << "    \"root_min_support\": " << options.root_min_support << ",\n";
        manifest << "    \"baseline_prefix_limit\": " << options.baseline_prefix_limit << ",\n";
        manifest << "    \"candidate_prefix_limit\": " << options.candidate_prefix_limit << "\n";
        manifest << "  },\n";
        manifest << "  \"output_files\": {\n";
        manifest << "    \"manifest\": \"manifest.json\",\n";
        manifest << "    \"build_summary\": \"build_summary.txt\",\n";
        manifest << "    \"root_screen_report\": \"root_screen_report.jsonl\",\n";
        manifest << "    \"screen_summary\": \"screen_summary.json\",\n";
        manifest << "    \"engine_eval_cache\": \"engine_eval_cache.sqlite\"\n";
        manifest << "  }\n";
        manifest << "}\n";
    }
    progress.stage_completed("artifacts written");

    progress.stage_started(ProgressStage::Finalize, "finalize");
    progress.update([](ProgressSnapshot& s) {
        s.risky_estimated_remaining_work = 0;
        s.stage_active = false;
    });
    progress.stage_completed("done");
    progress.finish();

    return 0;
}

}  // namespace otcb
