#include "otcb/practical_risk_screen.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <csignal>
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
#include <sys/types.h>
#include <sys/wait.h>
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

void stream_games(const std::filesystem::path& pgn_path,
                  ProgressReporter& progress,
                  const std::function<void(const ParsedGame&)>& callback) {
    std::ifstream in(pgn_path);
    if (!in) throw std::runtime_error("failed to open input PGN: " + pgn_path.string());

    ParsedGame current;
    std::string line;
    bool in_moves = false;
    while (std::getline(in, line)) {
        progress.update([&](ProgressSnapshot& s) {
            s.source_bytes_scanned = static_cast<std::uint64_t>(in.tellg() < 0 ? s.source_bytes_scanned : static_cast<std::uint64_t>(in.tellg()));
            if (s.source_file_size && *s.source_file_size > 0) {
                s.percent_complete = std::min(100.0, 100.0 * static_cast<double>(s.source_bytes_scanned) / static_cast<double>(*s.source_file_size));
            }
        });

        const std::string t = trim(line);
        if (t.empty()) {
            if (!current.movetext.empty() && !current.tags.empty()) {
                callback(current);
                current = ParsedGame{};
                in_moves = false;
            }
            continue;
        }
        if (!t.empty() && t.front() == '[' && t.back() == ']') {
            const auto sp = t.find(' ');
            if (sp != std::string::npos && sp + 2 < t.size() && t[sp + 1] == '"' && t[t.size() - 2] == '"') {
                current.tags[t.substr(1, sp - 1)] = t.substr(sp + 2, t.size() - sp - 4);
            }
            continue;
        }
        if (in_moves || !current.tags.empty()) {
            if (!current.movetext.empty()) current.movetext.push_back(' ');
            current.movetext += t;
            in_moves = true;
        }
    }
    if (!current.movetext.empty() && !current.tags.empty()) callback(current);
}

double score_for_side(const std::string& result, const Color mover_side) {
    if (result == "1/2-1/2") return 0.5;
    if (result == "1-0") return mover_side == Color::White ? 1.0 : 0.0;
    if (result == "0-1") return mover_side == Color::Black ? 1.0 : 0.0;
    return 0.5;
}

struct MoveStats {
    int support = 0;
    double score_sum = 0.0;
    double score_sq_sum = 0.0;
};

struct RootStats {
    int root_support = 0;
    std::string position_key;
    std::string root_fen;
    std::map<std::string, MoveStats> move_stats;
};

struct EmpiricalMoments {
    double mu = 0.0;
    double sigma = 0.0;
    double ceiling = 0.0;
};

EmpiricalMoments moments(const MoveStats& stats) {
    if (stats.support <= 0) return {};
    const double n = static_cast<double>(stats.support);
    const double mu = stats.score_sum / n;
    const double ex2 = stats.score_sq_sum / n;
    const double var = std::max(0.0, ex2 - mu * mu);
    const double sigma = std::sqrt(var);
    return {mu, sigma, mu + sigma};
}

class UciEngine {
public:
    explicit UciEngine(const PracticalRiskScreenOptions& options) : options_(options) {}
    ~UciEngine() { shutdown(); }

    void start() {
        int to_child[2];
        int from_child[2];
        if (pipe(to_child) != 0 || pipe(from_child) != 0) throw std::runtime_error("failed to create pipes");
        child_pid_ = fork();
        if (child_pid_ < 0) throw std::runtime_error("failed to fork engine process");
        if (child_pid_ == 0) {
            dup2(to_child[0], STDIN_FILENO);
            dup2(from_child[1], STDOUT_FILENO);
            dup2(from_child[1], STDERR_FILENO);
            close(to_child[1]);
            close(from_child[0]);
            execl(options_.engine_path.c_str(), options_.engine_path.c_str(), static_cast<char*>(nullptr));
            std::exit(127);
        }
        close(to_child[0]);
        close(from_child[1]);
        in_ = fdopen(to_child[1], "w");
        out_ = fdopen(from_child[0], "r");
        if (!in_ || !out_) throw std::runtime_error("failed to connect stdio for engine");
        setvbuf(in_, nullptr, _IOLBF, 0);

        send("uci");
        std::string line;
        while (read_line(line)) {
            if (line.rfind("id name ", 0) == 0) engine_id_ = trim(line.substr(8));
            if (line == "uciok") break;
        }
        send("setoption name Hash value " + std::to_string(options_.engine_hash_mb));
        send("setoption name Threads value " + std::to_string(options_.engine_threads));
        send("isready");
        while (read_line(line)) {
            if (line == "readyok") break;
        }
    }

    std::string engine_id() const { return engine_id_.empty() ? "unknown-engine" : engine_id_; }

    int eval_root_cp(const std::string& fen) {
        send("position fen " + fen);
        send("go movetime " + std::to_string(options_.engine_movetime_ms));
        return read_score_cp_until_bestmove();
    }

    int eval_move_cp_for_mover(const std::string& fen, const std::string& move_uci) {
        const bool mover_white = fen.find(" w ") != std::string::npos;
        send("position fen " + fen + " moves " + move_uci);
        send("go movetime " + std::to_string(options_.engine_movetime_ms));
        const int cp_side_to_move = read_score_cp_until_bestmove();
        // after applying one move, side to move has flipped.
        const bool post_side_white = !mover_white;
        const int cp_for_mover = post_side_white == mover_white ? cp_side_to_move : -cp_side_to_move;
        return cp_for_mover;
    }

private:
    void send(const std::string& cmd) {
        if (std::fprintf(in_, "%s\n", cmd.c_str()) < 0) throw std::runtime_error("failed to write to engine");
        std::fflush(in_);
    }

    bool read_line(std::string& out_line) {
        std::array<char, 4096> buf{};
        if (!std::fgets(buf.data(), static_cast<int>(buf.size()), out_)) return false;
        out_line = trim(buf.data());
        return true;
    }

    int read_score_cp_until_bestmove() {
        std::string line;
        int last_cp = 0;
        while (read_line(line)) {
            const auto pos = line.find(" score cp ");
            if (pos != std::string::npos) {
                std::stringstream ss(line.substr(pos + 10));
                int cp = 0;
                ss >> cp;
                last_cp = cp;
            }
            if (line.rfind("bestmove ", 0) == 0) return last_cp;
        }
        throw std::runtime_error("engine stream ended before bestmove");
    }

    void shutdown() {
        if (in_) {
            std::fprintf(in_, "quit\n");
            std::fflush(in_);
            fclose(in_);
            in_ = nullptr;
        }
        if (out_) {
            fclose(out_);
            out_ = nullptr;
        }
        if (child_pid_ > 0) {
            int status = 0;
            waitpid(child_pid_, &status, 0);
            child_pid_ = -1;
        }
    }

    PracticalRiskScreenOptions options_;
    pid_t child_pid_ = -1;
    FILE* in_ = nullptr;
    FILE* out_ = nullptr;
    std::string engine_id_;
};

struct CacheEntry {
    int cp = 0;
};

class EngineEvalCache {
public:
    explicit EngineEvalCache(const std::filesystem::path& path) : path_(path) {
        if (sqlite3_open(path_.string().c_str(), &db_) != SQLITE_OK) throw std::runtime_error("failed to open sqlite cache");
        const char* ddl = "CREATE TABLE IF NOT EXISTS engine_eval_cache (cache_key TEXT PRIMARY KEY, cp INTEGER NOT NULL);";
        char* err = nullptr;
        if (sqlite3_exec(db_, ddl, nullptr, nullptr, &err) != SQLITE_OK) {
            std::string message = err ? err : "unknown sqlite error";
            sqlite3_free(err);
            throw std::runtime_error(message);
        }
    }

    ~EngineEvalCache() {
        if (db_) sqlite3_close(db_);
    }

    std::optional<CacheEntry> get(const std::string& key) {
        auto it = mem_.find(key);
        if (it != mem_.end()) return it->second;

        sqlite3_stmt* stmt = nullptr;
        const char* sql = "SELECT cp FROM engine_eval_cache WHERE cache_key = ?1";
        if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) != SQLITE_OK) return std::nullopt;
        sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_TRANSIENT);
        std::optional<CacheEntry> out;
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            out = CacheEntry{sqlite3_column_int(stmt, 0)};
            mem_.emplace(key, *out);
        }
        sqlite3_finalize(stmt);
        return out;
    }

    void put(const std::string& key, const CacheEntry entry) {
        mem_[key] = entry;
        sqlite3_stmt* stmt = nullptr;
        const char* sql = "INSERT OR REPLACE INTO engine_eval_cache(cache_key, cp) VALUES(?1, ?2)";
        if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) != SQLITE_OK) return;
        sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(stmt, 2, entry.cp);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }

private:
    std::filesystem::path path_;
    sqlite3* db_ = nullptr;
    std::unordered_map<std::string, CacheEntry> mem_;
};

struct CandidateReport {
    std::string candidate_move;
    int candidate_support = 0;
    EmpiricalMoments candidate_moments;
    std::string candidate_engine_reason;
    bool candidate_is_engine_fail = false;
    double baseline_ceiling_empirical = 0.0;
    bool candidate_ceiling_beats_baseline = false;
    bool final_pass = false;
    std::string final_reason_code;
};

}  // namespace

void print_practical_risk_screen_usage(const std::string& program_name) {
    std::cout << "Usage: " << program_name << " --input-pgn <path> --output-dir <dir> --artifact-id <id> ...\n";
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
    if (out.input_pgn.empty() || out.output_dir.empty() || out.artifact_id.empty() || out.engine_path.empty() ||
        out.time_control_id.empty() || out.time_format_label.empty()) {
        throw std::runtime_error("missing required args");
    }
    if (out.min_rating > out.max_rating) throw std::runtime_error("--min-rating must be <= --max-rating");
    if (out.retained_ply <= 0) throw std::runtime_error("--retained-ply must be > 0");
    if (out.time_controls.empty()) throw std::runtime_error("--time-controls required");
    return out;
}

int run_practical_risk_screen(const PracticalRiskScreenOptions& options) {
    const std::filesystem::path bundle_dir = options.output_dir / options.artifact_id;
    std::filesystem::create_directories(bundle_dir);

    ProgressReporter progress(ProgressReporterOptions{options.quiet_progress, options.emit_progress_log, options.emit_status_json, options.heartbeat_seconds, bundle_dir});
    progress.start();

    progress.stage_started(ProgressStage::Preflight, "preflight practical risk screen");
    progress.stage_completed("preflight complete");

    const auto source_size = std::filesystem::exists(options.input_pgn) ? std::optional<std::uint64_t>(std::filesystem::file_size(options.input_pgn)) : std::nullopt;
    progress.stage_started(ProgressStage::ScanHeaders, "scan-headers", source_size);

    std::set<std::string> tc_allowed(options.time_controls.begin(), options.time_controls.end());
    std::map<std::string, RootStats> roots;
    int games_seen = 0;
    int games_scope = 0;
    std::uint64_t move_rows_accum = 0;

    stream_games(options.input_pgn, progress, [&](const ParsedGame& game) {
        ++games_seen;
        const auto welo_it = game.tags.find("WhiteElo");
        const auto belo_it = game.tags.find("BlackElo");
        const auto tc_it = game.tags.find("TimeControl");
        const auto result_it = game.tags.find("Result");
        if (welo_it == game.tags.end() || belo_it == game.tags.end() || tc_it == game.tags.end() || result_it == game.tags.end()) {
            progress.update([&](ProgressSnapshot& s) { s.games_scanned = games_seen; s.games_rejected = games_seen - games_scope; });
            return;
        }
        const auto welo = parse_int(welo_it->second);
        const auto belo = parse_int(belo_it->second);
        if (!welo || !belo) return;
        if (!rating_policy_match(*welo, *belo, options.rating_policy, {EloRange{options.min_rating, options.max_rating}})) return;
        if (tc_allowed.find(tc_it->second) == tc_allowed.end()) return;

        ++games_scope;
        auto tokens = tokenize_movetext(game.movetext);
        ChessBoard board;
        for (std::size_t ply = 0; ply < tokens.size() && static_cast<int>(ply) < options.retained_ply; ++ply) {
            const std::string root_key = make_position_key(board, PositionKeyFormat::FenNormalized);
            RootStats& root = roots[root_key];
            root.position_key = root_key;
            if (root.root_fen.empty()) root.root_fen = board.to_fen();

            const auto resolved = resolve_san_move(board, tokens[ply]);
            if (!resolved.success || !resolved.move.has_value()) break;
            const std::string move_uci = move_to_uci(*resolved.move);
            const double score = score_for_side(result_it->second, board.side_to_move());
            ++root.root_support;
            MoveStats& ms = root.move_stats[move_uci];
            ++ms.support;
            ms.score_sum += score;
            ms.score_sq_sum += score * score;
            board.apply_move(*resolved.move);
        }

        move_rows_accum = 0;
        for (const auto& [k, r] : roots) {
            (void)k;
            move_rows_accum += r.move_stats.size();
        }
        progress.update([&](ProgressSnapshot& s) {
            s.games_scanned = games_seen;
            s.games_accepted = games_scope;
            s.games_rejected = games_seen - games_scope;
            s.aggregated_positions = static_cast<int>(roots.size());
            s.aggregate_move_entries = static_cast<int>(move_rows_accum);
            s.probe_nodes_built = static_cast<int>(roots.size());
        });
    });
    progress.stage_completed("scan-headers complete");

    progress.stage_started(ProgressStage::AggregateEmpiricalScreen, "aggregate-empirical-screen");

    std::ofstream root_report(bundle_dir / "root_screen_report.jsonl");

    struct SurvivorRoot {
        const RootStats* root = nullptr;
        std::string most_common_raw_baseline;
        int most_common_raw_baseline_support = 0;
        EmpiricalMoments most_common_raw_baseline_moments;
        std::vector<std::string> survivor_candidates;
    };

    std::vector<SurvivorRoot> survivor_roots;
    int roots_discarded_support = 0;
    int roots_discarded_no_candidate_support = 0;
    int roots_discarded_no_empirical_survivor = 0;

    for (auto& [root_key, root] : roots) {
        (void)root_key;
        std::vector<std::pair<std::string, MoveStats>> moves(root.move_stats.begin(), root.move_stats.end());
        std::sort(moves.begin(), moves.end(), [](const auto& a, const auto& b) {
            if (a.second.support != b.second.support) return a.second.support > b.second.support;
            return a.first < b.first;
        });

        std::string discard_reason;
        std::string baseline_move;
        int baseline_support = 0;
        EmpiricalMoments baseline_m;
        std::vector<std::string> survivor_candidates;

        if (root.root_support < options.root_min_support) {
            discard_reason = "discard_root_low_support";
            ++roots_discarded_support;
        } else if (moves.empty() || moves.front().second.support < options.baseline_min_support) {
            discard_reason = "discard_no_candidate_support";
            ++roots_discarded_no_candidate_support;
        } else {
            baseline_move = moves.front().first;
            baseline_support = moves.front().second.support;
            baseline_m = moments(moves.front().second);
            bool has_candidate_support = false;
            for (const auto& [move, ms] : moves) {
                if (move == baseline_move) continue;
                if (ms.support >= options.candidate_min_support) {
                    has_candidate_support = true;
                    if (moments(ms).ceiling > baseline_m.ceiling) {
                        survivor_candidates.push_back(move);
                    }
                }
            }
            if (!has_candidate_support) {
                discard_reason = "discard_no_candidate_support";
                ++roots_discarded_no_candidate_support;
            } else if (survivor_candidates.empty()) {
                discard_reason = "discard_no_candidate_empirical_survivors";
                ++roots_discarded_no_empirical_survivor;
            }
        }

        root_report << "{\"position_key\":\"" << json_escape(root.position_key) << "\",";
        root_report << "\"root_support\":" << root.root_support << ",";
        root_report << "\"all_observed_moves\":[";
        for (std::size_t i = 0; i < moves.size(); ++i) {
            const auto& [move, ms] = moves[i];
            const auto m = moments(ms);
            if (i) root_report << ',';
            root_report << "{\"move\":\"" << move << "\",\"support\":" << ms.support
                        << ",\"mu_empirical\":" << std::fixed << std::setprecision(6) << m.mu
                        << ",\"sigma_empirical\":" << m.sigma
                        << ",\"ceiling_empirical\":" << m.ceiling << "}";
        }
        root_report << "],";
        root_report << "\"most_common_raw_baseline_move\":\"" << json_escape(baseline_move) << "\",";
        root_report << "\"most_common_raw_baseline_support\":" << baseline_support << ",";
        root_report << "\"most_common_raw_baseline_ceiling\":" << baseline_m.ceiling << ",";
        root_report << "\"surviving_candidates_after_empirical_filter\":[";
        for (std::size_t i = 0; i < survivor_candidates.size(); ++i) {
            if (i) root_report << ',';
            root_report << "\"" << survivor_candidates[i] << "\"";
        }
        root_report << "],";
        root_report << "\"discard_reason\":\"" << discard_reason << "\"";

        if (discard_reason.empty()) {
            survivor_roots.push_back(SurvivorRoot{&root, baseline_move, baseline_support, baseline_m, survivor_candidates});
        }
        root_report << "}\n";
    }

    progress.stage_completed("aggregate-empirical-screen complete");

    progress.stage_started(ProgressStage::EngineBaselineScreen, "engine-baseline-screen");
    UciEngine engine(options);
    engine.start();
    EngineEvalCache cache(bundle_dir / "engine_eval_cache.sqlite");

    int roots_evaluated = 0;
    int baseline_evals = 0;
    int candidate_evals = 0;
    int cache_hits = 0;
    int cache_misses = 0;
    int roots_discarded_no_baseline = 0;
    int candidates_passed = 0;
    int candidates_rejected = 0;

    std::vector<std::string> final_root_lines;

    auto cache_key = [&](const RootStats& root, const std::string& move, const std::string& engine_id) {
        std::ostringstream out;
        out << root.position_key << '|' << move << '|' << engine_id << '|'
            << options.engine_movetime_ms << '|' << options.engine_accept_policy << '|'
            << options.engine_max_loss_cp << '|' << options.engine_reference_mode;
        return out.str();
    };

    for (const SurvivorRoot& sr : survivor_roots) {
        ++roots_evaluated;
        progress.update([&](ProgressSnapshot& s) {
            s.probe_entries_evaluated = roots_evaluated;
            s.probe_current_id = sr.root->position_key;
        });

        const std::string root_best_key = cache_key(*sr.root, "<root_best>", engine.engine_id());
        int root_best_cp = 0;
        if (auto v = cache.get(root_best_key)) {
            root_best_cp = v->cp;
            ++cache_hits;
        } else {
            root_best_cp = engine.eval_root_cp(sr.root->root_fen);
            cache.put(root_best_key, CacheEntry{root_best_cp});
            ++cache_misses;
        }

        std::vector<std::pair<std::string, MoveStats>> sorted(sr.root->move_stats.begin(), sr.root->move_stats.end());
        std::sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& b) {
            if (a.second.support != b.second.support) return a.second.support > b.second.support;
            return a.first < b.first;
        });

        std::string accepted_baseline_move;
        int accepted_baseline_support = 0;
        std::string accepted_baseline_engine_reason = "discard_no_engine_accepted_baseline";
        EmpiricalMoments accepted_baseline_m;

        int baseline_seen = 0;
        for (const auto& [move, ms] : sorted) {
            if (ms.support < options.baseline_min_support) continue;
            if (baseline_seen >= options.baseline_prefix_limit) break;
            ++baseline_seen;
            ++baseline_evals;
            const std::string key = cache_key(*sr.root, move, engine.engine_id());
            int move_cp = 0;
            if (auto v = cache.get(key)) {
                move_cp = v->cp;
                ++cache_hits;
            } else {
                move_cp = engine.eval_move_cp_for_mover(sr.root->root_fen, move);
                cache.put(key, CacheEntry{move_cp});
                ++cache_misses;
            }
            const int loss_cp = root_best_cp - move_cp;
            if (loss_cp <= options.engine_max_loss_cp) {
                accepted_baseline_move = move;
                accepted_baseline_support = ms.support;
                accepted_baseline_engine_reason = "accepted_engine_within_max_loss_cp";
                accepted_baseline_m = moments(ms);
                break;
            }
        }
        if (accepted_baseline_move.empty()) {
            ++roots_discarded_no_baseline;
        }

        std::vector<CandidateReport> candidate_reports;
        std::vector<std::pair<std::string, MoveStats>> cands;
        for (const auto& move : sr.survivor_candidates) {
            cands.push_back({move, sr.root->move_stats.at(move)});
        }
        std::sort(cands.begin(), cands.end(), [](const auto& a, const auto& b) {
            if (a.second.support != b.second.support) return a.second.support > b.second.support;
            return a.first < b.first;
        });

        int cand_seen = 0;
        for (const auto& [move, ms] : cands) {
            if (cand_seen >= options.candidate_prefix_limit) break;
            ++cand_seen;
            ++candidate_evals;
            CandidateReport cr;
            cr.candidate_move = move;
            cr.candidate_support = ms.support;
            cr.candidate_moments = moments(ms);
            cr.baseline_ceiling_empirical = accepted_baseline_m.ceiling;
            cr.candidate_ceiling_beats_baseline = cr.candidate_moments.ceiling > accepted_baseline_m.ceiling;

            const std::string key = cache_key(*sr.root, move, engine.engine_id());
            int move_cp = 0;
            if (auto v = cache.get(key)) {
                move_cp = v->cp;
                ++cache_hits;
            } else {
                move_cp = engine.eval_move_cp_for_mover(sr.root->root_fen, move);
                cache.put(key, CacheEntry{move_cp});
                ++cache_misses;
            }
            const int loss_cp = root_best_cp - move_cp;
            cr.candidate_is_engine_fail = loss_cp > options.engine_max_loss_cp;
            cr.candidate_engine_reason = cr.candidate_is_engine_fail ? "candidate_engine_fail_exceeds_max_loss_cp" : "discard_candidate_engine_not_fail";

            if (accepted_baseline_move.empty()) {
                cr.final_reason_code = "discard_no_engine_accepted_baseline";
                cr.final_pass = false;
            } else if (!cr.candidate_is_engine_fail) {
                cr.final_reason_code = "discard_candidate_engine_not_fail";
                cr.final_pass = false;
            } else if (!cr.candidate_ceiling_beats_baseline) {
                cr.final_reason_code = "discard_candidate_ceiling_not_above_baseline";
                cr.final_pass = false;
            } else {
                cr.final_reason_code = "pass_candidate_ceiling_above_engine_accepted_baseline";
                cr.final_pass = true;
            }
            if (cr.final_pass) ++candidates_passed;
            else ++candidates_rejected;
            candidate_reports.push_back(std::move(cr));
        }

        std::ostringstream line;
        line << "{\"position_key\":\"" << json_escape(sr.root->position_key) << "\",";
        line << "\"root_support\":" << sr.root->root_support << ",";
        line << "\"most_common_raw_baseline_move\":\"" << sr.most_common_raw_baseline << "\",";
        line << "\"most_common_raw_baseline_support\":" << sr.most_common_raw_baseline_support << ",";
        line << "\"most_common_raw_baseline_ceiling\":" << sr.most_common_raw_baseline_moments.ceiling << ",";
        line << "\"accepted_baseline_move\":" << (accepted_baseline_move.empty() ? "null" : ("\"" + accepted_baseline_move + "\"")) << ',';
        line << "\"accepted_baseline_support\":" << (accepted_baseline_move.empty() ? 0 : accepted_baseline_support) << ',';
        line << "\"accepted_baseline_engine_reason\":\"" << accepted_baseline_engine_reason << "\",";
        line << "\"accepted_baseline_mu_empirical\":" << accepted_baseline_m.mu << ',';
        line << "\"accepted_baseline_sigma_empirical\":" << accepted_baseline_m.sigma << ',';
        line << "\"accepted_baseline_ceiling_empirical\":" << accepted_baseline_m.ceiling << ',';
        line << "\"candidate_engine_checks\":[";
        for (std::size_t i = 0; i < candidate_reports.size(); ++i) {
            const auto& cr = candidate_reports[i];
            if (i) line << ',';
            line << "{\"candidate_move\":\"" << cr.candidate_move << "\",\"candidate_support\":" << cr.candidate_support
                 << ",\"candidate_mu_empirical\":" << cr.candidate_moments.mu
                 << ",\"candidate_sigma_empirical\":" << cr.candidate_moments.sigma
                 << ",\"candidate_ceiling_empirical\":" << cr.candidate_moments.ceiling
                 << ",\"candidate_engine_reason\":\"" << cr.candidate_engine_reason
                 << "\",\"candidate_is_engine_fail\":" << (cr.candidate_is_engine_fail ? "true" : "false")
                 << ",\"baseline_ceiling_empirical\":" << cr.baseline_ceiling_empirical
                 << ",\"candidate_ceiling_beats_baseline\":" << (cr.candidate_ceiling_beats_baseline ? "true" : "false")
                 << ",\"final_pass\":" << (cr.final_pass ? "true" : "false")
                 << ",\"final_reason_code\":\"" << cr.final_reason_code << "\"}";
        }
        line << "]}";
        final_root_lines.push_back(line.str());
    }
    progress.stage_completed("engine-baseline-screen complete");

    progress.stage_started(ProgressStage::WriteArtifacts, "write-artifacts");
    for (const auto& line : final_root_lines) root_report << line << "\n";
    root_report.flush();

    {
        std::ofstream summary(bundle_dir / "build_summary.txt");
        summary << "practical risk screening complete\n";
        summary << "games seen: " << games_seen << "\n";
        summary << "games in scope: " << games_scope << "\n";
        summary << "roots observed: " << roots.size() << "\n";
        summary << "survivor roots: " << survivor_roots.size() << "\n";
        summary << "candidates passed: " << candidates_passed << "\n";
    }
    {
        std::ofstream manifest(bundle_dir / "manifest.json");
        manifest << "{\n";
        manifest << "  \"artifact_id\": \"" << json_escape(options.artifact_id) << "\",\n";
        manifest << "  \"artifact_role\": \"practical_risk_screening_artifact\",\n";
        manifest << "  \"stockfish_used\": true,\n";
        manifest << "  \"external_book_dependency_used\": false,\n";
        manifest << "  \"evaluation_policy\": {\n";
        manifest << "    \"engine_accept_policy\": \"" << json_escape(options.engine_accept_policy) << "\",\n";
        manifest << "    \"engine_max_loss_cp\": " << options.engine_max_loss_cp << ",\n";
        manifest << "    \"engine_reference_mode\": \"" << json_escape(options.engine_reference_mode) << "\"\n";
        manifest << "  },\n";
        manifest << "  \"candidate_min_support\": " << options.candidate_min_support << ",\n";
        manifest << "  \"baseline_min_support\": " << options.baseline_min_support << ",\n";
        manifest << "  \"root_min_support\": " << options.root_min_support << ",\n";
        manifest << "  \"baseline_prefix_limit\": " << options.baseline_prefix_limit << ",\n";
        manifest << "  \"candidate_prefix_limit\": " << options.candidate_prefix_limit << ",\n";
        manifest << "  \"raw_empirical_ceiling_rule_used\": true,\n";
        manifest << "  \"mean_penalty_veto_used\": false\n";
        manifest << "}\n";
    }
    {
        std::ofstream screen(bundle_dir / "screen_summary.json");
        screen << "{\n";
        screen << "  \"games_seen\": " << games_seen << ",\n";
        screen << "  \"games_in_scope\": " << games_scope << ",\n";
        screen << "  \"roots_observed\": " << roots.size() << ",\n";
        screen << "  \"survivor_roots\": " << survivor_roots.size() << ",\n";
        screen << "  \"roots_discarded_low_support\": " << roots_discarded_support << ",\n";
        screen << "  \"roots_discarded_no_candidate_support\": " << roots_discarded_no_candidate_support << ",\n";
        screen << "  \"roots_discarded_no_candidate_empirical_survivors\": " << roots_discarded_no_empirical_survivor << ",\n";
        screen << "  \"roots_discarded_no_engine_accepted_baseline\": " << roots_discarded_no_baseline << ",\n";
        screen << "  \"baseline_engine_evaluations\": " << baseline_evals << ",\n";
        screen << "  \"candidate_engine_evaluations\": " << candidate_evals << ",\n";
        screen << "  \"cache_hits\": " << cache_hits << ",\n";
        screen << "  \"cache_misses\": " << cache_misses << ",\n";
        screen << "  \"candidates_passed\": " << candidates_passed << ",\n";
        screen << "  \"candidates_rejected\": " << candidates_rejected << "\n";
        screen << "}\n";
    }
    progress.stage_completed("write-artifacts complete");

    progress.stage_started(ProgressStage::Finalize, "finalize");
    progress.update([&](ProgressSnapshot& s) {
        s.risky_estimated_remaining_work = std::nullopt;
        s.last_event_message = "finalized";
    });
    progress.stage_completed("finalize complete");
    progress.finish();
    return 0;
}

}  // namespace otcb
