#include "otcb/practical_risk/prestockfish.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
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
    for (const char ch : movetext) {
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
        std::size_t p = 0;
        const int out = std::stoi(value, &p);
        if (p != value.size()) return std::nullopt;
        return out;
    } catch (...) {
        return std::nullopt;
    }
}

double score_for_side(const std::string& result, const Color side) {
    if (result == "1/2-1/2") return 0.5;
    if (result == "1-0") return side == Color::White ? 1.0 : 0.0;
    if (result == "0-1") return side == Color::Black ? 1.0 : 0.0;
    return 0.5;
}

struct ParsedGame {
    std::map<std::string, std::string> tags;
    std::string movetext;
};

template <typename Callback>
void stream_games(const std::filesystem::path& pgn_path, ProgressReporter& progress, Callback&& callback) {
    std::ifstream in(pgn_path);
    if (!in) throw std::runtime_error("unable to open input pgn: " + pgn_path.string());

    ParsedGame current;
    bool in_moves = false;
    std::string line;
    while (std::getline(in, line)) {
        progress.update([&](ProgressSnapshot& s) { s.source_bytes_scanned += static_cast<std::uint64_t>(line.size() + 1); });

        const std::string t = trim(line);
        if (t.empty()) {
            if (in_moves && !current.movetext.empty() && !current.tags.empty()) {
                callback(current);
                current = ParsedGame{};
                in_moves = false;
            }
            continue;
        }

        if (!in_moves && t.front() == '[') {
            const auto q1 = t.find('"');
            const auto q2 = t.rfind('"');
            if (q1 != std::string::npos && q2 != std::string::npos && q2 > q1) {
                const std::string name = trim(t.substr(1, q1 - 1));
                const std::string value = t.substr(q1 + 1, q2 - q1 - 1);
                if (!name.empty()) current.tags[name] = value;
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

struct OutcomeCounts {
    int wins = 0;
    int draws = 0;
    int losses = 0;

    int support() const { return wins + draws + losses; }

    void add_utility(const double utility) {
        if (utility >= 0.999) ++wins;
        else if (utility <= 0.001) ++losses;
        else ++draws;
    }

    double mu() const {
        const int n = support();
        if (n <= 0) return 0.0;
        return (static_cast<double>(wins) + 0.5 * static_cast<double>(draws)) / static_cast<double>(n);
    }
};

struct LineStats {
    OutcomeCounts outcomes;
    int own_ply_index = 0;
    int total_ply_depth = 0;
};

struct MoveStats {
    OutcomeCounts outcomes;
    std::map<std::string, LineStats> lines;
};

struct RootStats {
    int root_support = 0;
    Color side_to_move = Color::White;
    std::map<std::string, MoveStats> moves;
};

struct PriorBucket {
    std::string bucket_key;
    std::string rating_band;
    std::string time_control_id;
    std::string evaluating_side;
    int deep_total_plies = 0;
    int deep_own_plies = 0;
    int support_floor = 0;
    double sigma_global_deep = 0.0;
    int source_line_count = 0;
    std::string weighting_rule;

    double weighted_sigma_sum = 0.0;
    int weighted_sigma_weight = 0;
};

std::string join_moves(const std::vector<std::string>& moves) {
    std::ostringstream out;
    for (std::size_t i = 0; i < moves.size(); ++i) {
        if (i) out << ' ';
        out << moves[i];
    }
    return out.str();
}

double weighted_sigma_from_lines(const std::vector<std::pair<int, double>>& weighted_mu_lines) {
    int weight_sum = 0;
    double weighted_mean_sum = 0.0;
    for (const auto& item : weighted_mu_lines) {
        weight_sum += item.first;
        weighted_mean_sum += static_cast<double>(item.first) * item.second;
    }
    if (weight_sum <= 0) return 0.0;
    const double mean = weighted_mean_sum / static_cast<double>(weight_sum);
    double variance_num = 0.0;
    for (const auto& item : weighted_mu_lines) {
        const double diff = item.second - mean;
        variance_num += static_cast<double>(item.first) * diff * diff;
    }
    const double variance = variance_num / static_cast<double>(weight_sum);
    return std::sqrt(std::max(0.0, variance));
}

void sqlite_exec(sqlite3* db, const char* sql) {
    char* err = nullptr;
    if (sqlite3_exec(db, sql, nullptr, nullptr, &err) != SQLITE_OK) {
        const std::string msg = err ? err : "sqlite error";
        sqlite3_free(err);
        throw std::runtime_error(msg);
    }
}

std::string side_label(const Color color) {
    return color == Color::White ? "white" : "black";
}

}  // namespace

void print_practical_risk_prestockfish_usage(const std::string& program_name) {
    std::cout
        << "Usage: " << program_name << " --input-pgn <path> --output-dir <dir> --artifact-id <id>\\n"
        << "       --min-rating <n> --max-rating <n> --rating-policy <both_in_band|average_in_band|white_in_band|black_in_band>\\n"
        << "       --retained-ply <n> --time-controls <tc1,tc2> --time-control-id <id> --initial-time-seconds <sec>\\n"
        << "       --increment-seconds <sec> --time-format-label <label> [stage-a options]\\n";
}

PracticalRiskPrestockfishOptions parse_practical_risk_prestockfish_cli(int argc, char** argv) {
    PracticalRiskPrestockfishOptions out;
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
        else if (arg == "--root-min-support") out.root_min_support = std::stoi(require_value("--root-min-support"));
        else if (arg == "--move-min-support") out.move_min_support = std::stoi(require_value("--move-min-support"));
        else if (arg == "--deep-line-min-support") out.deep_line_min_support = std::stoi(require_value("--deep-line-min-support"));
        else if (arg == "--deep-total-plies") out.deep_total_plies = std::stoi(require_value("--deep-total-plies"));
        else if (arg == "--deep-own-plies") out.deep_own_plies = std::stoi(require_value("--deep-own-plies"));
        else if (arg == "--sigma-global-min-lines") out.sigma_global_min_lines = std::stoi(require_value("--sigma-global-min-lines"));
        else if (arg == "--emit-progress-log") out.emit_progress_log = true;
        else if (arg == "--emit-status-json") out.emit_status_json = true;
        else if (arg == "--heartbeat-seconds") out.heartbeat_seconds = std::stoi(require_value("--heartbeat-seconds"));
        else if (arg == "--quiet-progress") out.quiet_progress = true;
        else if (arg == "--help" || arg == "-h") {
            print_practical_risk_prestockfish_usage(argv[0]);
            std::exit(0);
        } else {
            throw std::runtime_error("unknown argument: " + arg);
        }
    }

    if (out.input_pgn.empty() || out.output_dir.empty() || out.artifact_id.empty()) {
        throw std::runtime_error("missing required arguments --input-pgn --output-dir --artifact-id");
    }
    if (out.retained_ply <= 0) throw std::runtime_error("--retained-ply must be > 0");
    if (out.time_controls.empty()) throw std::runtime_error("--time-controls required");
    if (out.time_control_id.empty()) throw std::runtime_error("--time-control-id required");
    if (out.min_rating > out.max_rating) throw std::runtime_error("--min-rating must be <= --max-rating");
    if (out.root_min_support < 1 || out.move_min_support < 1 || out.deep_line_min_support < 1) {
        throw std::runtime_error("support floors must be >= 1");
    }
    if (out.deep_total_plies < 1 || out.deep_own_plies < 1 || out.sigma_global_min_lines < 1) {
        throw std::runtime_error("deep/sigma parameters must be >= 1");
    }
    return out;
}

int run_practical_risk_prestockfish(const PracticalRiskPrestockfishOptions& options) {
    const auto bundle_dir = options.output_dir / options.artifact_id;
    std::filesystem::create_directories(bundle_dir);

    ProgressReporter progress(ProgressReporterOptions{options.quiet_progress, options.emit_progress_log, options.emit_status_json, options.heartbeat_seconds, bundle_dir});
    progress.start();

    progress.stage_started(ProgressStage::Preflight, "preflight practical-risk prestockfish");
    progress.stage_completed("preflight complete");

    const auto source_size = std::filesystem::exists(options.input_pgn) ? std::optional<std::uint64_t>(std::filesystem::file_size(options.input_pgn)) : std::nullopt;
    progress.stage_started(ProgressStage::AggregateEmpiricalScreen, "aggregate-empirical-screen", source_size);

    std::map<std::string, RootStats> roots;
    std::set<std::string> tc_allowed(options.time_controls.begin(), options.time_controls.end());
    std::uint64_t root_move_rows = 0;
    std::uint64_t deep_line_rows = 0;
    int games_seen = 0;
    int games_accepted = 0;

    stream_games(options.input_pgn, progress, [&](const ParsedGame& game) {
        ++games_seen;
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

        ++games_accepted;
        ChessBoard board;
        const auto san_moves = tokenize_movetext(game.movetext);
        for (std::size_t ply = 0; ply < san_moves.size() && static_cast<int>(ply) < options.retained_ply; ++ply) {
            const auto resolved = resolve_san_move(board, san_moves[ply]);
            if (!resolved.success || !resolved.move.has_value()) break;

            const Color mover = board.side_to_move();
            const std::string position_key = make_position_key(board, PositionKeyFormat::FenNormalized);
            const std::string move_uci = move_to_uci(*resolved.move);
            const double utility = score_for_side(result_it->second, mover);

            RootStats& root = roots[position_key];
            root.side_to_move = mover;
            ++root.root_support;
            MoveStats& move_stats = root.moves[move_uci];
            move_stats.outcomes.add_utility(utility);
            ++root_move_rows;

            std::vector<std::string> line_moves;
            line_moves.reserve(static_cast<std::size_t>(options.deep_total_plies));
            line_moves.push_back(move_uci);
            int mover_own_plies = 1;
            int total_depth = 1;

            ChessBoard line_board = board;
            line_board.apply_move(*resolved.move);

            for (int j = static_cast<int>(ply) + 1; j < static_cast<int>(san_moves.size()) && total_depth < options.deep_total_plies; ++j) {
                const auto deeper = resolve_san_move(line_board, san_moves[static_cast<std::size_t>(j)]);
                if (!deeper.success || !deeper.move.has_value()) break;
                const Color current_side = line_board.side_to_move();
                const std::string deep_move_uci = move_to_uci(*deeper.move);
                line_moves.push_back(deep_move_uci);
                if (current_side == mover) ++mover_own_plies;
                ++total_depth;
                line_board.apply_move(*deeper.move);
            }

            if (total_depth >= options.deep_total_plies && mover_own_plies >= options.deep_own_plies) {
                const std::string line_key = join_moves(line_moves);
                LineStats& line_stats = move_stats.lines[line_key];
                line_stats.outcomes.add_utility(utility);
                line_stats.own_ply_index = options.deep_own_plies;
                line_stats.total_ply_depth = options.deep_total_plies;
                ++deep_line_rows;
            }

            board.apply_move(*resolved.move);
        }

        progress.update([&](ProgressSnapshot& s) {
            s.games_scanned = games_seen;
            s.games_accepted = games_accepted;
            s.games_rejected = games_seen - games_accepted;
            s.aggregated_positions = static_cast<int>(roots.size());
            s.aggregate_move_entries = static_cast<int>(root_move_rows);
            s.raw_observations = static_cast<int>(deep_line_rows);
            const double elapsed = std::max(1.0, std::chrono::duration<double>(std::chrono::steady_clock::now() - s.stage_started_at).count());
            s.throughput_per_second = static_cast<double>(games_seen) / elapsed;
            if (s.source_file_size.has_value() && s.source_bytes_scanned > 0) {
                const double pct = std::min(100.0, 100.0 * static_cast<double>(s.source_bytes_scanned) / static_cast<double>(*s.source_file_size));
                s.percent_complete = pct;
                if (s.throughput_per_second.has_value() && *s.throughput_per_second > 0.0) {
                    const double remaining_games = std::max(0.0, static_cast<double>(games_seen) * ((100.0 - pct) / std::max(0.1, pct)));
                    s.eta = std::chrono::seconds(static_cast<int>(remaining_games / *s.throughput_per_second));
                }
            }
        });
    });
    progress.stage_completed("aggregate-empirical-screen complete");

    progress.stage_started(ProgressStage::ComputeSigmaPriors, "compute-sigma-priors");

    const std::string rating_band = std::to_string(options.min_rating) + "-" + std::to_string(options.max_rating);
    std::map<std::string, PriorBucket> priors;

    struct ComputedMove {
        std::string position_key;
        std::string move_uci;
        int move_support = 0;
        OutcomeCounts outcomes;
        double mu = 0.0;
        double sigma_deep = 0.0;
        std::string sigma_mode;
        double ceiling = 0.0;
        int n_qual = 0;
        int popularity_rank = 0;
        std::string evaluating_side;
        std::vector<std::tuple<std::string, LineStats, bool>> lines;
    };

    std::map<std::string, std::vector<ComputedMove>> computed_by_root;

    int roots_retained = 0;
    int moves_retained = 0;
    int mode_observed = 0;
    int mode_shrunk = 0;
    int mode_prior_only = 0;

    for (const auto& [position_key, root] : roots) {
        if (root.root_support < options.root_min_support) continue;
        ++roots_retained;

        std::vector<std::pair<std::string, const MoveStats*>> ranked;
        ranked.reserve(root.moves.size());
        for (const auto& [move_uci, move_stats] : root.moves) {
            ranked.push_back({move_uci, &move_stats});
        }
        std::sort(ranked.begin(), ranked.end(), [](const auto& a, const auto& b) {
            if (a.second->outcomes.support() != b.second->outcomes.support()) return a.second->outcomes.support() > b.second->outcomes.support();
            return a.first < b.first;
        });

        std::vector<ComputedMove> kept_moves;
        int rank = 0;
        for (const auto& [move_uci, move_stats] : ranked) {
            ++rank;
            if (move_stats->outcomes.support() < options.move_min_support) continue;
            ComputedMove row;
            row.position_key = position_key;
            row.move_uci = move_uci;
            row.move_support = move_stats->outcomes.support();
            row.outcomes = move_stats->outcomes;
            row.mu = move_stats->outcomes.mu();
            row.popularity_rank = rank;
            row.evaluating_side = side_label(root.side_to_move);

            std::vector<std::pair<int, double>> qualified;
            for (const auto& [line_key, line] : move_stats->lines) {
                const bool qualifies = line.outcomes.support() >= options.deep_line_min_support;
                row.lines.push_back({line_key, line, qualifies});
                if (qualifies) {
                    qualified.push_back({line.outcomes.support(), line.outcomes.mu()});
                }
            }

            row.n_qual = static_cast<int>(qualified.size());
            if (row.n_qual >= options.sigma_global_min_lines) {
                row.sigma_mode = "observed";
                row.sigma_deep = weighted_sigma_from_lines(qualified);
                const std::string bucket_key = rating_band + "|" + options.time_control_id + "|" + row.evaluating_side + "|d" + std::to_string(options.deep_total_plies) + "|o" + std::to_string(options.deep_own_plies) + "|sf" + std::to_string(options.deep_line_min_support);
                PriorBucket& bucket = priors[bucket_key];
                bucket.bucket_key = bucket_key;
                bucket.rating_band = rating_band;
                bucket.time_control_id = options.time_control_id;
                bucket.evaluating_side = row.evaluating_side;
                bucket.deep_total_plies = options.deep_total_plies;
                bucket.deep_own_plies = options.deep_own_plies;
                bucket.support_floor = options.deep_line_min_support;
                bucket.weighting_rule = "weighted_mean_of_observed_sigma_by_n_qual";
                bucket.weighted_sigma_sum += row.sigma_deep * static_cast<double>(row.n_qual);
                bucket.weighted_sigma_weight += row.n_qual;
                bucket.source_line_count += row.n_qual;
                ++mode_observed;
            }
            kept_moves.push_back(row);
        }
        if (!kept_moves.empty()) {
            computed_by_root[position_key] = std::move(kept_moves);
            moves_retained += static_cast<int>(computed_by_root[position_key].size());
        }
    }

    for (auto& [_, bucket] : priors) {
        bucket.sigma_global_deep = bucket.weighted_sigma_weight > 0 ? bucket.weighted_sigma_sum / static_cast<double>(bucket.weighted_sigma_weight) : 0.0;
    }

    for (auto& [_, moves] : computed_by_root) {
        for (auto& row : moves) {
            if (row.sigma_mode == "observed") {
                row.ceiling = row.mu + row.sigma_deep;
                continue;
            }
            const std::string bucket_key = rating_band + "|" + options.time_control_id + "|" + row.evaluating_side + "|d" + std::to_string(options.deep_total_plies) + "|o" + std::to_string(options.deep_own_plies) + "|sf" + std::to_string(options.deep_line_min_support);
            const auto prior_it = priors.find(bucket_key);
            const double prior_sigma = prior_it == priors.end() ? 0.0 : prior_it->second.sigma_global_deep;

            if (row.n_qual == 0) {
                row.sigma_mode = "prior_only_sparse";
                row.sigma_deep = prior_sigma;
                ++mode_prior_only;
            } else {
                std::vector<std::pair<int, double>> qualified;
                for (const auto& [_, line, qualifies] : row.lines) {
                    if (qualifies) qualified.push_back({line.outcomes.support(), line.outcomes.mu()});
                }
                const double observed_sparse = weighted_sigma_from_lines(qualified);
                const double alpha = std::min(1.0, static_cast<double>(row.n_qual) / static_cast<double>(options.sigma_global_min_lines));
                row.sigma_mode = "shrunk_sparse";
                row.sigma_deep = alpha * observed_sparse + (1.0 - alpha) * prior_sigma;
                ++mode_shrunk;
            }
            row.ceiling = row.mu + row.sigma_deep;
        }
    }

    progress.update([&](ProgressSnapshot& s) {
        s.aggregated_positions = roots_retained;
        s.aggregate_move_entries = moves_retained;
        s.risky_admitted_rows = mode_observed;
        s.risky_unresolved_rows = mode_shrunk;
        s.risky_rejected_rows = mode_prior_only;
    });
    progress.stage_completed("compute-sigma-priors complete");

    progress.stage_started(ProgressStage::WriteArtifacts, "write-artifacts");

    const auto sqlite_path = bundle_dir / "practical_risk_prestockfish.sqlite";
    sqlite3* db = nullptr;
    if (sqlite3_open(sqlite_path.string().c_str(), &db) != SQLITE_OK) {
        throw std::runtime_error("unable to open sqlite output");
    }

    sqlite_exec(db, "PRAGMA journal_mode=WAL;");
    sqlite_exec(db,
                "CREATE TABLE artifact_metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);"
                "CREATE TABLE roots(position_key TEXT PRIMARY KEY, root_support INTEGER NOT NULL, side_to_move TEXT NOT NULL, rating_band TEXT NOT NULL, rating_policy TEXT NOT NULL, time_control_id TEXT NOT NULL, retained_ply INTEGER NOT NULL, deep_total_plies INTEGER NOT NULL, deep_own_plies INTEGER NOT NULL);"
                "CREATE TABLE root_moves(position_key TEXT NOT NULL, move_uci TEXT NOT NULL, move_support INTEGER NOT NULL, wins INTEGER NOT NULL, draws INTEGER NOT NULL, losses INTEGER NOT NULL, mu REAL NOT NULL, sigma_deep REAL NOT NULL, sigma_mode TEXT NOT NULL, ceiling REAL NOT NULL, n_qual INTEGER NOT NULL, popularity_rank INTEGER NOT NULL, PRIMARY KEY(position_key, move_uci));"
                "CREATE TABLE deep_lines(position_key TEXT NOT NULL, root_move_uci TEXT NOT NULL, line_key TEXT NOT NULL, own_ply_index INTEGER NOT NULL, total_ply_depth INTEGER NOT NULL, support_count INTEGER NOT NULL, wins INTEGER NOT NULL, draws INTEGER NOT NULL, losses INTEGER NOT NULL, line_mu REAL NOT NULL, qualifies_sigma INTEGER NOT NULL, PRIMARY KEY(position_key, root_move_uci, line_key));"
                "CREATE TABLE sigma_global_priors(bucket_key TEXT PRIMARY KEY, rating_band TEXT NOT NULL, time_control_id TEXT NOT NULL, evaluating_side TEXT NOT NULL, deep_total_plies INTEGER NOT NULL, deep_own_plies INTEGER NOT NULL, support_floor INTEGER NOT NULL, sigma_global_deep REAL NOT NULL, source_line_count INTEGER NOT NULL, weighting_rule TEXT NOT NULL);");

    sqlite_exec(db, "BEGIN TRANSACTION;");

    auto insert_kv = [&](const std::string& k, const std::string& v) {
        sqlite3_stmt* stmt = nullptr;
        sqlite3_prepare_v2(db, "INSERT INTO artifact_metadata(key, value) VALUES(?1, ?2)", -1, &stmt, nullptr);
        sqlite3_bind_text(stmt, 1, k.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, v.c_str(), -1, SQLITE_TRANSIENT);
        (void)sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    };

    insert_kv("artifact_role", "practical_risk_prestockfish");
    insert_kv("stockfish_used", "false");
    insert_kv("external_book_dependency_used", "false");
    insert_kv("rating_band", rating_band);
    insert_kv("rating_policy", to_string(options.rating_policy));
    insert_kv("time_control_id", options.time_control_id);
    insert_kv("retained_ply", std::to_string(options.retained_ply));
    insert_kv("root_min_support", std::to_string(options.root_min_support));
    insert_kv("move_min_support", std::to_string(options.move_min_support));
    insert_kv("deep_line_min_support", std::to_string(options.deep_line_min_support));
    insert_kv("deep_total_plies", std::to_string(options.deep_total_plies));
    insert_kv("deep_own_plies", std::to_string(options.deep_own_plies));
    insert_kv("sigma_global_min_lines", std::to_string(options.sigma_global_min_lines));

    sqlite3_stmt* root_stmt = nullptr;
    sqlite3_stmt* move_stmt = nullptr;
    sqlite3_stmt* line_stmt = nullptr;
    sqlite3_stmt* prior_stmt = nullptr;

    sqlite3_prepare_v2(db, "INSERT INTO roots(position_key, root_support, side_to_move, rating_band, rating_policy, time_control_id, retained_ply, deep_total_plies, deep_own_plies) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)", -1, &root_stmt, nullptr);
    sqlite3_prepare_v2(db, "INSERT INTO root_moves(position_key, move_uci, move_support, wins, draws, losses, mu, sigma_deep, sigma_mode, ceiling, n_qual, popularity_rank) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)", -1, &move_stmt, nullptr);
    sqlite3_prepare_v2(db, "INSERT INTO deep_lines(position_key, root_move_uci, line_key, own_ply_index, total_ply_depth, support_count, wins, draws, losses, line_mu, qualifies_sigma) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)", -1, &line_stmt, nullptr);
    sqlite3_prepare_v2(db, "INSERT INTO sigma_global_priors(bucket_key, rating_band, time_control_id, evaluating_side, deep_total_plies, deep_own_plies, support_floor, sigma_global_deep, source_line_count, weighting_rule) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)", -1, &prior_stmt, nullptr);

    for (const auto& [position_key, root] : roots) {
        if (root.root_support < options.root_min_support) continue;
        if (computed_by_root.find(position_key) == computed_by_root.end()) continue;

        sqlite3_reset(root_stmt);
        sqlite3_bind_text(root_stmt, 1, position_key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(root_stmt, 2, root.root_support);
        const std::string stm = side_label(root.side_to_move);
        sqlite3_bind_text(root_stmt, 3, stm.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(root_stmt, 4, rating_band.c_str(), -1, SQLITE_TRANSIENT);
        const std::string rating_policy = to_string(options.rating_policy);
        sqlite3_bind_text(root_stmt, 5, rating_policy.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(root_stmt, 6, options.time_control_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(root_stmt, 7, options.retained_ply);
        sqlite3_bind_int(root_stmt, 8, options.deep_total_plies);
        sqlite3_bind_int(root_stmt, 9, options.deep_own_plies);
        (void)sqlite3_step(root_stmt);

        for (const auto& move : computed_by_root[position_key]) {
            sqlite3_reset(move_stmt);
            sqlite3_bind_text(move_stmt, 1, position_key.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(move_stmt, 2, move.move_uci.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(move_stmt, 3, move.move_support);
            sqlite3_bind_int(move_stmt, 4, move.outcomes.wins);
            sqlite3_bind_int(move_stmt, 5, move.outcomes.draws);
            sqlite3_bind_int(move_stmt, 6, move.outcomes.losses);
            sqlite3_bind_double(move_stmt, 7, move.mu);
            sqlite3_bind_double(move_stmt, 8, move.sigma_deep);
            sqlite3_bind_text(move_stmt, 9, move.sigma_mode.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_double(move_stmt, 10, move.ceiling);
            sqlite3_bind_int(move_stmt, 11, move.n_qual);
            sqlite3_bind_int(move_stmt, 12, move.popularity_rank);
            (void)sqlite3_step(move_stmt);

            for (const auto& [line_key, line, qualifies] : move.lines) {
                sqlite3_reset(line_stmt);
                sqlite3_bind_text(line_stmt, 1, position_key.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(line_stmt, 2, move.move_uci.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(line_stmt, 3, line_key.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int(line_stmt, 4, line.own_ply_index);
                sqlite3_bind_int(line_stmt, 5, line.total_ply_depth);
                sqlite3_bind_int(line_stmt, 6, line.outcomes.support());
                sqlite3_bind_int(line_stmt, 7, line.outcomes.wins);
                sqlite3_bind_int(line_stmt, 8, line.outcomes.draws);
                sqlite3_bind_int(line_stmt, 9, line.outcomes.losses);
                sqlite3_bind_double(line_stmt, 10, line.outcomes.mu());
                sqlite3_bind_int(line_stmt, 11, qualifies ? 1 : 0);
                (void)sqlite3_step(line_stmt);
            }
        }
    }

    for (const auto& [bucket_key, prior] : priors) {
        sqlite3_reset(prior_stmt);
        sqlite3_bind_text(prior_stmt, 1, bucket_key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(prior_stmt, 2, prior.rating_band.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(prior_stmt, 3, prior.time_control_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(prior_stmt, 4, prior.evaluating_side.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(prior_stmt, 5, prior.deep_total_plies);
        sqlite3_bind_int(prior_stmt, 6, prior.deep_own_plies);
        sqlite3_bind_int(prior_stmt, 7, prior.support_floor);
        sqlite3_bind_double(prior_stmt, 8, prior.sigma_global_deep);
        sqlite3_bind_int(prior_stmt, 9, prior.source_line_count);
        sqlite3_bind_text(prior_stmt, 10, prior.weighting_rule.c_str(), -1, SQLITE_TRANSIENT);
        (void)sqlite3_step(prior_stmt);
    }

    sqlite3_finalize(root_stmt);
    sqlite3_finalize(move_stmt);
    sqlite3_finalize(line_stmt);
    sqlite3_finalize(prior_stmt);

    sqlite_exec(db, "COMMIT;");
    sqlite3_close(db);

    std::ofstream manifest(bundle_dir / "manifest.json");
    manifest << "{\n";
    manifest << "  \"artifact_id\": \"" << json_escape(options.artifact_id) << "\",\n";
    manifest << "  \"artifact_role\": \"practical_risk_prestockfish\",\n";
    manifest << "  \"stockfish_used\": false,\n";
    manifest << "  \"external_book_dependency_used\": false,\n";
    manifest << "  \"rating_band\": \"" << json_escape(rating_band) << "\",\n";
    manifest << "  \"rating_policy\": \"" << json_escape(to_string(options.rating_policy)) << "\",\n";
    manifest << "  \"time_control_id\": \"" << json_escape(options.time_control_id) << "\",\n";
    manifest << "  \"retained_ply\": " << options.retained_ply << ",\n";
    manifest << "  \"deep_total_plies\": " << options.deep_total_plies << ",\n";
    manifest << "  \"deep_own_plies\": " << options.deep_own_plies << ",\n";
    manifest << "  \"support_floors\": {\"root_min_support\": " << options.root_min_support << ", \"move_min_support\": " << options.move_min_support << ", \"deep_line_min_support\": " << options.deep_line_min_support << "},\n";
    manifest << "  \"sigma_policy\": {\"sigma_global_min_lines\": " << options.sigma_global_min_lines << ", \"sparse_modes\": [\"observed\", \"shrunk_sparse\", \"prior_only_sparse\"]},\n";
    manifest << "  \"output_files\": {\"sqlite\": \"practical_risk_prestockfish.sqlite\", \"summary\": \"summary.json\", \"build_summary\": \"build_summary.txt\"}\n";
    manifest << "}\n";

    std::ofstream build_summary(bundle_dir / "build_summary.txt");
    build_summary << "practical-risk prestockfish build complete\n";
    build_summary << "roots_observed=" << roots.size() << "\n";
    build_summary << "roots_retained=" << roots_retained << "\n";
    build_summary << "moves_retained=" << moves_retained << "\n";
    build_summary << "sigma_mode_observed=" << mode_observed << "\n";
    build_summary << "sigma_mode_shrunk_sparse=" << mode_shrunk << "\n";
    build_summary << "sigma_mode_prior_only_sparse=" << mode_prior_only << "\n";
    build_summary << "prior_buckets=" << priors.size() << "\n";

    std::ofstream summary(bundle_dir / "summary.json");
    summary << "{\n";
    summary << "  \"roots_observed\": " << roots.size() << ",\n";
    summary << "  \"roots_retained\": " << roots_retained << ",\n";
    summary << "  \"moves_retained\": " << moves_retained << ",\n";
    summary << "  \"sigma_mode_counts\": {\"observed\": " << mode_observed << ", \"shrunk_sparse\": " << mode_shrunk << ", \"prior_only_sparse\": " << mode_prior_only << "},\n";
    summary << "  \"prior_buckets_created\": " << priors.size() << "\n";
    summary << "}\n";

    progress.stage_completed("write-artifacts complete");
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
