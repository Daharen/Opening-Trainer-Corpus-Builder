#include "otcb/practical_risk/stockfish_overlay.hpp"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <sqlite3.h>

#include "otcb/chess_board.hpp"
#include "otcb/chess_types.hpp"
#include "otcb/practical_risk/engine_eval_cache.hpp"
#include "otcb/practical_risk/uci_engine.hpp"
#include "otcb/progress.hpp"

namespace otcb {
namespace {

struct RetainedMove {
    std::string move_uci;
    int move_support = 0;
    int popularity_rank = 0;
    double ceiling = 0.0;
};

struct RootData {
    std::string position_key;
    std::string rating_band;
    std::string time_control_id;
    std::string evaluating_side;
    int deep_total_plies = 0;
    int deep_own_plies = 0;
    int support_floor = 0;
    std::vector<RetainedMove> moves;
};

struct MoveEval {
    std::string position_key;
    std::string move_uci;
    int move_support = 0;
    int popularity_rank = 0;
    double root_best_cp = 0.0;
    double move_cp = 0.0;
    double raw_loss_cp = 0.0;
    double loss_cp = 0.0;
    std::string engine_quality_class;
    double ceiling = 0.0;
    int is_engine_accepted = 0;
    int is_engine_fail = 0;
    std::string eval_source;
    int cache_hit = 0;
};

struct BaselineRow {
    std::string position_key;
    std::optional<std::string> accepted_baseline_move;
    int accepted_baseline_support = 0;
    int accepted_baseline_rank = 0;
    int baseline_found = 0;
    std::string reason_code;
};

struct PriorAccumulator {
    std::string rating_band;
    std::string time_control_id;
    std::string evaluating_side;
    int deep_total_plies = 0;
    int deep_own_plies = 0;
    int support_floor = 0;
    double weighted_ceiling_sum = 0.0;
    int total_support = 0;
    int move_count = 0;
};

struct RootEngineThresholdRow {
    std::string position_key;
    std::optional<double> good_inclusive_min_ceiling;
    std::optional<double> good_exclusive_min_ceiling;
    std::optional<std::string> good_inclusive_min_move;
    std::optional<std::string> good_exclusive_min_move;
    int accepted_move_count = 0;
    int accepted_move_count_good_inclusive = 0;
    int accepted_move_count_good_exclusive = 0;
    int failed_move_count = 0;
};

constexpr double kScoreInvariantEpsilon = 1e-9;

struct EngineQualityThresholdPolicy {
    double book_max_loss_cp = 0.0;
    double best_max_loss_cp = 10.0;
    double excellent_max_loss_cp = 20.0;
};

constexpr EngineQualityThresholdPolicy kEngineQualityThresholdPolicy{};

struct ClassifiedMoveEval {
    MoveEval row;
    double raw_root_best_cp = 0.0;
    double raw_post_move_cp = 0.0;
    double normalized_move_cp_for_root_side = 0.0;
    double raw_loss_cp = 0.0;
    double effective_loss_cp = 0.0;
};

double effective_threshold(double configured_max_loss_cp, double policy_threshold_cp) {
    return std::min(configured_max_loss_cp, policy_threshold_cp);
}

std::string classify_engine_quality(double effective_loss_cp,
                                    int engine_max_loss_cp,
                                    const EngineQualityThresholdPolicy& policy) {
    const double configured_max_loss_cp = static_cast<double>(engine_max_loss_cp);
    if (std::abs(effective_loss_cp) <= kScoreInvariantEpsilon) return "book";
    if (effective_loss_cp <= effective_threshold(configured_max_loss_cp, policy.best_max_loss_cp)) return "best";
    if (effective_loss_cp <= effective_threshold(configured_max_loss_cp, policy.excellent_max_loss_cp)) return "excellent";
    if (effective_loss_cp <= configured_max_loss_cp) return "good";
    return "fail";
}

bool engine_quality_is_fail(const std::string& engine_quality_class) {
    return engine_quality_class == "fail";
}

bool engine_quality_in_good_exclusive(const std::string& engine_quality_class) {
    return engine_quality_class == "book" || engine_quality_class == "best" || engine_quality_class == "excellent";
}

std::string color_to_string(Color c) {
    return c == Color::White ? "white" : "black";
}

ClassifiedMoveEval make_validated_move_eval_row(const RootData& root,
                                                const RetainedMove& mv,
                                                double raw_root_best_cp,
                                                double raw_post_move_cp,
                                                bool root_cached,
                                                bool move_cached,
                                                int engine_max_loss_cp,
                                                const EngineQualityThresholdPolicy& quality_policy) {
    const double root_best_cp_for_root_side = raw_root_best_cp;
    const double move_cp_for_root_side = -raw_post_move_cp;
    const double computed_raw_loss_cp = root_best_cp_for_root_side - move_cp_for_root_side;
    const double computed_effective_loss_cp = std::max(0.0, computed_raw_loss_cp);
    const std::string engine_quality_class = classify_engine_quality(computed_effective_loss_cp, engine_max_loss_cp, quality_policy);
    const bool failed = engine_quality_is_fail(engine_quality_class);
    const bool accepted = !failed;

    MoveEval row{
        .position_key = root.position_key,
        .move_uci = mv.move_uci,
        .move_support = mv.move_support,
        .popularity_rank = mv.popularity_rank,
        .root_best_cp = root_best_cp_for_root_side,
        .move_cp = move_cp_for_root_side,
        .raw_loss_cp = computed_raw_loss_cp,
        .loss_cp = computed_effective_loss_cp,
        .engine_quality_class = engine_quality_class,
        .ceiling = mv.ceiling,
        .is_engine_accepted = accepted ? 1 : 0,
        .is_engine_fail = failed ? 1 : 0,
        .eval_source = (root_cached || move_cached) ? "cache_or_live" : "live",
        .cache_hit = move_cached ? 1 : 0,
    };
    if (std::abs(row.raw_loss_cp - (row.root_best_cp - row.move_cp)) > kScoreInvariantEpsilon) {
        throw std::runtime_error("stage-b invariant violation: raw_loss_cp must equal root_best_cp - move_cp");
    }
    if (std::abs(row.loss_cp - std::max(0.0, row.raw_loss_cp)) > kScoreInvariantEpsilon) {
        throw std::runtime_error("stage-b invariant violation: loss_cp must equal max(0, raw_loss_cp)");
    }
    if (row.engine_quality_class != classify_engine_quality(row.loss_cp, engine_max_loss_cp, quality_policy)) {
        throw std::runtime_error("stage-b invariant violation: engine_quality_class must match threshold policy");
    }
    if (row.is_engine_accepted != (engine_quality_is_fail(row.engine_quality_class) ? 0 : 1)) {
        throw std::runtime_error("stage-b invariant violation: is_engine_accepted must match class mapping");
    }
    if (row.is_engine_fail != (engine_quality_is_fail(row.engine_quality_class) ? 1 : 0)) {
        throw std::runtime_error("stage-b invariant violation: is_engine_fail must match class mapping");
    }
    if ((row.is_engine_accepted + row.is_engine_fail) != 1) {
        throw std::runtime_error("stage-b invariant violation: accepted/fail must be mutually exclusive");
    }
    return ClassifiedMoveEval{
        .row = std::move(row),
        .raw_root_best_cp = raw_root_best_cp,
        .raw_post_move_cp = raw_post_move_cp,
        .normalized_move_cp_for_root_side = move_cp_for_root_side,
        .raw_loss_cp = computed_raw_loss_cp,
        .effective_loss_cp = computed_effective_loss_cp,
    };
}

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

std::string sql_quote(const std::string& input) {
    std::string out;
    out.reserve(input.size() + 8);
    out.push_back('\'');
    for (const char c : input) {
        if (c == '\'') out += "''";
        else out.push_back(c);
    }
    out.push_back('\'');
    return out;
}

void sqlite_exec(sqlite3* db, const char* sql) {
    char* err = nullptr;
    if (sqlite3_exec(db, sql, nullptr, nullptr, &err) != SQLITE_OK) {
        const std::string msg = err ? err : "sqlite error";
        sqlite3_free(err);
        throw std::runtime_error(msg);
    }
}

std::string policy_hash(const PracticalRiskStockfishOverlayOptions& o) {
    std::ostringstream out;
    out << o.engine_accept_policy << '|' << o.engine_max_loss_cp << '|' << o.engine_reference_mode;
    return out.str();
}

std::string cache_key(const std::string& position_key,
                      const std::string& move_uci,
                      const std::string& engine_id,
                      int movetime_ms,
                      const std::string& policy_hash_value) {
    return position_key + "|" + move_uci + "|" + engine_id + "|" + std::to_string(movetime_ms) + "|" + policy_hash_value;
}

std::optional<ChessBoard> reconstruct_board_from_position_key(const std::string& position_key) {
    std::string full = trim(position_key);
    int spaces = 0;
    for (char c : full) if (c == ' ') ++spaces;
    if (spaces == 3) full += " 0 1";
    return ChessBoard::from_fen(full);
}

std::optional<Move> resolve_uci_move(const ChessBoard& board, const std::string& move_uci) {
    const auto legal = board.generate_legal_moves();
    for (const Move& m : legal) {
        if (move_to_uci(m) == move_uci) return m;
    }
    return std::nullopt;
}

void write_negative_loss_anomaly_artifact(const std::filesystem::path& bundle_dir,
                                          const std::string& artifact_id,
                                          const RootData& root,
                                          const RetainedMove& mv,
                                          const std::string& fen_root,
                                          const std::string& fen_after_move,
                                          Color root_side,
                                          Color side_to_move_after_move,
                                          const ClassifiedMoveEval& classified_eval,
                                          int engine_max_loss_cp,
                                          bool root_cached,
                                          bool move_cached,
                                          const std::string& engine_id,
                                          int engine_movetime_ms,
                                          const std::string& engine_accept_policy,
                                          const std::string& engine_reference_mode) {
    const auto anomaly_path = bundle_dir / "progress" / "first_negative_loss_anomaly.json";
    std::ofstream out(anomaly_path);
    if (!out) {
        throw std::runtime_error("failed to write stage-b anomaly artifact: " + anomaly_path.string());
    }
    out << "{\n";
    out << "  \"artifact_id\": \"" << json_escape(artifact_id) << "\",\n";
    out << "  \"position_key\": \"" << json_escape(root.position_key) << "\",\n";
    out << "  \"move_uci\": \"" << json_escape(mv.move_uci) << "\",\n";
    out << "  \"move_support\": " << mv.move_support << ",\n";
    out << "  \"popularity_rank\": " << mv.popularity_rank << ",\n";
    out << "  \"root_side\": \"" << color_to_string(root_side) << "\",\n";
    out << "  \"side_to_move_after_move\": \"" << color_to_string(side_to_move_after_move) << "\",\n";
    out << "  \"fen_root\": \"" << json_escape(fen_root) << "\",\n";
    out << "  \"fen_after_move\": \"" << json_escape(fen_after_move) << "\",\n";
    out << "  \"raw_root_best_cp\": " << classified_eval.raw_root_best_cp << ",\n";
    out << "  \"raw_post_move_cp\": " << classified_eval.raw_post_move_cp << ",\n";
    out << "  \"normalized_move_cp_for_root_side\": " << classified_eval.normalized_move_cp_for_root_side << ",\n";
    out << "  \"raw_loss_cp\": " << classified_eval.raw_loss_cp << ",\n";
    out << "  \"effective_loss_cp\": " << classified_eval.effective_loss_cp << ",\n";
    out << "  \"engine_max_loss_cp\": " << engine_max_loss_cp << ",\n";
    out << "  \"root_cached\": " << (root_cached ? "true" : "false") << ",\n";
    out << "  \"move_cached\": " << (move_cached ? "true" : "false") << ",\n";
    out << "  \"engine_id\": \"" << json_escape(engine_id) << "\",\n";
    out << "  \"engine_movetime_ms\": " << engine_movetime_ms << ",\n";
    out << "  \"engine_accept_policy\": \"" << json_escape(engine_accept_policy) << "\",\n";
    out << "  \"engine_reference_mode\": \"" << json_escape(engine_reference_mode) << "\"\n";
    out << "}\n";
}

std::filesystem::path stage_a_sqlite(const std::filesystem::path& bundle) {
    const auto p = bundle / "practical_risk_prestockfish.sqlite";
    if (!std::filesystem::exists(p)) throw std::runtime_error("missing Stage A sqlite: " + p.string());
    return p;
}

}  // namespace

void print_practical_risk_stockfish_overlay_usage(const std::string& program_name) {
    std::cout
        << "Usage: " << program_name << " --prestockfish-bundle <dir> --output-dir <dir> --artifact-id <id> --engine-path <path>\n"
        << "       [--engine-movetime-ms 200] [--engine-hash-mb 256] [--engine-threads 4]\n"
        << "       [--engine-accept-policy max_loss_cp] [--engine-max-loss-cp 40] [--engine-reference-mode root_best]\n"
        << "       [--baseline-prefix-limit 8] [--candidate-prefix-limit 8]\n";
}

PracticalRiskStockfishOverlayOptions parse_practical_risk_stockfish_overlay_cli(int argc, char** argv) {
    PracticalRiskStockfishOverlayOptions out;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto require_value = [&](const char* flag) -> std::string {
            if (i + 1 >= argc) throw std::runtime_error(std::string("missing value for ") + flag);
            return argv[++i];
        };

        if (arg == "--prestockfish-bundle") out.prestockfish_bundle = require_value("--prestockfish-bundle");
        else if (arg == "--output-dir") out.output_dir = require_value("--output-dir");
        else if (arg == "--artifact-id") out.artifact_id = require_value("--artifact-id");
        else if (arg == "--engine-path") out.engine_path = require_value("--engine-path");
        else if (arg == "--engine-movetime-ms") out.engine_movetime_ms = std::stoi(require_value("--engine-movetime-ms"));
        else if (arg == "--engine-hash-mb") out.engine_hash_mb = std::stoi(require_value("--engine-hash-mb"));
        else if (arg == "--engine-threads") out.engine_threads = std::stoi(require_value("--engine-threads"));
        else if (arg == "--engine-accept-policy") out.engine_accept_policy = require_value("--engine-accept-policy");
        else if (arg == "--engine-max-loss-cp") out.engine_max_loss_cp = std::stoi(require_value("--engine-max-loss-cp"));
        else if (arg == "--engine-reference-mode") out.engine_reference_mode = require_value("--engine-reference-mode");
        else if (arg == "--baseline-prefix-limit") out.baseline_prefix_limit = std::stoi(require_value("--baseline-prefix-limit"));
        else if (arg == "--candidate-prefix-limit") out.candidate_prefix_limit = std::stoi(require_value("--candidate-prefix-limit"));
        else if (arg == "--emit-progress-log") out.emit_progress_log = true;
        else if (arg == "--emit-status-json") out.emit_status_json = true;
        else if (arg == "--heartbeat-seconds") out.heartbeat_seconds = std::stoi(require_value("--heartbeat-seconds"));
        else if (arg == "--quiet-progress") out.quiet_progress = true;
        else if (arg == "--help" || arg == "-h") {
            print_practical_risk_stockfish_overlay_usage(argv[0]);
            std::exit(0);
        }
    }

    if (out.prestockfish_bundle.empty() || out.output_dir.empty() || out.artifact_id.empty() || out.engine_path.empty()) {
        throw std::runtime_error("missing required arguments --prestockfish-bundle --output-dir --artifact-id --engine-path");
    }
    if (out.engine_accept_policy != "max_loss_cp") throw std::runtime_error("only --engine-accept-policy=max_loss_cp supported");
    if (out.engine_reference_mode != "root_best") throw std::runtime_error("only --engine-reference-mode=root_best supported");
    if (out.engine_movetime_ms <= 0 || out.engine_hash_mb <= 0 || out.engine_threads <= 0) {
        throw std::runtime_error("engine options must be positive");
    }
    return out;
}

int run_practical_risk_stockfish_overlay(const PracticalRiskStockfishOverlayOptions& options) {
    const auto bundle_dir = options.output_dir / options.artifact_id;
    std::filesystem::create_directories(bundle_dir / "progress");

    ProgressReporter progress(ProgressReporterOptions{
        .quiet = options.quiet_progress,
        .emit_progress_log = options.emit_progress_log,
        .emit_status_json = options.emit_status_json,
        .heartbeat_seconds = options.heartbeat_seconds,
        .artifact_bundle_root = bundle_dir,
    });
    progress.start();

    std::vector<RootData> roots;
    std::unordered_map<std::string, PriorAccumulator> priors;
    std::vector<MoveEval> move_evals;
    std::vector<BaselineRow> baselines;
    std::vector<RootEngineThresholdRow> root_engine_thresholds;
    int cache_hits = 0;
    int cache_misses = 0;
    int baseline_evals = 0;
    int candidate_evals = 0;
    int found_baselines = 0;
    int missing_baselines = 0;
    int negative_raw_loss_rows_clamped = 0;
    bool saw_any_raw_loss = false;
    double min_raw_loss_cp = 0.0;
    double max_raw_loss_cp = 0.0;
    bool negative_loss_artifact_emitted = false;
    int book_count = 0;
    int best_count = 0;
    int excellent_count = 0;
    int good_count = 0;
    int fail_count = 0;
    int roots_with_good_inclusive_min = 0;
    int roots_with_good_exclusive_min = 0;

    try {
        progress.stage_started(ProgressStage::Preflight, "preflight stockfish overlay");
        const auto stage_a_db_path = stage_a_sqlite(options.prestockfish_bundle);
        progress.stage_completed("preflight complete");

        progress.stage_started(ProgressStage::LoadPrestockfish, "load-prestockfish");
        sqlite3* stage_a = nullptr;
        if (sqlite3_open(stage_a_db_path.string().c_str(), &stage_a) != SQLITE_OK) {
            throw std::runtime_error("unable to open Stage A sqlite");
        }
        std::unique_ptr<sqlite3, decltype(&sqlite3_close)> stage_a_guard(stage_a, sqlite3_close);

        int support_floor = 0;
        {
            char* err = nullptr;
            auto cb = [](void* data, int argc, char** argv, char**) -> int {
                if (argc > 0 && argv[0]) *static_cast<int*>(data) = std::stoi(argv[0]);
                return 0;
            };
            if (sqlite3_exec(stage_a, "SELECT value FROM artifact_metadata WHERE key='move_min_support'", cb, &support_floor, &err) != SQLITE_OK) {
                const std::string msg = err ? err : "sqlite error";
                sqlite3_free(err);
                throw std::runtime_error("failed reading move_min_support: " + msg);
            }
        }

        {
            char* err = nullptr;
            auto roots_cb = [](void* data, int argc, char** argv, char**) -> int {
                if (argc < 6) return 0;
                auto* vec = static_cast<std::vector<RootData>*>(data);
                RootData r;
                r.position_key = argv[0] ? argv[0] : "";
                r.rating_band = argv[1] ? argv[1] : "";
                r.time_control_id = argv[2] ? argv[2] : "";
                r.evaluating_side = argv[3] ? argv[3] : "";
                r.deep_total_plies = argv[4] ? std::stoi(argv[4]) : 0;
                r.deep_own_plies = argv[5] ? std::stoi(argv[5]) : 0;
                vec->push_back(std::move(r));
                return 0;
            };
            if (sqlite3_exec(stage_a, "SELECT position_key, rating_band, time_control_id, side_to_move, deep_total_plies, deep_own_plies FROM roots", roots_cb, &roots, &err) != SQLITE_OK) {
                const std::string msg = err ? err : "sqlite error";
                sqlite3_free(err);
                throw std::runtime_error("failed loading roots: " + msg);
            }
        }

        for (auto& root : roots) {
            root.support_floor = support_floor;
            char* err = nullptr;
            auto moves_cb = [](void* data, int argc, char** argv, char**) -> int {
                if (argc < 4) return 0;
                auto* target = static_cast<std::vector<RetainedMove>*>(data);
                RetainedMove m;
                m.move_uci = argv[0] ? argv[0] : "";
                m.move_support = argv[1] ? std::stoi(argv[1]) : 0;
                m.popularity_rank = argv[2] ? std::stoi(argv[2]) : 0;
                m.ceiling = argv[3] ? std::stod(argv[3]) : 0.0;
                target->push_back(std::move(m));
                return 0;
            };
            const std::string sql = "SELECT move_uci, move_support, popularity_rank, ceiling FROM root_moves WHERE position_key=" +
                sql_quote(root.position_key) + " ORDER BY popularity_rank ASC";
            if (sqlite3_exec(stage_a, sql.c_str(), moves_cb, &root.moves, &err) != SQLITE_OK) {
                const std::string msg = err ? err : "sqlite error";
                sqlite3_free(err);
                throw std::runtime_error("failed loading root moves: " + msg);
            }
        }

        progress.update([&](ProgressSnapshot& s) {
            s.risky_positions_considered = static_cast<int>(roots.size());
        });
        progress.stage_completed("load-prestockfish complete");

        progress.stage_started(ProgressStage::EngineBaselineScreen, "engine-baseline-screen");
        UciEngine engine(options.engine_path, options.engine_hash_mb, options.engine_threads);
        EngineEvalCache cache(bundle_dir / "engine_eval_cache.sqlite");
        const std::string ph = policy_hash(options);

        auto eval_at = [&](const std::string& position_key, const std::string& fen, const std::string& move_uci) -> std::pair<double, bool> {
            const std::string key = cache_key(position_key, move_uci, engine.engine_id(), options.engine_movetime_ms, ph);
            if (const auto cached = cache.get(key); cached.has_value()) {
                ++cache_hits;
                return {*cached, true};
            }
            const double raw_engine_cp = engine.eval_cp(fen, move_uci.empty() ? std::nullopt : std::optional<std::string>(move_uci), options.engine_movetime_ms);
            cache.put(key, position_key, move_uci, engine.engine_id(), options.engine_movetime_ms, ph, raw_engine_cp);
            ++cache_misses;
            return {raw_engine_cp, false};
        };

        for (const auto& root : roots) {
            BaselineRow baseline;
            baseline.position_key = root.position_key;

            if (root.moves.empty()) {
                baseline.reason_code = "no_retained_moves";
                baselines.push_back(std::move(baseline));
                ++missing_baselines;
                continue;
            }

            const auto maybe_board = reconstruct_board_from_position_key(root.position_key);
            if (!maybe_board.has_value()) {
                baseline.reason_code = "board_reconstruction_failed";
                baselines.push_back(std::move(baseline));
                ++missing_baselines;
                continue;
            }
            const ChessBoard board = *maybe_board;
            const std::string fen = board.to_fen();

            const Color root_side = board.side_to_move();
            const auto [raw_root_best_cp, root_cached] = eval_at(root.position_key, fen, "");
            ++baseline_evals;

            std::vector<RetainedMove> candidate_moves = root.moves;
            if (options.candidate_prefix_limit > 0 && static_cast<int>(candidate_moves.size()) > options.candidate_prefix_limit) {
                candidate_moves.resize(static_cast<std::size_t>(options.candidate_prefix_limit));
            }

            for (const auto& mv : candidate_moves) {
                auto resolved = resolve_uci_move(board, mv.move_uci);
                if (!resolved.has_value()) continue;
                const ChessBoard board_after_move = board.after_move(*resolved);

                const auto [raw_post_move_cp, move_cached] = eval_at(root.position_key, fen, mv.move_uci);
                ++candidate_evals;
                const Color side_to_move_after_move = board_after_move.side_to_move();
                if (side_to_move_after_move == root_side) {
                    throw std::runtime_error("post-move side-to-move unexpectedly unchanged in stage-b overlay");
                }
                const ClassifiedMoveEval classified_eval = make_validated_move_eval_row(
                    root,
                    mv,
                    raw_root_best_cp,
                    raw_post_move_cp,
                    root_cached,
                    move_cached,
                    options.engine_max_loss_cp,
                    kEngineQualityThresholdPolicy);
                if (!saw_any_raw_loss) {
                    min_raw_loss_cp = classified_eval.raw_loss_cp;
                    max_raw_loss_cp = classified_eval.raw_loss_cp;
                    saw_any_raw_loss = true;
                } else {
                    min_raw_loss_cp = std::min(min_raw_loss_cp, classified_eval.raw_loss_cp);
                    max_raw_loss_cp = std::max(max_raw_loss_cp, classified_eval.raw_loss_cp);
                }
                if (classified_eval.raw_loss_cp < 0.0) {
                    ++negative_raw_loss_rows_clamped;
                    if (!negative_loss_artifact_emitted) {
                        write_negative_loss_anomaly_artifact(
                            bundle_dir,
                            options.artifact_id,
                            root,
                            mv,
                            fen,
                            board_after_move.to_fen(),
                            root_side,
                            side_to_move_after_move,
                            classified_eval,
                            options.engine_max_loss_cp,
                            root_cached,
                            move_cached,
                            engine.engine_id(),
                            options.engine_movetime_ms,
                            options.engine_accept_policy,
                            options.engine_reference_mode);
                        negative_loss_artifact_emitted = true;
                    }
                }

                move_evals.push_back(classified_eval.row);
                const MoveEval& persisted_row = move_evals.back();
                if (persisted_row.engine_quality_class == "book") ++book_count;
                else if (persisted_row.engine_quality_class == "best") ++best_count;
                else if (persisted_row.engine_quality_class == "excellent") ++excellent_count;
                else if (persisted_row.engine_quality_class == "good") ++good_count;
                else if (persisted_row.engine_quality_class == "fail") ++fail_count;
                else throw std::runtime_error("stage-b invariant violation: unknown engine_quality_class");
                const bool accepted = persisted_row.is_engine_accepted == 1;

                if (accepted) {
                    const std::string bucket_key = root.rating_band + "|" + root.time_control_id + "|" + root.evaluating_side + "|" + std::to_string(root.deep_total_plies) +
                        "|" + std::to_string(root.deep_own_plies) + "|" + std::to_string(root.support_floor);
                    auto& acc = priors[bucket_key];
                    acc.rating_band = root.rating_band;
                    acc.time_control_id = root.time_control_id;
                    acc.evaluating_side = root.evaluating_side;
                    acc.deep_total_plies = root.deep_total_plies;
                    acc.deep_own_plies = root.deep_own_plies;
                    acc.support_floor = root.support_floor;
                    acc.move_count += 1;
                    acc.total_support += mv.move_support;
                    acc.weighted_ceiling_sum += (static_cast<double>(mv.move_support) * mv.ceiling);
                }
            }

            RootEngineThresholdRow root_thresholds;
            root_thresholds.position_key = root.position_key;
            for (const auto& rm : candidate_moves) {
                const auto it = std::find_if(move_evals.begin(), move_evals.end(), [&](const MoveEval& e) {
                    return e.position_key == root.position_key && e.move_uci == rm.move_uci;
                });
                if (it == move_evals.end()) continue;
                const bool is_fail = engine_quality_is_fail(it->engine_quality_class);
                if (is_fail) {
                    root_thresholds.failed_move_count += 1;
                } else {
                    root_thresholds.accepted_move_count += 1;
                    root_thresholds.accepted_move_count_good_inclusive += 1;
                    if (!root_thresholds.good_inclusive_min_ceiling.has_value() || it->ceiling < *root_thresholds.good_inclusive_min_ceiling) {
                        root_thresholds.good_inclusive_min_ceiling = it->ceiling;
                        root_thresholds.good_inclusive_min_move = it->move_uci;
                    }
                    if (engine_quality_in_good_exclusive(it->engine_quality_class)) {
                        root_thresholds.accepted_move_count_good_exclusive += 1;
                        if (!root_thresholds.good_exclusive_min_ceiling.has_value() || it->ceiling < *root_thresholds.good_exclusive_min_ceiling) {
                            root_thresholds.good_exclusive_min_ceiling = it->ceiling;
                            root_thresholds.good_exclusive_min_move = it->move_uci;
                        }
                    }
                }
            }
            if (root_thresholds.good_inclusive_min_ceiling.has_value()) ++roots_with_good_inclusive_min;
            if (root_thresholds.good_exclusive_min_ceiling.has_value()) ++roots_with_good_exclusive_min;
            root_engine_thresholds.push_back(std::move(root_thresholds));

            std::vector<RetainedMove> ranked = root.moves;
            if (options.baseline_prefix_limit > 0 && static_cast<int>(ranked.size()) > options.baseline_prefix_limit) {
                ranked.resize(static_cast<std::size_t>(options.baseline_prefix_limit));
            }
            bool found = false;
            for (const auto& rm : ranked) {
                const auto it = std::find_if(move_evals.begin(), move_evals.end(), [&](const MoveEval& e) {
                    return e.position_key == root.position_key && e.move_uci == rm.move_uci;
                });
                if (it != move_evals.end() && !engine_quality_is_fail(it->engine_quality_class)) {
                    baseline.accepted_baseline_move = rm.move_uci;
                    baseline.accepted_baseline_support = rm.move_support;
                    baseline.accepted_baseline_rank = rm.popularity_rank;
                    baseline.baseline_found = 1;
                    baseline.reason_code = "found_supported_engine_accepted_baseline";
                    found = true;
                    ++found_baselines;
                    break;
                }
            }
            if (!found) {
                baseline.reason_code = "no_supported_engine_accepted_baseline";
                ++missing_baselines;
            }
            baselines.push_back(std::move(baseline));
        }

        progress.update([&](ProgressSnapshot& s) {
            s.risky_positions_considered = static_cast<int>(roots.size());
            s.risky_candidates_evaluated = candidate_evals;
            s.risky_admitted_rows = found_baselines;
            s.risky_rejected_rows = missing_baselines;
        });
        progress.stage_completed("engine-baseline-screen complete");

        progress.stage_started(ProgressStage::ComputeAcceptedPriors, "compute-accepted-priors");
        progress.stage_completed("compute-accepted-priors complete");

        progress.stage_started(ProgressStage::WriteArtifacts, "write-artifacts");
        sqlite3* out_db = nullptr;
        const auto out_db_path = bundle_dir / "practical_risk_stockfish_overlay.sqlite";
        if (sqlite3_open(out_db_path.string().c_str(), &out_db) != SQLITE_OK) {
            throw std::runtime_error("unable to open output sqlite");
        }
        std::unique_ptr<sqlite3, decltype(&sqlite3_close)> out_guard(out_db, sqlite3_close);

        sqlite_exec(out_db,
            "BEGIN IMMEDIATE;"
            "CREATE TABLE artifact_metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);"
            "CREATE TABLE engine_metadata(engine_id TEXT NOT NULL, engine_path TEXT NOT NULL, engine_movetime_ms INTEGER NOT NULL, engine_hash_mb INTEGER NOT NULL, engine_threads INTEGER NOT NULL, engine_accept_policy TEXT NOT NULL, engine_max_loss_cp INTEGER NOT NULL, engine_reference_mode TEXT NOT NULL, policy_hash TEXT NOT NULL);"
            "CREATE TABLE move_engine_evals(position_key TEXT NOT NULL, move_uci TEXT NOT NULL, move_support INTEGER NOT NULL, popularity_rank INTEGER NOT NULL, root_best_cp REAL NOT NULL, move_cp REAL NOT NULL, raw_loss_cp REAL NOT NULL, loss_cp REAL NOT NULL, engine_quality_class TEXT NOT NULL, ceiling REAL NOT NULL, is_engine_accepted INTEGER NOT NULL, is_engine_fail INTEGER NOT NULL, eval_source TEXT NOT NULL, cache_hit INTEGER NOT NULL, PRIMARY KEY(position_key, move_uci));"
            "CREATE TABLE root_direct_baselines(position_key TEXT PRIMARY KEY, accepted_baseline_move TEXT NULL, accepted_baseline_support INTEGER NOT NULL, accepted_baseline_rank INTEGER NOT NULL, baseline_found INTEGER NOT NULL, reason_code TEXT NOT NULL);"
            "CREATE TABLE root_engine_thresholds(position_key TEXT PRIMARY KEY, good_inclusive_min_ceiling REAL NULL, good_exclusive_min_ceiling REAL NULL, good_inclusive_min_move TEXT NULL, good_exclusive_min_move TEXT NULL, accepted_move_count INTEGER NOT NULL, accepted_move_count_good_inclusive INTEGER NOT NULL, accepted_move_count_good_exclusive INTEGER NOT NULL, failed_move_count INTEGER NOT NULL);"
            "CREATE TABLE accepted_bucket_ceiling_priors(bucket_key TEXT PRIMARY KEY, rating_band TEXT NOT NULL, time_control_id TEXT NOT NULL, evaluating_side TEXT NOT NULL, deep_total_plies INTEGER NOT NULL, deep_own_plies INTEGER NOT NULL, support_floor INTEGER NOT NULL, weighted_ceiling REAL NOT NULL, move_count INTEGER NOT NULL, total_support INTEGER NOT NULL);"
        );

        sqlite3_stmt* stmt = nullptr;
        sqlite3_prepare_v2(out_db, "INSERT INTO artifact_metadata(key, value) VALUES(?1, ?2)", -1, &stmt, nullptr);
        auto insert_kv = [&](const std::string& k, const std::string& v) {
            sqlite3_bind_text(stmt, 1, k.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 2, v.c_str(), -1, SQLITE_TRANSIENT);
            if (sqlite3_step(stmt) != SQLITE_DONE) throw std::runtime_error("artifact_metadata insert failed");
            sqlite3_reset(stmt);
            sqlite3_clear_bindings(stmt);
        };
        insert_kv("artifact_role", "practical_risk_stockfish_overlay");
        insert_kv("stockfish_used", "true");
        insert_kv("external_book_dependency_used", "false");
        insert_kv("source_stage_a_bundle", options.prestockfish_bundle.string());
        insert_kv("source_stage_a_artifact_id", options.prestockfish_bundle.filename().string());
        sqlite3_finalize(stmt);

        sqlite3_stmt* em = nullptr;
        sqlite3_prepare_v2(out_db,
            "INSERT INTO engine_metadata(engine_id, engine_path, engine_movetime_ms, engine_hash_mb, engine_threads, engine_accept_policy, engine_max_loss_cp, engine_reference_mode, policy_hash) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            -1,
            &em,
            nullptr);
        sqlite3_bind_text(em, 1, engine.engine_id().c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(em, 2, options.engine_path.string().c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(em, 3, options.engine_movetime_ms);
        sqlite3_bind_int(em, 4, options.engine_hash_mb);
        sqlite3_bind_int(em, 5, options.engine_threads);
        sqlite3_bind_text(em, 6, options.engine_accept_policy.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(em, 7, options.engine_max_loss_cp);
        sqlite3_bind_text(em, 8, options.engine_reference_mode.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(em, 9, ph.c_str(), -1, SQLITE_TRANSIENT);
        if (sqlite3_step(em) != SQLITE_DONE) throw std::runtime_error("engine_metadata insert failed");
        sqlite3_finalize(em);

        sqlite3_stmt* me = nullptr;
        sqlite3_prepare_v2(out_db,
            "INSERT INTO move_engine_evals(position_key, move_uci, move_support, popularity_rank, root_best_cp, move_cp, raw_loss_cp, loss_cp, engine_quality_class, ceiling, is_engine_accepted, is_engine_fail, eval_source, cache_hit) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
            -1,
            &me,
            nullptr);
        for (const auto& row : move_evals) {
            sqlite3_bind_text(me, 1, row.position_key.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(me, 2, row.move_uci.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(me, 3, row.move_support);
            sqlite3_bind_int(me, 4, row.popularity_rank);
            sqlite3_bind_double(me, 5, row.root_best_cp);
            sqlite3_bind_double(me, 6, row.move_cp);
            sqlite3_bind_double(me, 7, row.raw_loss_cp);
            sqlite3_bind_double(me, 8, row.loss_cp);
            sqlite3_bind_text(me, 9, row.engine_quality_class.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_double(me, 10, row.ceiling);
            sqlite3_bind_int(me, 11, row.is_engine_accepted);
            sqlite3_bind_int(me, 12, row.is_engine_fail);
            sqlite3_bind_text(me, 13, row.eval_source.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(me, 14, row.cache_hit);
            if (sqlite3_step(me) != SQLITE_DONE) throw std::runtime_error("move_engine_evals insert failed");
            sqlite3_reset(me);
            sqlite3_clear_bindings(me);
        }
        sqlite3_finalize(me);

        sqlite3_stmt* rb = nullptr;
        sqlite3_prepare_v2(out_db,
            "INSERT INTO root_direct_baselines(position_key, accepted_baseline_move, accepted_baseline_support, accepted_baseline_rank, baseline_found, reason_code) VALUES(?1,?2,?3,?4,?5,?6)",
            -1,
            &rb,
            nullptr);
        for (const auto& row : baselines) {
            sqlite3_bind_text(rb, 1, row.position_key.c_str(), -1, SQLITE_TRANSIENT);
            if (row.accepted_baseline_move.has_value()) sqlite3_bind_text(rb, 2, row.accepted_baseline_move->c_str(), -1, SQLITE_TRANSIENT);
            else sqlite3_bind_null(rb, 2);
            sqlite3_bind_int(rb, 3, row.accepted_baseline_support);
            sqlite3_bind_int(rb, 4, row.accepted_baseline_rank);
            sqlite3_bind_int(rb, 5, row.baseline_found);
            sqlite3_bind_text(rb, 6, row.reason_code.c_str(), -1, SQLITE_TRANSIENT);
            if (sqlite3_step(rb) != SQLITE_DONE) throw std::runtime_error("root_direct_baselines insert failed");
            sqlite3_reset(rb);
            sqlite3_clear_bindings(rb);
        }
        sqlite3_finalize(rb);

        sqlite3_stmt* rt = nullptr;
        sqlite3_prepare_v2(out_db,
            "INSERT INTO root_engine_thresholds(position_key, good_inclusive_min_ceiling, good_exclusive_min_ceiling, good_inclusive_min_move, good_exclusive_min_move, accepted_move_count, accepted_move_count_good_inclusive, accepted_move_count_good_exclusive, failed_move_count) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            -1,
            &rt,
            nullptr);
        for (const auto& row : root_engine_thresholds) {
            sqlite3_bind_text(rt, 1, row.position_key.c_str(), -1, SQLITE_TRANSIENT);
            if (row.good_inclusive_min_ceiling.has_value()) sqlite3_bind_double(rt, 2, *row.good_inclusive_min_ceiling);
            else sqlite3_bind_null(rt, 2);
            if (row.good_exclusive_min_ceiling.has_value()) sqlite3_bind_double(rt, 3, *row.good_exclusive_min_ceiling);
            else sqlite3_bind_null(rt, 3);
            if (row.good_inclusive_min_move.has_value()) sqlite3_bind_text(rt, 4, row.good_inclusive_min_move->c_str(), -1, SQLITE_TRANSIENT);
            else sqlite3_bind_null(rt, 4);
            if (row.good_exclusive_min_move.has_value()) sqlite3_bind_text(rt, 5, row.good_exclusive_min_move->c_str(), -1, SQLITE_TRANSIENT);
            else sqlite3_bind_null(rt, 5);
            sqlite3_bind_int(rt, 6, row.accepted_move_count);
            sqlite3_bind_int(rt, 7, row.accepted_move_count_good_inclusive);
            sqlite3_bind_int(rt, 8, row.accepted_move_count_good_exclusive);
            sqlite3_bind_int(rt, 9, row.failed_move_count);
            if (sqlite3_step(rt) != SQLITE_DONE) throw std::runtime_error("root_engine_thresholds insert failed");
            sqlite3_reset(rt);
            sqlite3_clear_bindings(rt);
        }
        sqlite3_finalize(rt);

        sqlite3_stmt* pri = nullptr;
        sqlite3_prepare_v2(out_db,
            "INSERT INTO accepted_bucket_ceiling_priors(bucket_key, rating_band, time_control_id, evaluating_side, deep_total_plies, deep_own_plies, support_floor, weighted_ceiling, move_count, total_support) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            -1,
            &pri,
            nullptr);
        for (const auto& [bucket_key, acc] : priors) {
            if (acc.total_support <= 0) continue;
            const double weighted_ceiling = acc.weighted_ceiling_sum / static_cast<double>(acc.total_support);
            sqlite3_bind_text(pri, 1, bucket_key.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(pri, 2, acc.rating_band.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(pri, 3, acc.time_control_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(pri, 4, acc.evaluating_side.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(pri, 5, acc.deep_total_plies);
            sqlite3_bind_int(pri, 6, acc.deep_own_plies);
            sqlite3_bind_int(pri, 7, acc.support_floor);
            sqlite3_bind_double(pri, 8, weighted_ceiling);
            sqlite3_bind_int(pri, 9, acc.move_count);
            sqlite3_bind_int(pri, 10, acc.total_support);
            if (sqlite3_step(pri) != SQLITE_DONE) throw std::runtime_error("accepted_bucket_ceiling_priors insert failed");
            sqlite3_reset(pri);
            sqlite3_clear_bindings(pri);
        }
        sqlite3_finalize(pri);
        sqlite_exec(out_db, "COMMIT;");

        {
            std::ofstream manifest(bundle_dir / "manifest.json");
            manifest << "{\n";
            manifest << "  \"artifact_id\": \"" << json_escape(options.artifact_id) << "\",\n";
            manifest << "  \"artifact_role\": \"practical_risk_stockfish_overlay\",\n";
            manifest << "  \"stockfish_used\": true,\n";
            manifest << "  \"external_book_dependency_used\": false,\n";
            manifest << "  \"source_stage_a_bundle\": \"" << json_escape(options.prestockfish_bundle.string()) << "\",\n";
            manifest << "  \"engine_identity\": \"" << json_escape(engine.engine_id()) << "\",\n";
            manifest << "  \"engine_movetime_ms\": " << options.engine_movetime_ms << ",\n";
            manifest << "  \"engine_threads\": " << options.engine_threads << ",\n";
            manifest << "  \"engine_hash_mb\": " << options.engine_hash_mb << ",\n";
            manifest << "  \"engine_accept_policy\": \"" << json_escape(options.engine_accept_policy) << "\",\n";
            manifest << "  \"engine_max_loss_cp\": " << options.engine_max_loss_cp << ",\n";
            manifest << "  \"engine_quality_policy\": \"loss_cp_tiers\",\n";
            manifest << "  \"book_max_loss_cp\": " << kEngineQualityThresholdPolicy.book_max_loss_cp << ",\n";
            manifest << "  \"best_max_loss_cp\": " << effective_threshold(static_cast<double>(options.engine_max_loss_cp), kEngineQualityThresholdPolicy.best_max_loss_cp) << ",\n";
            manifest << "  \"excellent_max_loss_cp\": " << effective_threshold(static_cast<double>(options.engine_max_loss_cp), kEngineQualityThresholdPolicy.excellent_max_loss_cp) << ",\n";
            manifest << "  \"good_max_loss_cp\": " << options.engine_max_loss_cp << ",\n";
            manifest << "  \"engine_reference_mode\": \"" << json_escape(options.engine_reference_mode) << "\",\n";
            manifest << "  \"baseline_prefix_limit\": " << options.baseline_prefix_limit << ",\n";
            manifest << "  \"candidate_prefix_limit\": " << options.candidate_prefix_limit << ",\n";
            manifest << "  \"build_notes\": [\"Stage B perspective normalization fix applied\"]\n";
            manifest << "}\n";
        }

        {
            std::ofstream summary(bundle_dir / "build_summary.txt");
            summary << "overlay practical-risk stockfish build complete\n";
            summary << "roots=" << roots.size() << "\n";
            summary << "baseline_engine_evaluations=" << baseline_evals << "\n";
            summary << "candidate_engine_evaluations=" << candidate_evals << "\n";
            summary << "cache_hits=" << cache_hits << "\n";
            summary << "cache_misses=" << cache_misses << "\n";
            summary << "roots_with_direct_accepted_baseline=" << found_baselines << "\n";
            summary << "roots_without_direct_accepted_baseline=" << missing_baselines << "\n";
            summary << "negative_raw_loss_rows_clamped=" << negative_raw_loss_rows_clamped << "\n";
            summary << "book_count=" << book_count << "\n";
            summary << "best_count=" << best_count << "\n";
            summary << "excellent_count=" << excellent_count << "\n";
            summary << "good_count=" << good_count << "\n";
            summary << "fail_count=" << fail_count << "\n";
            summary << "roots_with_good_inclusive_min=" << roots_with_good_inclusive_min << "\n";
            summary << "roots_with_good_exclusive_min=" << roots_with_good_exclusive_min << "\n";
            summary << "min_raw_loss_cp=" << (saw_any_raw_loss ? min_raw_loss_cp : 0.0) << "\n";
            summary << "max_raw_loss_cp=" << (saw_any_raw_loss ? max_raw_loss_cp : 0.0) << "\n";
        }

        {
            std::ofstream summary(bundle_dir / "summary.json");
            summary << "{\n"
                    << "  \"roots\": " << roots.size() << ",\n"
                    << "  \"baseline_engine_evaluations\": " << baseline_evals << ",\n"
                    << "  \"candidate_engine_evaluations\": " << candidate_evals << ",\n"
                    << "  \"cache_hits\": " << cache_hits << ",\n"
                    << "  \"cache_misses\": " << cache_misses << ",\n"
                    << "  \"roots_with_direct_accepted_baseline\": " << found_baselines << ",\n"
                    << "  \"roots_without_direct_accepted_baseline\": " << missing_baselines << ",\n"
                    << "  \"negative_raw_loss_rows_clamped\": " << negative_raw_loss_rows_clamped << ",\n"
                    << "  \"book_count\": " << book_count << ",\n"
                    << "  \"best_count\": " << best_count << ",\n"
                    << "  \"excellent_count\": " << excellent_count << ",\n"
                    << "  \"good_count\": " << good_count << ",\n"
                    << "  \"fail_count\": " << fail_count << ",\n"
                    << "  \"roots_with_good_inclusive_min\": " << roots_with_good_inclusive_min << ",\n"
                    << "  \"roots_with_good_exclusive_min\": " << roots_with_good_exclusive_min << ",\n"
                    << "  \"min_raw_loss_cp\": " << (saw_any_raw_loss ? min_raw_loss_cp : 0.0) << ",\n"
                    << "  \"max_raw_loss_cp\": " << (saw_any_raw_loss ? max_raw_loss_cp : 0.0) << "\n"
                    << "}\n";
        }
        progress.stage_completed("write-artifacts complete");

        progress.stage_started(ProgressStage::Finalize, "finalize");
        progress.update([&](ProgressSnapshot& s) { s.risky_estimated_remaining_work = 0; });
        progress.stage_completed("finalize complete");
        progress.finish();
        return 0;
    } catch (...) {
        progress.stage_failed("stockfish-overlay failed");
        progress.finish();
        throw;
    }
}

}  // namespace otcb
