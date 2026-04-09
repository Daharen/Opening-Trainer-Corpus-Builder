#include "otcb/practical_risk/final.hpp"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <sqlite3.h>

#include "otcb/progress.hpp"

namespace otcb {
namespace {

struct StageAMove {
    int move_support = 0;
    int popularity_rank = 0;
    double ceiling = 0.0;
};

struct StageARoot {
    std::string position_key;
    std::unordered_map<std::string, StageAMove> moves;
};

struct StageBMoveEval {
    std::string position_key;
    std::string move_uci;
    std::string engine_quality_class;
    int is_engine_accepted = 0;
    int is_engine_fail = 0;
    double raw_loss_cp = 0.0;
    double loss_cp = 0.0;
};

struct StageBRootThreshold {
    std::optional<std::string> good_inclusive_min_move;
    std::optional<double> good_inclusive_min_ceiling;
    std::optional<std::string> good_exclusive_min_move;
    std::optional<double> good_exclusive_min_ceiling;
};

struct FinalMoveAdmissionRow {
    std::string position_key;
    std::string move_uci;
    int move_support = 0;
    int popularity_rank = 0;
    double ceiling = 0.0;
    std::string engine_quality_class;
    int is_engine_accepted = 0;
    int is_engine_fail = 0;
    double raw_loss_cp = 0.0;
    double loss_cp = 0.0;
    std::optional<std::string> good_inclusive_min_move;
    std::optional<double> good_inclusive_min_ceiling;
    std::optional<std::string> good_exclusive_min_move;
    std::optional<double> good_exclusive_min_ceiling;
    int admitted_if_good_accepted = 0;
    int admitted_if_good_rejected = 0;
    std::string admission_reason_good_accepted;
    std::string admission_reason_good_rejected;
};

struct RootFinalThresholdRow {
    std::string position_key;
    std::optional<std::string> good_inclusive_min_move;
    std::optional<double> good_inclusive_min_ceiling;
    std::optional<std::string> good_exclusive_min_move;
    std::optional<double> good_exclusive_min_ceiling;
    int total_move_count = 0;
    int accepted_move_count = 0;
    int failed_move_count = 0;
    int admitted_move_count_good_accepted = 0;
    int admitted_move_count_good_rejected = 0;
    int admitted_failed_move_count_good_accepted = 0;
    int admitted_failed_move_count_good_rejected = 0;
};

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

void sqlite_exec(sqlite3* db, const char* sql) {
    char* err = nullptr;
    if (sqlite3_exec(db, sql, nullptr, nullptr, &err) != SQLITE_OK) {
        const std::string msg = err ? err : "sqlite error";
        sqlite3_free(err);
        throw std::runtime_error(msg);
    }
}

bool table_exists(sqlite3* db, const std::string& table_name) {
    int exists = 0;
    char* err = nullptr;
    const std::string sql = "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='" + table_name + "')";
    const auto cb = [](void* data, int argc, char** argv, char**) -> int {
        if (argc > 0 && argv[0]) *static_cast<int*>(data) = std::stoi(argv[0]);
        return 0;
    };
    if (sqlite3_exec(db, sql.c_str(), cb, &exists, &err) != SQLITE_OK) {
        const std::string msg = err ? err : "sqlite error";
        sqlite3_free(err);
        throw std::runtime_error("table_exists query failed: " + msg);
    }
    return exists;
}

std::filesystem::path stage_a_sqlite(const std::filesystem::path& bundle) {
    const auto p = bundle / "practical_risk_prestockfish.sqlite";
    if (!std::filesystem::exists(p)) throw std::runtime_error("missing Stage A sqlite: " + p.string());
    return p;
}

std::filesystem::path stage_b_sqlite(const std::filesystem::path& bundle) {
    const auto p = bundle / "practical_risk_stockfish_overlay.sqlite";
    if (!std::filesystem::exists(p)) throw std::runtime_error("missing Stage B sqlite: " + p.string());
    return p;
}

bool class_auto_accepted_in_good_inclusive(const std::string& quality_class) {
    return quality_class == "book" || quality_class == "best" || quality_class == "excellent" || quality_class == "good";
}

bool class_auto_accepted_in_good_exclusive(const std::string& quality_class) {
    return quality_class == "book" || quality_class == "best" || quality_class == "excellent";
}

bool class_is_fail(const std::string& quality_class) {
    return quality_class == "fail";
}

bool class_is_good(const std::string& quality_class) {
    return quality_class == "good";
}

bool class_known(const std::string& quality_class) {
    return class_auto_accepted_in_good_exclusive(quality_class) || class_is_good(quality_class) || class_is_fail(quality_class);
}

}  // namespace

void print_practical_risk_final_usage(const std::string& program_name) {
    std::cout
        << "Usage: " << program_name << " --prestockfish-bundle <dir> --stockfish-overlay-bundle <dir> --output-dir <dir> --artifact-id <id>\n"
        << "       [--emit-progress-log] [--emit-status-json] [--heartbeat-seconds 30] [--quiet-progress]\n";
}

PracticalRiskFinalOptions parse_practical_risk_final_cli(int argc, char** argv) {
    PracticalRiskFinalOptions out;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto require_value = [&](const char* flag) -> std::string {
            if (i + 1 >= argc) throw std::runtime_error(std::string("missing value for ") + flag);
            return argv[++i];
        };

        if (arg == "--prestockfish-bundle") out.prestockfish_bundle = require_value("--prestockfish-bundle");
        else if (arg == "--stockfish-overlay-bundle") out.stockfish_overlay_bundle = require_value("--stockfish-overlay-bundle");
        else if (arg == "--output-dir") out.output_dir = require_value("--output-dir");
        else if (arg == "--artifact-id") out.artifact_id = require_value("--artifact-id");
        else if (arg == "--emit-progress-log") out.emit_progress_log = true;
        else if (arg == "--emit-status-json") out.emit_status_json = true;
        else if (arg == "--heartbeat-seconds") out.heartbeat_seconds = std::stoi(require_value("--heartbeat-seconds"));
        else if (arg == "--quiet-progress") out.quiet_progress = true;
        else if (arg == "--help" || arg == "-h") {
            print_practical_risk_final_usage(argv[0]);
            std::exit(0);
        }
    }

    if (out.prestockfish_bundle.empty() || out.stockfish_overlay_bundle.empty() || out.output_dir.empty() || out.artifact_id.empty()) {
        throw std::runtime_error("missing required arguments --prestockfish-bundle --stockfish-overlay-bundle --output-dir --artifact-id");
    }
    if (out.heartbeat_seconds <= 0) throw std::runtime_error("--heartbeat-seconds must be positive");
    return out;
}

int run_practical_risk_final(const PracticalRiskFinalOptions& options) {
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

    std::unordered_map<std::string, StageARoot> stage_a_roots;
    std::vector<StageBMoveEval> stage_b_moves;
    std::unordered_map<std::string, StageBRootThreshold> stage_b_thresholds;
    std::vector<FinalMoveAdmissionRow> final_rows;
    std::vector<RootFinalThresholdRow> root_rows;

    int admitted_move_count_good_accepted = 0;
    int admitted_move_count_good_rejected = 0;
    int admitted_failed_move_count_good_accepted = 0;
    int admitted_failed_move_count_good_rejected = 0;
    int roots_with_good_inclusive_min = 0;
    int roots_with_good_exclusive_min = 0;

    try {
        progress.stage_started(ProgressStage::Preflight, "preflight");
        const auto stage_a_db_path = stage_a_sqlite(options.prestockfish_bundle);
        const auto stage_b_db_path = stage_b_sqlite(options.stockfish_overlay_bundle);

        sqlite3* stage_a = nullptr;
        if (sqlite3_open(stage_a_db_path.string().c_str(), &stage_a) != SQLITE_OK) {
            throw std::runtime_error("unable to open Stage A sqlite");
        }
        std::unique_ptr<sqlite3, decltype(&sqlite3_close)> stage_a_guard(stage_a, sqlite3_close);

        sqlite3* stage_b = nullptr;
        if (sqlite3_open(stage_b_db_path.string().c_str(), &stage_b) != SQLITE_OK) {
            throw std::runtime_error("unable to open Stage B sqlite");
        }
        std::unique_ptr<sqlite3, decltype(&sqlite3_close)> stage_b_guard(stage_b, sqlite3_close);

        if (!table_exists(stage_a, "roots") || !table_exists(stage_a, "root_moves")) {
            throw std::runtime_error("Stage A sqlite missing required tables roots/root_moves");
        }
        if (!table_exists(stage_b, "move_engine_evals") || !table_exists(stage_b, "root_direct_baselines") || !table_exists(stage_b, "root_engine_thresholds")) {
            throw std::runtime_error("Stage B sqlite missing required tables move_engine_evals/root_direct_baselines/root_engine_thresholds");
        }

        progress.stage_completed("preflight complete");

        progress.stage_started(ProgressStage::LoadPrestockfish, "load-stage-a");
        {
            char* err = nullptr;
            const auto roots_cb = [](void* data, int argc, char** argv, char**) -> int {
                if (argc < 1 || !argv[0]) return 0;
                auto* roots = static_cast<std::unordered_map<std::string, StageARoot>*>(data);
                StageARoot root;
                root.position_key = argv[0];
                roots->emplace(root.position_key, std::move(root));
                return 0;
            };
            if (sqlite3_exec(stage_a, "SELECT position_key FROM roots", roots_cb, &stage_a_roots, &err) != SQLITE_OK) {
                const std::string msg = err ? err : "sqlite error";
                sqlite3_free(err);
                throw std::runtime_error("failed loading Stage A roots: " + msg);
            }

            const auto moves_cb = [](void* data, int argc, char** argv, char**) -> int {
                if (argc < 5) return 0;
                auto* roots = static_cast<std::unordered_map<std::string, StageARoot>*>(data);
                const std::string position_key = argv[0] ? argv[0] : "";
                const std::string move_uci = argv[1] ? argv[1] : "";
                auto it = roots->find(position_key);
                if (it == roots->end()) return 1;
                it->second.moves[move_uci] = StageAMove{
                    .move_support = argv[2] ? std::stoi(argv[2]) : 0,
                    .popularity_rank = argv[3] ? std::stoi(argv[3]) : 0,
                    .ceiling = argv[4] ? std::stod(argv[4]) : 0.0,
                };
                return 0;
            };
            if (sqlite3_exec(stage_a, "SELECT position_key, move_uci, move_support, popularity_rank, ceiling FROM root_moves", moves_cb, &stage_a_roots, &err) != SQLITE_OK) {
                const std::string msg = err ? err : "sqlite error";
                sqlite3_free(err);
                throw std::runtime_error("failed loading Stage A root moves: " + msg);
            }
        }
        progress.stage_completed("load-stage-a complete");

        progress.stage_started(ProgressStage::LoadPrestockfish, "load-stage-b");
        {
            char* err = nullptr;
            const auto moves_cb = [](void* data, int argc, char** argv, char**) -> int {
                if (argc < 7) return 0;
                auto* rows = static_cast<std::vector<StageBMoveEval>*>(data);
                rows->push_back(StageBMoveEval{
                    .position_key = argv[0] ? argv[0] : "",
                    .move_uci = argv[1] ? argv[1] : "",
                    .engine_quality_class = argv[2] ? argv[2] : "",
                    .is_engine_accepted = argv[3] ? std::stoi(argv[3]) : 0,
                    .is_engine_fail = argv[4] ? std::stoi(argv[4]) : 0,
                    .raw_loss_cp = argv[5] ? std::stod(argv[5]) : 0.0,
                    .loss_cp = argv[6] ? std::stod(argv[6]) : 0.0,
                });
                return 0;
            };
            if (sqlite3_exec(stage_b, "SELECT position_key, move_uci, engine_quality_class, is_engine_accepted, is_engine_fail, raw_loss_cp, loss_cp FROM move_engine_evals", moves_cb, &stage_b_moves, &err) != SQLITE_OK) {
                const std::string msg = err ? err : "sqlite error";
                sqlite3_free(err);
                throw std::runtime_error("failed loading Stage B move_engine_evals: " + msg);
            }

            const auto thresholds_cb = [](void* data, int argc, char** argv, char**) -> int {
                if (argc < 5) return 0;
                auto* thresholds = static_cast<std::unordered_map<std::string, StageBRootThreshold>*>(data);
                const std::string position_key = argv[0] ? argv[0] : "";
                StageBRootThreshold t;
                if (argv[1]) t.good_inclusive_min_move = argv[1];
                if (argv[2]) t.good_inclusive_min_ceiling = std::stod(argv[2]);
                if (argv[3]) t.good_exclusive_min_move = argv[3];
                if (argv[4]) t.good_exclusive_min_ceiling = std::stod(argv[4]);
                (*thresholds)[position_key] = std::move(t);
                return 0;
            };
            if (sqlite3_exec(stage_b, "SELECT position_key, good_inclusive_min_move, good_inclusive_min_ceiling, good_exclusive_min_move, good_exclusive_min_ceiling FROM root_engine_thresholds", thresholds_cb, &stage_b_thresholds, &err) != SQLITE_OK) {
                const std::string msg = err ? err : "sqlite error";
                sqlite3_free(err);
                throw std::runtime_error("failed loading Stage B root_engine_thresholds: " + msg);
            }
        }

        if (stage_b_moves.empty()) throw std::runtime_error("Stage B move_engine_evals has no rows");
        if (stage_b_thresholds.empty()) throw std::runtime_error("Stage B root_engine_thresholds has no rows");

        std::set<std::string> stage_b_move_roots;
        for (const auto& row : stage_b_moves) stage_b_move_roots.insert(row.position_key);

        int baseline_root_count = 0;
        {
            char* err = nullptr;
            const auto cb = [](void* data, int argc, char** argv, char**) -> int {
                if (argc > 0 && argv[0]) *static_cast<int*>(data) = std::stoi(argv[0]);
                return 0;
            };
            if (sqlite3_exec(stage_b, "SELECT COUNT(*) FROM root_direct_baselines", cb, &baseline_root_count, &err) != SQLITE_OK) {
                const std::string msg = err ? err : "sqlite error";
                sqlite3_free(err);
                throw std::runtime_error("failed counting Stage B root_direct_baselines: " + msg);
            }
        }

        const int move_root_count = static_cast<int>(stage_b_move_roots.size());
        const int threshold_root_count = static_cast<int>(stage_b_thresholds.size());
        int matched_root_count = 0;
        for (const auto& root_key : stage_b_move_roots) {
            if (stage_a_roots.find(root_key) != stage_a_roots.end()) ++matched_root_count;
        }
        if (matched_root_count != move_root_count) {
            throw std::runtime_error("preflight consistency error: Stage B has position_key values not present in Stage A roots");
        }
        if (threshold_root_count != move_root_count || baseline_root_count != move_root_count) {
            throw std::runtime_error("preflight consistency error: Stage B root counts are incoherent across move_engine_evals/root_engine_thresholds/root_direct_baselines");
        }

        for (const auto& row : stage_b_moves) {
            const auto root_it = stage_a_roots.find(row.position_key);
            if (root_it == stage_a_roots.end()) {
                throw std::runtime_error("preflight consistency error: Stage B position_key missing in Stage A roots");
            }
            if (root_it->second.moves.find(row.move_uci) == root_it->second.moves.end()) {
                throw std::runtime_error("preflight consistency error: Stage B move missing in Stage A root_moves for position_key=" + row.position_key + " move=" + row.move_uci);
            }
            if (stage_b_thresholds.find(row.position_key) == stage_b_thresholds.end()) {
                throw std::runtime_error("preflight consistency error: Stage B root threshold missing for position_key=" + row.position_key);
            }
            if (!class_known(row.engine_quality_class)) {
                throw std::runtime_error("Stage B move_engine_evals contains unsupported engine_quality_class=" + row.engine_quality_class);
            }
        }

        progress.update([&](ProgressSnapshot& s) {
            s.risky_positions_considered = static_cast<int>(stage_b_move_roots.size());
            s.risky_candidates_evaluated = static_cast<int>(stage_b_moves.size());
        });
        progress.stage_completed("load-stage-b complete");

        progress.stage_started(ProgressStage::ComputeAcceptedPriors, "compute-final-admissions");
        std::unordered_map<std::string, RootFinalThresholdRow> root_acc;
        for (const auto& row : stage_b_moves) {
            const auto root_it = stage_a_roots.find(row.position_key);
            const auto move_it = root_it->second.moves.find(row.move_uci);
            const StageAMove& a_move = move_it->second;
            const StageBRootThreshold& threshold = stage_b_thresholds.at(row.position_key);

            FinalMoveAdmissionRow out;
            out.position_key = row.position_key;
            out.move_uci = row.move_uci;
            out.move_support = a_move.move_support;
            out.popularity_rank = a_move.popularity_rank;
            out.ceiling = a_move.ceiling;
            out.engine_quality_class = row.engine_quality_class;
            out.is_engine_accepted = row.is_engine_accepted;
            out.is_engine_fail = row.is_engine_fail;
            out.raw_loss_cp = row.raw_loss_cp;
            out.loss_cp = row.loss_cp;
            out.good_inclusive_min_move = threshold.good_inclusive_min_move;
            out.good_inclusive_min_ceiling = threshold.good_inclusive_min_ceiling;
            out.good_exclusive_min_move = threshold.good_exclusive_min_move;
            out.good_exclusive_min_ceiling = threshold.good_exclusive_min_ceiling;

            if (class_auto_accepted_in_good_inclusive(out.engine_quality_class)) {
                out.admitted_if_good_accepted = 1;
                out.admission_reason_good_accepted = "engine_class_accepted";
            } else if (class_is_fail(out.engine_quality_class)) {
                if (!out.good_inclusive_min_ceiling.has_value()) {
                    out.admitted_if_good_accepted = 0;
                    out.admission_reason_good_accepted = "no_good_inclusive_min_available";
                } else if (out.ceiling >= *out.good_inclusive_min_ceiling) {
                    out.admitted_if_good_accepted = 1;
                    out.admission_reason_good_accepted = "failed_move_clears_good_inclusive_min";
                } else {
                    out.admitted_if_good_accepted = 0;
                    out.admission_reason_good_accepted = "failed_move_below_good_inclusive_min";
                }
            }

            if (class_auto_accepted_in_good_exclusive(out.engine_quality_class)) {
                out.admitted_if_good_rejected = 1;
                out.admission_reason_good_rejected = "engine_class_accepted_excluding_good";
            } else if (class_is_good(out.engine_quality_class)) {
                out.admitted_if_good_rejected = 0;
                out.admission_reason_good_rejected = "good_rejected_in_strict_mode";
            } else if (class_is_fail(out.engine_quality_class)) {
                if (!out.good_exclusive_min_ceiling.has_value()) {
                    out.admitted_if_good_rejected = 0;
                    out.admission_reason_good_rejected = "no_good_exclusive_min_available";
                } else if (out.ceiling >= *out.good_exclusive_min_ceiling) {
                    out.admitted_if_good_rejected = 1;
                    out.admission_reason_good_rejected = "failed_move_clears_good_exclusive_min";
                } else {
                    out.admitted_if_good_rejected = 0;
                    out.admission_reason_good_rejected = "failed_move_below_good_exclusive_min";
                }
            }

            auto& root = root_acc[out.position_key];
            root.position_key = out.position_key;
            root.good_inclusive_min_move = out.good_inclusive_min_move;
            root.good_inclusive_min_ceiling = out.good_inclusive_min_ceiling;
            root.good_exclusive_min_move = out.good_exclusive_min_move;
            root.good_exclusive_min_ceiling = out.good_exclusive_min_ceiling;
            root.total_move_count += 1;
            if (out.is_engine_accepted == 1) root.accepted_move_count += 1;
            if (out.is_engine_fail == 1) root.failed_move_count += 1;
            if (out.admitted_if_good_accepted == 1) root.admitted_move_count_good_accepted += 1;
            if (out.admitted_if_good_rejected == 1) root.admitted_move_count_good_rejected += 1;
            if (class_is_fail(out.engine_quality_class) && out.admitted_if_good_accepted == 1) root.admitted_failed_move_count_good_accepted += 1;
            if (class_is_fail(out.engine_quality_class) && out.admitted_if_good_rejected == 1) root.admitted_failed_move_count_good_rejected += 1;

            admitted_move_count_good_accepted += out.admitted_if_good_accepted;
            admitted_move_count_good_rejected += out.admitted_if_good_rejected;
            if (class_is_fail(out.engine_quality_class)) {
                admitted_failed_move_count_good_accepted += out.admitted_if_good_accepted;
                admitted_failed_move_count_good_rejected += out.admitted_if_good_rejected;
            }
            final_rows.push_back(std::move(out));
        }

        root_rows.reserve(root_acc.size());
        for (const auto& [_, row] : root_acc) {
            if (row.good_inclusive_min_ceiling.has_value()) ++roots_with_good_inclusive_min;
            if (row.good_exclusive_min_ceiling.has_value()) ++roots_with_good_exclusive_min;
            root_rows.push_back(row);
        }

        std::sort(final_rows.begin(), final_rows.end(), [](const FinalMoveAdmissionRow& a, const FinalMoveAdmissionRow& b) {
            if (a.position_key != b.position_key) return a.position_key < b.position_key;
            return a.popularity_rank < b.popularity_rank;
        });
        std::sort(root_rows.begin(), root_rows.end(), [](const RootFinalThresholdRow& a, const RootFinalThresholdRow& b) {
            return a.position_key < b.position_key;
        });

        progress.update([&](ProgressSnapshot& s) {
            s.risky_admitted_rows = admitted_move_count_good_accepted;
            s.risky_rejected_rows = static_cast<int>(final_rows.size()) - admitted_move_count_good_accepted;
        });
        progress.stage_completed("compute-final-admissions complete");

        progress.stage_started(ProgressStage::WriteArtifacts, "write-artifacts");
        sqlite3* out_db = nullptr;
        const auto out_db_path = bundle_dir / "practical_risk_final.sqlite";
        if (sqlite3_open(out_db_path.string().c_str(), &out_db) != SQLITE_OK) {
            throw std::runtime_error("unable to open output sqlite");
        }
        std::unique_ptr<sqlite3, decltype(&sqlite3_close)> out_guard(out_db, sqlite3_close);

        sqlite_exec(out_db,
            "BEGIN IMMEDIATE;"
            "CREATE TABLE artifact_metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);"
            "CREATE TABLE final_move_admissions(position_key TEXT NOT NULL, move_uci TEXT NOT NULL, move_support INTEGER NOT NULL, popularity_rank INTEGER NOT NULL, ceiling REAL NOT NULL, engine_quality_class TEXT NOT NULL, is_engine_accepted INTEGER NOT NULL, is_engine_fail INTEGER NOT NULL, raw_loss_cp REAL NOT NULL, loss_cp REAL NOT NULL, good_inclusive_min_move TEXT NULL, good_inclusive_min_ceiling REAL NULL, good_exclusive_min_move TEXT NULL, good_exclusive_min_ceiling REAL NULL, admitted_if_good_accepted INTEGER NOT NULL, admitted_if_good_rejected INTEGER NOT NULL, admission_reason_good_accepted TEXT NOT NULL, admission_reason_good_rejected TEXT NOT NULL, PRIMARY KEY(position_key, move_uci));"
            "CREATE TABLE root_final_thresholds(position_key TEXT PRIMARY KEY, good_inclusive_min_move TEXT NULL, good_inclusive_min_ceiling REAL NULL, good_exclusive_min_move TEXT NULL, good_exclusive_min_ceiling REAL NULL, total_move_count INTEGER NOT NULL, accepted_move_count INTEGER NOT NULL, failed_move_count INTEGER NOT NULL, admitted_move_count_good_accepted INTEGER NOT NULL, admitted_move_count_good_rejected INTEGER NOT NULL, admitted_failed_move_count_good_accepted INTEGER NOT NULL, admitted_failed_move_count_good_rejected INTEGER NOT NULL);"
        );

        sqlite3_stmt* meta_stmt = nullptr;
        sqlite3_prepare_v2(out_db, "INSERT INTO artifact_metadata(key, value) VALUES(?1, ?2)", -1, &meta_stmt, nullptr);
        auto insert_kv = [&](const std::string& k, const std::string& v) {
            sqlite3_bind_text(meta_stmt, 1, k.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(meta_stmt, 2, v.c_str(), -1, SQLITE_TRANSIENT);
            if (sqlite3_step(meta_stmt) != SQLITE_DONE) throw std::runtime_error("artifact_metadata insert failed");
            sqlite3_reset(meta_stmt);
            sqlite3_clear_bindings(meta_stmt);
        };
        insert_kv("artifact_id", options.artifact_id);
        insert_kv("artifact_role", "practical_risk_final");
        insert_kv("source_stage_a_bundle", options.prestockfish_bundle.string());
        insert_kv("source_stage_b_bundle", options.stockfish_overlay_bundle.string());
        insert_kv("good_inclusive_policy", "true");
        insert_kv("good_exclusive_policy", "true");
        insert_kv("stockfish_used", "false");
        insert_kv("depends_on_stage_b_stockfish", "true");
        sqlite3_finalize(meta_stmt);

        sqlite3_stmt* fm_stmt = nullptr;
        sqlite3_prepare_v2(out_db,
            "INSERT INTO final_move_admissions(position_key, move_uci, move_support, popularity_rank, ceiling, engine_quality_class, is_engine_accepted, is_engine_fail, raw_loss_cp, loss_cp, good_inclusive_min_move, good_inclusive_min_ceiling, good_exclusive_min_move, good_exclusive_min_ceiling, admitted_if_good_accepted, admitted_if_good_rejected, admission_reason_good_accepted, admission_reason_good_rejected) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)",
            -1,
            &fm_stmt,
            nullptr);
        for (const auto& row : final_rows) {
            sqlite3_bind_text(fm_stmt, 1, row.position_key.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(fm_stmt, 2, row.move_uci.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(fm_stmt, 3, row.move_support);
            sqlite3_bind_int(fm_stmt, 4, row.popularity_rank);
            sqlite3_bind_double(fm_stmt, 5, row.ceiling);
            sqlite3_bind_text(fm_stmt, 6, row.engine_quality_class.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(fm_stmt, 7, row.is_engine_accepted);
            sqlite3_bind_int(fm_stmt, 8, row.is_engine_fail);
            sqlite3_bind_double(fm_stmt, 9, row.raw_loss_cp);
            sqlite3_bind_double(fm_stmt, 10, row.loss_cp);
            if (row.good_inclusive_min_move.has_value()) sqlite3_bind_text(fm_stmt, 11, row.good_inclusive_min_move->c_str(), -1, SQLITE_TRANSIENT);
            else sqlite3_bind_null(fm_stmt, 11);
            if (row.good_inclusive_min_ceiling.has_value()) sqlite3_bind_double(fm_stmt, 12, *row.good_inclusive_min_ceiling);
            else sqlite3_bind_null(fm_stmt, 12);
            if (row.good_exclusive_min_move.has_value()) sqlite3_bind_text(fm_stmt, 13, row.good_exclusive_min_move->c_str(), -1, SQLITE_TRANSIENT);
            else sqlite3_bind_null(fm_stmt, 13);
            if (row.good_exclusive_min_ceiling.has_value()) sqlite3_bind_double(fm_stmt, 14, *row.good_exclusive_min_ceiling);
            else sqlite3_bind_null(fm_stmt, 14);
            sqlite3_bind_int(fm_stmt, 15, row.admitted_if_good_accepted);
            sqlite3_bind_int(fm_stmt, 16, row.admitted_if_good_rejected);
            sqlite3_bind_text(fm_stmt, 17, row.admission_reason_good_accepted.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(fm_stmt, 18, row.admission_reason_good_rejected.c_str(), -1, SQLITE_TRANSIENT);
            if (sqlite3_step(fm_stmt) != SQLITE_DONE) throw std::runtime_error("final_move_admissions insert failed");
            sqlite3_reset(fm_stmt);
            sqlite3_clear_bindings(fm_stmt);
        }
        sqlite3_finalize(fm_stmt);

        sqlite3_stmt* rt_stmt = nullptr;
        sqlite3_prepare_v2(out_db,
            "INSERT INTO root_final_thresholds(position_key, good_inclusive_min_move, good_inclusive_min_ceiling, good_exclusive_min_move, good_exclusive_min_ceiling, total_move_count, accepted_move_count, failed_move_count, admitted_move_count_good_accepted, admitted_move_count_good_rejected, admitted_failed_move_count_good_accepted, admitted_failed_move_count_good_rejected) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            -1,
            &rt_stmt,
            nullptr);
        for (const auto& row : root_rows) {
            sqlite3_bind_text(rt_stmt, 1, row.position_key.c_str(), -1, SQLITE_TRANSIENT);
            if (row.good_inclusive_min_move.has_value()) sqlite3_bind_text(rt_stmt, 2, row.good_inclusive_min_move->c_str(), -1, SQLITE_TRANSIENT);
            else sqlite3_bind_null(rt_stmt, 2);
            if (row.good_inclusive_min_ceiling.has_value()) sqlite3_bind_double(rt_stmt, 3, *row.good_inclusive_min_ceiling);
            else sqlite3_bind_null(rt_stmt, 3);
            if (row.good_exclusive_min_move.has_value()) sqlite3_bind_text(rt_stmt, 4, row.good_exclusive_min_move->c_str(), -1, SQLITE_TRANSIENT);
            else sqlite3_bind_null(rt_stmt, 4);
            if (row.good_exclusive_min_ceiling.has_value()) sqlite3_bind_double(rt_stmt, 5, *row.good_exclusive_min_ceiling);
            else sqlite3_bind_null(rt_stmt, 5);
            sqlite3_bind_int(rt_stmt, 6, row.total_move_count);
            sqlite3_bind_int(rt_stmt, 7, row.accepted_move_count);
            sqlite3_bind_int(rt_stmt, 8, row.failed_move_count);
            sqlite3_bind_int(rt_stmt, 9, row.admitted_move_count_good_accepted);
            sqlite3_bind_int(rt_stmt, 10, row.admitted_move_count_good_rejected);
            sqlite3_bind_int(rt_stmt, 11, row.admitted_failed_move_count_good_accepted);
            sqlite3_bind_int(rt_stmt, 12, row.admitted_failed_move_count_good_rejected);
            if (sqlite3_step(rt_stmt) != SQLITE_DONE) throw std::runtime_error("root_final_thresholds insert failed");
            sqlite3_reset(rt_stmt);
            sqlite3_clear_bindings(rt_stmt);
        }
        sqlite3_finalize(rt_stmt);

        sqlite_exec(out_db, "COMMIT;");

        {
            std::ofstream manifest(bundle_dir / "manifest.json");
            manifest << "{\n";
            manifest << "  \"artifact_id\": \"" << json_escape(options.artifact_id) << "\",\n";
            manifest << "  \"artifact_role\": \"practical_risk_final\",\n";
            manifest << "  \"source_stage_a_bundle\": \"" << json_escape(options.prestockfish_bundle.string()) << "\",\n";
            manifest << "  \"source_stage_b_bundle\": \"" << json_escape(options.stockfish_overlay_bundle.string()) << "\",\n";
            manifest << "  \"good_inclusive_policy\": true,\n";
            manifest << "  \"good_exclusive_policy\": true,\n";
            manifest << "  \"stockfish_used\": false,\n";
            manifest << "  \"depends_on_stage_b_stockfish\": true\n";
            manifest << "}\n";
        }

        {
            std::ofstream summary(bundle_dir / "summary.json");
            summary << "{\n";
            summary << "  \"roots\": " << root_rows.size() << ",\n";
            summary << "  \"move_rows\": " << final_rows.size() << ",\n";
            summary << "  \"admitted_move_count_good_accepted\": " << admitted_move_count_good_accepted << ",\n";
            summary << "  \"admitted_move_count_good_rejected\": " << admitted_move_count_good_rejected << ",\n";
            summary << "  \"admitted_failed_move_count_good_accepted\": " << admitted_failed_move_count_good_accepted << ",\n";
            summary << "  \"admitted_failed_move_count_good_rejected\": " << admitted_failed_move_count_good_rejected << ",\n";
            summary << "  \"roots_with_good_inclusive_min\": " << roots_with_good_inclusive_min << ",\n";
            summary << "  \"roots_with_good_exclusive_min\": " << roots_with_good_exclusive_min << "\n";
            summary << "}\n";
        }

        {
            std::ofstream build_summary(bundle_dir / "build_summary.txt");
            build_summary << "practical-risk final build complete\n";
            build_summary << "roots=" << root_rows.size() << "\n";
            build_summary << "move_rows=" << final_rows.size() << "\n";
            build_summary << "admitted_move_count_good_accepted=" << admitted_move_count_good_accepted << "\n";
            build_summary << "admitted_move_count_good_rejected=" << admitted_move_count_good_rejected << "\n";
            build_summary << "admitted_failed_move_count_good_accepted=" << admitted_failed_move_count_good_accepted << "\n";
            build_summary << "admitted_failed_move_count_good_rejected=" << admitted_failed_move_count_good_rejected << "\n";
            build_summary << "roots_with_good_inclusive_min=" << roots_with_good_inclusive_min << "\n";
            build_summary << "roots_with_good_exclusive_min=" << roots_with_good_exclusive_min << "\n";
        }

        progress.stage_completed("write-artifacts complete");

        progress.stage_started(ProgressStage::Finalize, "finalize");
        progress.update([&](ProgressSnapshot& s) { s.risky_estimated_remaining_work = 0; });
        progress.stage_completed("finalize complete");
        progress.finish();
        return 0;
    } catch (...) {
        progress.stage_failed("practical-risk-final failed");
        progress.finish();
        throw;
    }
}

}  // namespace otcb
