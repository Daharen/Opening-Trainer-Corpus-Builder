#include "otcb/practical_risk/reconciled.hpp"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <sqlite3.h>

#include "otcb/progress.hpp"

namespace otcb {
namespace {

struct StageCRow {
    int admitted_if_good_accepted = 0;
    int admitted_if_good_rejected = 0;
    std::string admission_reason_good_accepted;
    std::string admission_reason_good_rejected;
    std::string engine_quality_class;
    double ceiling = 0.0;
    std::optional<double> good_inclusive_min_ceiling;
    std::optional<double> good_exclusive_min_ceiling;
};

struct ModeState {
    int local_admitted = 0;
    std::string local_reason;
    int reconciled_admitted = 0;
    std::string origin;
    std::optional<std::string> highest_locally_admitted_band;
    std::optional<std::string> first_failing_higher_band;
    std::optional<std::string> first_failure_reason;
};

struct ReconciledMoveRow {
    std::string band_id;
    std::string position_key;
    std::string move_uci;
    ModeState inclusive;
    ModeState exclusive;
    std::string engine_quality_class;
    double ceiling = 0.0;
    std::optional<double> good_inclusive_min_ceiling;
    std::optional<double> good_exclusive_min_ceiling;
};

struct CompatibilityState {
    std::optional<std::string> artifact_role;
    std::optional<std::string> time_control_id;
    std::optional<std::string> time_control_family_id;
    std::optional<std::string> policy_version_identity;
    std::optional<std::string> explanation_family_assumptions;
    std::optional<std::string> dual_policy_model;
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

std::vector<std::string> split_csv(const std::string& csv) {
    std::vector<std::string> out;
    std::string current;
    for (char c : csv) {
        if (c == ',') {
            if (!current.empty()) out.push_back(current);
            current.clear();
        } else {
            current.push_back(c);
        }
    }
    if (!current.empty()) out.push_back(current);
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

std::string sqlite_quote(const std::string& value) {
    std::string out;
    out.reserve(value.size() + 2);
    out.push_back('\'');
    for (const char ch : value) {
        if (ch == '\'') out += "''";
        else out.push_back(ch);
    }
    out.push_back('\'');
    return out;
}

class SqliteStatement {
public:
    SqliteStatement(sqlite3* db, const std::string& sql) {
        if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt_, nullptr) != SQLITE_OK) {
            throw std::runtime_error("sqlite prepare failed: " + std::string(sqlite3_errmsg(db)));
        }
    }

    ~SqliteStatement() {
        if (stmt_) {
            sqlite3_finalize(stmt_);
        }
    }

    sqlite3_stmt* get() const { return stmt_; }

private:
    sqlite3_stmt* stmt_ = nullptr;
};

void bind_text(sqlite3_stmt* stmt, int index, const std::string& value) {
    sqlite3_bind_text(stmt, index, value.c_str(), -1, SQLITE_TRANSIENT);
}

void bind_optional_text(sqlite3_stmt* stmt, int index, const std::optional<std::string>& value) {
    if (value.has_value()) {
        sqlite3_bind_text(stmt, index, value->c_str(), -1, SQLITE_TRANSIENT);
    } else {
        sqlite3_bind_null(stmt, index);
    }
}

void bind_optional_double(sqlite3_stmt* stmt, int index, const std::optional<double>& value) {
    if (value.has_value()) sqlite3_bind_double(stmt, index, *value);
    else sqlite3_bind_null(stmt, index);
}

void step_done(sqlite3_stmt* stmt, sqlite3* db, const std::string& context) {
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        throw std::runtime_error(context + ": " + sqlite3_errmsg(db));
    }
    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);
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
    return exists != 0;
}

void validate_key_match(const std::optional<std::string>& expected, const std::optional<std::string>& value, const std::string& key, const std::string& band_id) {
    if (!expected.has_value() || !value.has_value()) return;
    if (*expected != *value) {
        throw std::runtime_error("Stage C compatibility mismatch for key '" + key + "' at band " + band_id + ": expected=" + *expected + " got=" + *value);
    }
}

std::optional<std::string> get_meta(sqlite3* db, const std::string& key) {
    std::optional<std::string> value;
    const std::string sql = "SELECT value FROM artifact_metadata WHERE key='" + key + "' LIMIT 1";
    char* err = nullptr;
    const auto cb = [](void* data, int argc, char** argv, char**) -> int {
        if (argc > 0 && argv[0]) {
            *static_cast<std::optional<std::string>*>(data) = std::string(argv[0]);
        }
        return 0;
    };
    if (sqlite3_exec(db, sql.c_str(), cb, &value, &err) != SQLITE_OK) {
        const std::string msg = err ? err : "sqlite error";
        sqlite3_free(err);
        throw std::runtime_error("failed reading artifact_metadata: " + msg);
    }
    return value;
}

std::filesystem::path resolve_band_bundle_path(const PracticalRiskReconciledOptions& options, const std::string& band_id) {
    if (!options.final_bundle_root.empty()) {
        const auto candidate = options.final_bundle_root / band_id;
        if (std::filesystem::exists(candidate / "practical_risk_final.sqlite")) return candidate;
        throw std::runtime_error("unable to resolve Stage C bundle for band '" + band_id + "' under --final-bundle-root");
    }
    for (const auto& b : options.final_bundles) {
        if (b.filename().string() == band_id && std::filesystem::exists(b / "practical_risk_final.sqlite")) {
            return b;
        }
    }
    throw std::runtime_error("unable to resolve Stage C bundle for band '" + band_id + "' from --final-bundle list");
}

void fill_mode_boundary(
    std::vector<ReconciledMoveRow>& ladder,
    const std::vector<std::string>& band_order,
    const std::function<ModeState&(ReconciledMoveRow&)>& selector) {

    std::optional<std::string> highest_local;
    for (std::size_t i = 0; i < ladder.size(); ++i) {
        if (selector(ladder[i]).local_admitted != 0) {
            highest_local = band_order[i];
            break;
        }
    }

    for (std::size_t i = 0; i < ladder.size(); ++i) {
        auto& state = selector(ladder[i]);
        state.highest_locally_admitted_band = highest_local;

        bool has_lower_or_equal_local = false;
        for (std::size_t j = i; j < ladder.size(); ++j) {
            if (selector(ladder[j]).local_admitted != 0) {
                has_lower_or_equal_local = true;
                break;
            }
        }

        state.first_failing_higher_band = std::nullopt;
        state.first_failure_reason = std::nullopt;
        if (!has_lower_or_equal_local) continue;

        for (std::size_t j = i; j > 0; --j) {
            auto& higher = selector(ladder[j - 1]);
            if (higher.local_admitted == 0) {
                state.first_failing_higher_band = band_order[j - 1];
                state.first_failure_reason = higher.local_reason;
                break;
            }
        }
    }
}

std::pair<std::string, std::string> classify_failure_reason(
    const ModeState& mode,
    const std::string& mode_id,
    bool inclusive_reconciled,
    bool exclusive_reconciled) {

    if (mode_id == "good_exclusive" && !exclusive_reconciled && inclusive_reconciled) {
        if (mode.local_reason == "good_rejected_in_strict_mode") {
            return {"strict_mode_rejects_good", "FAIL_STRICT_MODE_REJECTS_GOOD"};
        }
        return {"would_pass_if_sharp_toggle_enabled", "FAIL_WOULD_PASS_IF_SHARP_TOGGLE_ENABLED"};
    }

    if (mode.local_reason == "failed_move_below_good_inclusive_min" || mode.local_reason == "failed_move_below_good_exclusive_min") {
        return {"failed_below_threshold", "FAIL_BELOW_THRESHOLD"};
    }

    if (mode.local_reason == "no_good_inclusive_min_available" || mode.local_reason == "no_good_exclusive_min_available") {
        return {"no_threshold_available", "FAIL_NO_THRESHOLD"};
    }

    if (mode.first_failing_higher_band.has_value() && mode.highest_locally_admitted_band.has_value()) {
        return {"outgrown_above_band", "FAIL_OUTGROWN_AFTER_BAND"};
    }

    return {"failed_below_threshold", "FAIL_BELOW_THRESHOLD"};
}

void upsert_root_summary(
    sqlite3* out_db,
    sqlite3_stmt* stmt,
    const std::string& band_id,
    const std::string& position_key,
    int local_inclusive,
    int local_exclusive,
    int reconciled_inclusive,
    int reconciled_exclusive,
    int inherited_inclusive,
    int inherited_exclusive,
    int failure_inclusive,
    int failure_exclusive) {

    bind_text(stmt, 1, band_id);
    bind_text(stmt, 2, position_key);
    sqlite3_bind_int(stmt, 3, local_inclusive);
    sqlite3_bind_int(stmt, 4, local_exclusive);
    sqlite3_bind_int(stmt, 5, reconciled_inclusive);
    sqlite3_bind_int(stmt, 6, reconciled_exclusive);
    sqlite3_bind_int(stmt, 7, inherited_inclusive);
    sqlite3_bind_int(stmt, 8, inherited_exclusive);
    sqlite3_bind_int(stmt, 9, failure_inclusive);
    sqlite3_bind_int(stmt, 10, failure_exclusive);
    step_done(stmt, out_db, "reconciled_root_summaries upsert failed");
}

}  // namespace

void print_practical_risk_reconciled_usage(const std::string& program_name) {
    std::cout
        << "Usage: " << program_name << " --final-bundle-root <dir> --output-dir <dir> --artifact-family-id <id> --time-control-id <id> --band-order <high-to-low-csv>\n"
        << "       [--final-bundle <dir>] [--sharp-family-label sharp/gambit] [--emit-progress-log] [--emit-status-json] [--heartbeat-seconds 30] [--quiet-progress]\n";
}

PracticalRiskReconciledOptions parse_practical_risk_reconciled_cli(int argc, char** argv) {
    PracticalRiskReconciledOptions out;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto require_value = [&](const char* flag) -> std::string {
            if (i + 1 >= argc) throw std::runtime_error(std::string("missing value for ") + flag);
            return argv[++i];
        };

        if (arg == "--final-bundle-root") out.final_bundle_root = require_value("--final-bundle-root");
        else if (arg == "--final-bundle") out.final_bundles.emplace_back(require_value("--final-bundle"));
        else if (arg == "--output-dir") out.output_dir = require_value("--output-dir");
        else if (arg == "--artifact-family-id") out.artifact_family_id = require_value("--artifact-family-id");
        else if (arg == "--time-control-id") out.time_control_id = require_value("--time-control-id");
        else if (arg == "--band-order") out.band_order = split_csv(require_value("--band-order"));
        else if (arg == "--sharp-family-label") out.sharp_family_label = require_value("--sharp-family-label");
        else if (arg == "--emit-progress-log") out.emit_progress_log = true;
        else if (arg == "--emit-status-json") out.emit_status_json = true;
        else if (arg == "--heartbeat-seconds") out.heartbeat_seconds = std::stoi(require_value("--heartbeat-seconds"));
        else if (arg == "--quiet-progress") out.quiet_progress = true;
        else if (arg == "--help" || arg == "-h") {
            print_practical_risk_reconciled_usage(argv[0]);
            std::exit(0);
        }
    }

    if (out.output_dir.empty() || out.artifact_family_id.empty() || out.time_control_id.empty() || out.band_order.empty()) {
        throw std::runtime_error("missing required arguments --output-dir --artifact-family-id --time-control-id --band-order");
    }
    if (out.final_bundle_root.empty() && out.final_bundles.empty()) {
        throw std::runtime_error("either --final-bundle-root or repeated --final-bundle must be provided");
    }
    if (out.heartbeat_seconds <= 0) throw std::runtime_error("--heartbeat-seconds must be positive");
    return out;
}

int run_practical_risk_reconciled(const PracticalRiskReconciledOptions& options) {
    const auto bundle_dir = options.output_dir / options.artifact_family_id;
    std::filesystem::create_directories(bundle_dir / "progress");

    ProgressReporter progress(ProgressReporterOptions{
        .quiet = options.quiet_progress,
        .emit_progress_log = options.emit_progress_log,
        .emit_status_json = options.emit_status_json,
        .heartbeat_seconds = options.heartbeat_seconds,
        .artifact_bundle_root = bundle_dir,
    });
    progress.start();

    CompatibilityState compatibility;
    int inherited_inclusive = 0;
    int inherited_exclusive = 0;
    int failure_count_inclusive = 0;
    int failure_count_exclusive = 0;
    int with_boundary_inclusive = 0;
    int with_boundary_exclusive = 0;
    std::uint64_t groups_processed = 0;
    std::uint64_t reconciled_rows_written = 0;
    std::uint64_t failure_rows_written = 0;
    std::uint64_t root_summaries_finalized = 0;

    sqlite3* work_db = nullptr;
    sqlite3* out_db = nullptr;

    try {
        progress.stage_started(ProgressStage::Preflight, "preflight");
        std::vector<std::pair<std::string, std::filesystem::path>> band_bundles;
        for (const auto& band_id : options.band_order) {
            if (band_id.empty()) throw std::runtime_error("--band-order contains empty band token");
            const auto bundle = resolve_band_bundle_path(options, band_id);
            if (!std::filesystem::exists(bundle / "practical_risk_final.sqlite")) {
                throw std::runtime_error("missing practical_risk_final.sqlite for band " + band_id);
            }
            band_bundles.emplace_back(band_id, bundle);
        }
        progress.stage_completed("preflight complete");

        const auto work_db_path = bundle_dir / "progress" / "practical_risk_reconciled_working.sqlite";
        if (std::filesystem::exists(work_db_path)) {
            std::filesystem::remove(work_db_path);
        }
        if (sqlite3_open(work_db_path.string().c_str(), &work_db) != SQLITE_OK) {
            throw std::runtime_error("unable to create Stage D working sqlite");
        }

        sqlite_exec(work_db,
            "PRAGMA journal_mode=WAL;"
            "PRAGMA synchronous=NORMAL;"
            "BEGIN IMMEDIATE;"
            "CREATE TABLE band_index(band_id TEXT PRIMARY KEY, band_rank INTEGER NOT NULL);"
            "CREATE TABLE stage_c_rows("
            " band_id TEXT NOT NULL,"
            " band_rank INTEGER NOT NULL,"
            " position_key TEXT NOT NULL,"
            " move_uci TEXT NOT NULL,"
            " local_admitted_if_good_accepted INTEGER NOT NULL,"
            " local_admitted_if_good_rejected INTEGER NOT NULL,"
            " local_reason_good_accepted TEXT NOT NULL,"
            " local_reason_good_rejected TEXT NOT NULL,"
            " engine_quality_class TEXT NOT NULL,"
            " ceiling REAL NOT NULL,"
            " good_inclusive_min_ceiling REAL NULL,"
            " good_exclusive_min_ceiling REAL NULL,"
            " PRIMARY KEY(band_id, position_key, move_uci)"
            ");"
            "CREATE INDEX idx_stage_c_rows_pos_move_rank ON stage_c_rows(position_key, move_uci, band_rank);"
            "CREATE INDEX idx_stage_c_rows_position ON stage_c_rows(position_key);"
        );

        SqliteStatement insert_band_stmt(work_db, "INSERT INTO band_index(band_id, band_rank) VALUES(?1, ?2)");
        for (std::size_t i = 0; i < options.band_order.size(); ++i) {
            bind_text(insert_band_stmt.get(), 1, options.band_order[i]);
            sqlite3_bind_int(insert_band_stmt.get(), 2, static_cast<int>(i));
            step_done(insert_band_stmt.get(), work_db, "band_index insert failed");
        }

        progress.stage_started(ProgressStage::LoadPrestockfish, "load-stage-c-family");
        SqliteStatement insert_stage_c_stmt(
            work_db,
            "INSERT INTO stage_c_rows("
            "band_id, band_rank, position_key, move_uci, local_admitted_if_good_accepted, local_admitted_if_good_rejected, local_reason_good_accepted, local_reason_good_rejected, engine_quality_class, ceiling, good_inclusive_min_ceiling, good_exclusive_min_ceiling"
            ") VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)");

        for (std::size_t band_rank = 0; band_rank < band_bundles.size(); ++band_rank) {
            const auto& band_id = band_bundles[band_rank].first;
            const auto& bundle = band_bundles[band_rank].second;
            sqlite3* db = nullptr;
            if (sqlite3_open((bundle / "practical_risk_final.sqlite").string().c_str(), &db) != SQLITE_OK) {
                throw std::runtime_error("unable to open Stage C sqlite for band " + band_id);
            }
            std::unique_ptr<sqlite3, decltype(&sqlite3_close)> db_guard(db, sqlite3_close);

            if (!table_exists(db, "final_move_admissions") || !table_exists(db, "root_final_thresholds") || !table_exists(db, "artifact_metadata")) {
                throw std::runtime_error("Stage C sqlite missing required tables for band " + band_id);
            }

            const auto role = get_meta(db, "artifact_role");
            if (role.has_value() && *role != "practical_risk_final") {
                throw std::runtime_error("Stage C artifact_role mismatch at band " + band_id + ": " + *role);
            }
            if (!compatibility.artifact_role.has_value()) compatibility.artifact_role = role;
            validate_key_match(compatibility.artifact_role, role, "artifact_role", band_id);

            const auto tc = get_meta(db, "time_control_id");
            if (tc.has_value() && *tc != options.time_control_id) {
                throw std::runtime_error("time_control_id mismatch at band " + band_id + ": expected=" + options.time_control_id + " got=" + *tc);
            }
            if (!compatibility.time_control_id.has_value()) compatibility.time_control_id = tc;
            validate_key_match(compatibility.time_control_id, tc, "time_control_id", band_id);

            const auto tcf = get_meta(db, "time_control_family_id");
            if (!compatibility.time_control_family_id.has_value()) compatibility.time_control_family_id = tcf;
            validate_key_match(compatibility.time_control_family_id, tcf, "time_control_family_id", band_id);

            const auto pvi = get_meta(db, "policy_version_identity");
            if (!compatibility.policy_version_identity.has_value()) compatibility.policy_version_identity = pvi;
            validate_key_match(compatibility.policy_version_identity, pvi, "policy_version_identity", band_id);

            const auto efa = get_meta(db, "explanation_family_assumptions");
            if (!compatibility.explanation_family_assumptions.has_value()) compatibility.explanation_family_assumptions = efa;
            validate_key_match(compatibility.explanation_family_assumptions, efa, "explanation_family_assumptions", band_id);

            const auto dpm = get_meta(db, "dual_policy_model");
            if (!compatibility.dual_policy_model.has_value()) compatibility.dual_policy_model = dpm;
            validate_key_match(compatibility.dual_policy_model, dpm, "dual_policy_model", band_id);

            char* err = nullptr;
            auto rows_cb = [](void* data, int argc, char** argv, char**) -> int {
                if (argc < 10 || !argv[0] || !argv[1]) return 1;
                auto* payload = static_cast<std::pair<std::pair<std::string, int>*, SqliteStatement*>*>(data);
                const std::string& band_id_local = payload->first->first;
                const int band_rank_local = payload->first->second;
                sqlite3_stmt* stmt = payload->second->get();
                bind_text(stmt, 1, band_id_local);
                sqlite3_bind_int(stmt, 2, band_rank_local);
                bind_text(stmt, 3, argv[0]);
                bind_text(stmt, 4, argv[1]);
                sqlite3_bind_int(stmt, 5, argv[2] ? std::stoi(argv[2]) : 0);
                sqlite3_bind_int(stmt, 6, argv[3] ? std::stoi(argv[3]) : 0);
                bind_text(stmt, 7, argv[4] ? argv[4] : "");
                bind_text(stmt, 8, argv[5] ? argv[5] : "");
                bind_text(stmt, 9, argv[6] ? argv[6] : "unknown");
                sqlite3_bind_double(stmt, 10, argv[7] ? std::stod(argv[7]) : 0.0);
                if (argv[8]) sqlite3_bind_double(stmt, 11, std::stod(argv[8])); else sqlite3_bind_null(stmt, 11);
                if (argv[9]) sqlite3_bind_double(stmt, 12, std::stod(argv[9])); else sqlite3_bind_null(stmt, 12);
                return sqlite3_step(stmt) == SQLITE_DONE ? (sqlite3_reset(stmt), sqlite3_clear_bindings(stmt), 0) : 1;
            };
            auto payload_info = std::make_pair(band_id, static_cast<int>(band_rank));
            auto payload = std::make_pair(&payload_info, &insert_stage_c_stmt);
            if (sqlite3_exec(
                    db,
                    "SELECT position_key, move_uci, admitted_if_good_accepted, admitted_if_good_rejected, admission_reason_good_accepted, admission_reason_good_rejected, engine_quality_class, ceiling, good_inclusive_min_ceiling, good_exclusive_min_ceiling FROM final_move_admissions",
                    rows_cb,
                    &payload,
                    &err) != SQLITE_OK) {
                const std::string msg = err ? err : "sqlite error";
                sqlite3_free(err);
                throw std::runtime_error("failed reading final_move_admissions for band " + band_id + ": " + msg);
            }
        }
        sqlite_exec(work_db, "COMMIT;");
        progress.stage_completed("load-stage-c-family complete");

        const auto out_db_path = bundle_dir / "practical_risk_reconciled.sqlite";
        if (std::filesystem::exists(out_db_path)) {
            std::filesystem::remove(out_db_path);
        }
        if (sqlite3_open(out_db_path.string().c_str(), &out_db) != SQLITE_OK) {
            throw std::runtime_error("unable to open output sqlite");
        }

        sqlite_exec(out_db,
            "BEGIN IMMEDIATE;"
            "CREATE TABLE artifact_metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);"
            "CREATE TABLE reconciled_move_admissions(band_id TEXT NOT NULL, position_key TEXT NOT NULL, move_uci TEXT NOT NULL, local_admitted_if_good_accepted INTEGER NOT NULL, local_admitted_if_good_rejected INTEGER NOT NULL, local_reason_good_accepted TEXT NOT NULL, local_reason_good_rejected TEXT NOT NULL, reconciled_admitted_if_good_accepted INTEGER NOT NULL, reconciled_admitted_if_good_rejected INTEGER NOT NULL, admission_origin_good_accepted TEXT NOT NULL, admission_origin_good_rejected TEXT NOT NULL, engine_quality_class TEXT NOT NULL, ceiling REAL NOT NULL, good_inclusive_min_ceiling REAL NULL, good_exclusive_min_ceiling REAL NULL, highest_locally_admitted_band_good_accepted TEXT NULL, highest_locally_admitted_band_good_rejected TEXT NULL, first_failing_higher_band_good_accepted TEXT NULL, first_failing_higher_band_good_rejected TEXT NULL, first_failure_reason_good_accepted TEXT NULL, first_failure_reason_good_rejected TEXT NULL, PRIMARY KEY(band_id, position_key, move_uci));"
            "CREATE TABLE failure_explanations(band_id TEXT NOT NULL, position_key TEXT NOT NULL, move_uci TEXT NOT NULL, mode_id TEXT NOT NULL, reason_code TEXT NOT NULL, template_id TEXT NOT NULL, family_label TEXT NOT NULL, current_band_id TEXT NOT NULL, max_practical_band_id TEXT NULL, first_failure_band_id TEXT NULL, toggle_state_required TEXT NULL, origin_flag TEXT NOT NULL, rendered_preview TEXT NULL, PRIMARY KEY(band_id, position_key, move_uci, mode_id));"
            "CREATE TABLE reconciled_root_summaries(band_id TEXT NOT NULL, position_key TEXT NOT NULL, local_admitted_count_good_accepted INTEGER NOT NULL, local_admitted_count_good_rejected INTEGER NOT NULL, reconciled_admitted_count_good_accepted INTEGER NOT NULL, reconciled_admitted_count_good_rejected INTEGER NOT NULL, inherited_count_good_accepted INTEGER NOT NULL, inherited_count_good_rejected INTEGER NOT NULL, failure_explanation_count_good_accepted INTEGER NOT NULL, failure_explanation_count_good_rejected INTEGER NOT NULL, PRIMARY KEY(band_id, position_key));"
        );

        {
            SqliteStatement meta_stmt(out_db, "INSERT INTO artifact_metadata(key, value) VALUES(?1, ?2)");
            auto insert_kv = [&](const std::string& k, const std::string& v) {
                bind_text(meta_stmt.get(), 1, k);
                bind_text(meta_stmt.get(), 2, v);
                step_done(meta_stmt.get(), out_db, "artifact_metadata insert failed");
            };

            std::ostringstream bands;
            for (std::size_t i = 0; i < options.band_order.size(); ++i) {
                if (i > 0) bands << ',';
                bands << options.band_order[i];
            }

            insert_kv("artifact_family_id", options.artifact_family_id);
            insert_kv("artifact_role", "practical_risk_reconciled");
            insert_kv("time_control_id", options.time_control_id);
            insert_kv("band_order", bands.str());
            insert_kv("source_stage_c_root", options.final_bundle_root.empty() ? "from_final_bundle_list" : options.final_bundle_root.string());
            insert_kv("downward_propagation_enabled", "true");
            insert_kv("upward_description_enabled", "true");
            insert_kv("success_threads_emitted", "false");
            insert_kv("failure_threads_emitted", "true");
            insert_kv("family_label", options.sharp_family_label);
        }

        SqliteStatement insert_reconciled_stmt(
            out_db,
            "INSERT INTO reconciled_move_admissions(band_id, position_key, move_uci, local_admitted_if_good_accepted, local_admitted_if_good_rejected, local_reason_good_accepted, local_reason_good_rejected, reconciled_admitted_if_good_accepted, reconciled_admitted_if_good_rejected, admission_origin_good_accepted, admission_origin_good_rejected, engine_quality_class, ceiling, good_inclusive_min_ceiling, good_exclusive_min_ceiling, highest_locally_admitted_band_good_accepted, highest_locally_admitted_band_good_rejected, first_failing_higher_band_good_accepted, first_failing_higher_band_good_rejected, first_failure_reason_good_accepted, first_failure_reason_good_rejected) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21)");

        SqliteStatement upsert_summary_stmt(
            out_db,
            "INSERT INTO reconciled_root_summaries(band_id, position_key, local_admitted_count_good_accepted, local_admitted_count_good_rejected, reconciled_admitted_count_good_accepted, reconciled_admitted_count_good_rejected, inherited_count_good_accepted, inherited_count_good_rejected, failure_explanation_count_good_accepted, failure_explanation_count_good_rejected) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) "
            "ON CONFLICT(band_id, position_key) DO UPDATE SET "
            "local_admitted_count_good_accepted = local_admitted_count_good_accepted + excluded.local_admitted_count_good_accepted, "
            "local_admitted_count_good_rejected = local_admitted_count_good_rejected + excluded.local_admitted_count_good_rejected, "
            "reconciled_admitted_count_good_accepted = reconciled_admitted_count_good_accepted + excluded.reconciled_admitted_count_good_accepted, "
            "reconciled_admitted_count_good_rejected = reconciled_admitted_count_good_rejected + excluded.reconciled_admitted_count_good_rejected, "
            "inherited_count_good_accepted = inherited_count_good_accepted + excluded.inherited_count_good_accepted, "
            "inherited_count_good_rejected = inherited_count_good_rejected + excluded.inherited_count_good_rejected, "
            "failure_explanation_count_good_accepted = failure_explanation_count_good_accepted + excluded.failure_explanation_count_good_accepted, "
            "failure_explanation_count_good_rejected = failure_explanation_count_good_rejected + excluded.failure_explanation_count_good_rejected");

        progress.stage_started(ProgressStage::ComputeRiskyOverlay, "compute-downward-reconciliation");
        std::vector<std::string> position_keys;
        {
            char* err = nullptr;
            const auto cb = [](void* data, int argc, char** argv, char**) -> int {
                if (argc > 0 && argv[0]) static_cast<std::vector<std::string>*>(data)->emplace_back(argv[0]);
                return 0;
            };
            if (sqlite3_exec(work_db, "SELECT DISTINCT position_key FROM stage_c_rows ORDER BY position_key", cb, &position_keys, &err) != SQLITE_OK) {
                const std::string msg = err ? err : "sqlite error";
                sqlite3_free(err);
                throw std::runtime_error("failed reading position keys: " + msg);
            }
        }

        for (const auto& position_key : position_keys) {
            std::vector<std::string> move_ucis;
            {
                char* err = nullptr;
                const std::string sql = "SELECT DISTINCT move_uci FROM stage_c_rows WHERE position_key=" + sqlite_quote(position_key) + " ORDER BY move_uci";
                const auto cb = [](void* data, int argc, char** argv, char**) -> int {
                    if (argc > 0 && argv[0]) static_cast<std::vector<std::string>*>(data)->emplace_back(argv[0]);
                    return 0;
                };
                if (sqlite3_exec(work_db, sql.c_str(), cb, &move_ucis, &err) != SQLITE_OK) {
                    const std::string msg = err ? err : "sqlite error";
                    sqlite3_free(err);
                    throw std::runtime_error("failed reading move keys: " + msg);
                }
            }

            for (const auto& move_uci : move_ucis) {
                std::unordered_map<std::string, StageCRow> by_band;
                {
                    char* err = nullptr;
                    const std::string sql =
                        "SELECT band_id, local_admitted_if_good_accepted, local_admitted_if_good_rejected, local_reason_good_accepted, local_reason_good_rejected, engine_quality_class, ceiling, good_inclusive_min_ceiling, good_exclusive_min_ceiling "
                        "FROM stage_c_rows WHERE position_key=" + sqlite_quote(position_key) + " AND move_uci=" + sqlite_quote(move_uci) + " ORDER BY band_rank";
                    const auto cb = [](void* data, int argc, char** argv, char**) -> int {
                        if (argc < 9 || !argv[0]) return 1;
                        auto* rows = static_cast<std::unordered_map<std::string, StageCRow>*>(data);
                        StageCRow row;
                        row.admitted_if_good_accepted = argv[1] ? std::stoi(argv[1]) : 0;
                        row.admitted_if_good_rejected = argv[2] ? std::stoi(argv[2]) : 0;
                        row.admission_reason_good_accepted = argv[3] ? argv[3] : "";
                        row.admission_reason_good_rejected = argv[4] ? argv[4] : "";
                        row.engine_quality_class = argv[5] ? argv[5] : "unknown";
                        row.ceiling = argv[6] ? std::stod(argv[6]) : 0.0;
                        if (argv[7]) row.good_inclusive_min_ceiling = std::stod(argv[7]);
                        if (argv[8]) row.good_exclusive_min_ceiling = std::stod(argv[8]);
                        (*rows)[argv[0]] = std::move(row);
                        return 0;
                    };
                    if (sqlite3_exec(work_db, sql.c_str(), cb, &by_band, &err) != SQLITE_OK) {
                        const std::string msg = err ? err : "sqlite error";
                        sqlite3_free(err);
                        throw std::runtime_error("failed reading reconciliation group rows: " + msg);
                    }
                }

                std::vector<ReconciledMoveRow> ladder;
                ladder.reserve(options.band_order.size());
                bool has_higher_inclusive = false;
                bool has_higher_exclusive = false;

                for (const auto& band_id : options.band_order) {
                    StageCRow row;
                    const auto it = by_band.find(band_id);
                    if (it != by_band.end()) {
                        row = it->second;
                    } else {
                        row.admission_reason_good_accepted = "move_not_present_in_band";
                        row.admission_reason_good_rejected = "move_not_present_in_band";
                        row.engine_quality_class = "unknown";
                    }

                    ReconciledMoveRow out;
                    out.band_id = band_id;
                    out.position_key = position_key;
                    out.move_uci = move_uci;
                    out.engine_quality_class = row.engine_quality_class;
                    out.ceiling = row.ceiling;
                    out.good_inclusive_min_ceiling = row.good_inclusive_min_ceiling;
                    out.good_exclusive_min_ceiling = row.good_exclusive_min_ceiling;

                    out.inclusive.local_admitted = row.admitted_if_good_accepted;
                    out.inclusive.local_reason = row.admission_reason_good_accepted;
                    out.inclusive.reconciled_admitted = (row.admitted_if_good_accepted != 0 || has_higher_inclusive) ? 1 : 0;
                    out.inclusive.origin = row.admitted_if_good_accepted != 0 ? "local" : (has_higher_inclusive ? "inherited_from_higher_band" : "not_admitted");

                    out.exclusive.local_admitted = row.admitted_if_good_rejected;
                    out.exclusive.local_reason = row.admission_reason_good_rejected;
                    out.exclusive.reconciled_admitted = (row.admitted_if_good_rejected != 0 || has_higher_exclusive) ? 1 : 0;
                    out.exclusive.origin = row.admitted_if_good_rejected != 0 ? "local" : (has_higher_exclusive ? "inherited_from_higher_band" : "not_admitted");

                    has_higher_inclusive = has_higher_inclusive || row.admitted_if_good_accepted != 0;
                    has_higher_exclusive = has_higher_exclusive || row.admitted_if_good_rejected != 0;
                    ladder.push_back(std::move(out));
                }

                fill_mode_boundary(ladder, options.band_order, [](ReconciledMoveRow& row) -> ModeState& { return row.inclusive; });
                fill_mode_boundary(ladder, options.band_order, [](ReconciledMoveRow& row) -> ModeState& { return row.exclusive; });

                for (const auto& row : ladder) {
                    bind_text(insert_reconciled_stmt.get(), 1, row.band_id);
                    bind_text(insert_reconciled_stmt.get(), 2, row.position_key);
                    bind_text(insert_reconciled_stmt.get(), 3, row.move_uci);
                    sqlite3_bind_int(insert_reconciled_stmt.get(), 4, row.inclusive.local_admitted);
                    sqlite3_bind_int(insert_reconciled_stmt.get(), 5, row.exclusive.local_admitted);
                    bind_text(insert_reconciled_stmt.get(), 6, row.inclusive.local_reason);
                    bind_text(insert_reconciled_stmt.get(), 7, row.exclusive.local_reason);
                    sqlite3_bind_int(insert_reconciled_stmt.get(), 8, row.inclusive.reconciled_admitted);
                    sqlite3_bind_int(insert_reconciled_stmt.get(), 9, row.exclusive.reconciled_admitted);
                    bind_text(insert_reconciled_stmt.get(), 10, row.inclusive.origin);
                    bind_text(insert_reconciled_stmt.get(), 11, row.exclusive.origin);
                    bind_text(insert_reconciled_stmt.get(), 12, row.engine_quality_class);
                    sqlite3_bind_double(insert_reconciled_stmt.get(), 13, row.ceiling);
                    bind_optional_double(insert_reconciled_stmt.get(), 14, row.good_inclusive_min_ceiling);
                    bind_optional_double(insert_reconciled_stmt.get(), 15, row.good_exclusive_min_ceiling);
                    bind_optional_text(insert_reconciled_stmt.get(), 16, row.inclusive.highest_locally_admitted_band);
                    bind_optional_text(insert_reconciled_stmt.get(), 17, row.exclusive.highest_locally_admitted_band);
                    bind_optional_text(insert_reconciled_stmt.get(), 18, row.inclusive.first_failing_higher_band);
                    bind_optional_text(insert_reconciled_stmt.get(), 19, row.exclusive.first_failing_higher_band);
                    bind_optional_text(insert_reconciled_stmt.get(), 20, row.inclusive.first_failure_reason);
                    bind_optional_text(insert_reconciled_stmt.get(), 21, row.exclusive.first_failure_reason);
                    step_done(insert_reconciled_stmt.get(), out_db, "reconciled_move_admissions insert failed");

                    upsert_root_summary(
                        out_db,
                        upsert_summary_stmt.get(),
                        row.band_id,
                        row.position_key,
                        row.inclusive.local_admitted,
                        row.exclusive.local_admitted,
                        row.inclusive.reconciled_admitted,
                        row.exclusive.reconciled_admitted,
                        row.inclusive.origin == "inherited_from_higher_band" ? 1 : 0,
                        row.exclusive.origin == "inherited_from_higher_band" ? 1 : 0,
                        0,
                        0);

                    if (row.inclusive.origin == "inherited_from_higher_band") ++inherited_inclusive;
                    if (row.exclusive.origin == "inherited_from_higher_band") ++inherited_exclusive;
                    if (row.inclusive.first_failing_higher_band.has_value()) ++with_boundary_inclusive;
                    if (row.exclusive.first_failing_higher_band.has_value()) ++with_boundary_exclusive;
                    ++reconciled_rows_written;
                }

                ++groups_processed;
                if (groups_processed % 1000 == 0) {
                    progress.update([&](ProgressSnapshot& snap) {
                        snap.risky_positions_considered = static_cast<int>(groups_processed);
                        snap.risky_admitted_rows = static_cast<int>(reconciled_rows_written);
                    });
                }
            }
        }
        progress.stage_completed("compute-downward-reconciliation complete");

        progress.stage_started(ProgressStage::ComputeAcceptedPriors, "compute-upward-failure-boundaries");
        progress.note_event("compute-upward-failure-boundaries complete");
        progress.stage_completed("compute-upward-failure-boundaries complete");

        progress.stage_started(ProgressStage::EngineBaselineScreen, "emit-failure-explanations");
        SqliteStatement insert_failure_stmt(
            out_db,
            "INSERT INTO failure_explanations(band_id, position_key, move_uci, mode_id, reason_code, template_id, family_label, current_band_id, max_practical_band_id, first_failure_band_id, toggle_state_required, origin_flag, rendered_preview) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)");
        char* err = nullptr;
        struct FailureContext {
            sqlite3* out_db = nullptr;
            sqlite3_stmt* insert_failure = nullptr;
            sqlite3_stmt* upsert_summary = nullptr;
            const std::string* family_label = nullptr;
            int* failure_inclusive = nullptr;
            int* failure_exclusive = nullptr;
            std::uint64_t* failure_written = nullptr;
        } failure_ctx{out_db, insert_failure_stmt.get(), upsert_summary_stmt.get(), &options.sharp_family_label, &failure_count_inclusive, &failure_count_exclusive, &failure_rows_written};
        const auto failure_cb = [](void* data, int argc, char** argv, char**) -> int {
            if (argc < 15 || !argv[0] || !argv[1] || !argv[2]) return 1;
            auto* ctx = static_cast<FailureContext*>(data);
            const std::string band_id = argv[0];
            const std::string position_key = argv[1];
            const std::string move_uci = argv[2];
            const bool inclusive_reconciled = argv[3] ? std::stoi(argv[3]) != 0 : false;
            const bool exclusive_reconciled = argv[4] ? std::stoi(argv[4]) != 0 : false;
            ModeState inclusive;
            ModeState exclusive;
            inclusive.local_reason = argv[5] ? argv[5] : "";
            exclusive.local_reason = argv[6] ? argv[6] : "";
            if (argv[7]) inclusive.highest_locally_admitted_band = std::string(argv[7]);
            if (argv[8]) exclusive.highest_locally_admitted_band = std::string(argv[8]);
            if (argv[9]) inclusive.first_failing_higher_band = std::string(argv[9]);
            if (argv[10]) exclusive.first_failing_higher_band = std::string(argv[10]);
            if (argv[11]) inclusive.first_failure_reason = std::string(argv[11]);
            if (argv[12]) exclusive.first_failure_reason = std::string(argv[12]);
            inclusive.origin = argv[13] ? argv[13] : "not_admitted";
            exclusive.origin = argv[14] ? argv[14] : "not_admitted";

            if (!inclusive_reconciled) {
                const auto [reason_code, template_id] = classify_failure_reason(inclusive, "good_inclusive", inclusive_reconciled, exclusive_reconciled);
                bind_text(ctx->insert_failure, 1, band_id);
                bind_text(ctx->insert_failure, 2, position_key);
                bind_text(ctx->insert_failure, 3, move_uci);
                bind_text(ctx->insert_failure, 4, "good_inclusive");
                bind_text(ctx->insert_failure, 5, reason_code);
                bind_text(ctx->insert_failure, 6, template_id);
                bind_text(ctx->insert_failure, 7, *ctx->family_label);
                bind_text(ctx->insert_failure, 8, band_id);
                bind_optional_text(ctx->insert_failure, 9, inclusive.highest_locally_admitted_band);
                bind_optional_text(ctx->insert_failure, 10, inclusive.first_failing_higher_band);
                sqlite3_bind_null(ctx->insert_failure, 11);
                bind_text(ctx->insert_failure, 12, inclusive.origin);
                sqlite3_bind_null(ctx->insert_failure, 13);
                if (sqlite3_step(ctx->insert_failure) != SQLITE_DONE) return 1;
                sqlite3_reset(ctx->insert_failure);
                sqlite3_clear_bindings(ctx->insert_failure);
                upsert_root_summary(ctx->out_db, ctx->upsert_summary, band_id, position_key, 0, 0, 0, 0, 0, 0, 1, 0);
                ++(*ctx->failure_inclusive);
                ++(*ctx->failure_written);
            }
            if (!exclusive_reconciled) {
                const auto [reason_code, template_id] = classify_failure_reason(exclusive, "good_exclusive", inclusive_reconciled, exclusive_reconciled);
                std::optional<std::string> toggle = std::nullopt;
                if (reason_code == "would_pass_if_sharp_toggle_enabled" || reason_code == "disabled_by_sharp_toggle" || reason_code == "strict_mode_rejects_good") {
                    toggle = "sharp_on";
                }
                bind_text(ctx->insert_failure, 1, band_id);
                bind_text(ctx->insert_failure, 2, position_key);
                bind_text(ctx->insert_failure, 3, move_uci);
                bind_text(ctx->insert_failure, 4, "good_exclusive");
                bind_text(ctx->insert_failure, 5, reason_code);
                bind_text(ctx->insert_failure, 6, template_id);
                bind_text(ctx->insert_failure, 7, *ctx->family_label);
                bind_text(ctx->insert_failure, 8, band_id);
                bind_optional_text(ctx->insert_failure, 9, exclusive.highest_locally_admitted_band);
                bind_optional_text(ctx->insert_failure, 10, exclusive.first_failing_higher_band);
                bind_optional_text(ctx->insert_failure, 11, toggle);
                bind_text(ctx->insert_failure, 12, exclusive.origin);
                sqlite3_bind_null(ctx->insert_failure, 13);
                if (sqlite3_step(ctx->insert_failure) != SQLITE_DONE) return 1;
                sqlite3_reset(ctx->insert_failure);
                sqlite3_clear_bindings(ctx->insert_failure);
                upsert_root_summary(ctx->out_db, ctx->upsert_summary, band_id, position_key, 0, 0, 0, 0, 0, 0, 0, 1);
                ++(*ctx->failure_exclusive);
                ++(*ctx->failure_written);
            }
            return 0;
        };
        if (sqlite3_exec(
                out_db,
                "SELECT band_id, position_key, move_uci, reconciled_admitted_if_good_accepted, reconciled_admitted_if_good_rejected, local_reason_good_accepted, local_reason_good_rejected, highest_locally_admitted_band_good_accepted, highest_locally_admitted_band_good_rejected, first_failing_higher_band_good_accepted, first_failing_higher_band_good_rejected, first_failure_reason_good_accepted, first_failure_reason_good_rejected, admission_origin_good_accepted, admission_origin_good_rejected FROM reconciled_move_admissions ORDER BY band_id, position_key, move_uci",
                failure_cb,
                &failure_ctx,
                &err) != SQLITE_OK) {
            const std::string msg = err ? err : "sqlite error";
            sqlite3_free(err);
            throw std::runtime_error("failed emitting failure explanations: " + msg);
        }
        progress.stage_completed("emit-failure-explanations complete");

        progress.stage_started(ProgressStage::WriteArtifacts, "write-artifacts");
        {
            char* err2 = nullptr;
            const auto cb = [](void* data, int argc, char** argv, char**) -> int {
                if (argc > 0 && argv[0]) *static_cast<std::uint64_t*>(data) = static_cast<std::uint64_t>(std::stoull(argv[0]));
                return 0;
            };
            if (sqlite3_exec(out_db, "SELECT COUNT(*) FROM reconciled_root_summaries", cb, &root_summaries_finalized, &err2) != SQLITE_OK) {
                const std::string msg = err2 ? err2 : "sqlite error";
                sqlite3_free(err2);
                throw std::runtime_error("failed counting reconciled root summaries: " + msg);
            }
        }
        sqlite_exec(out_db, "COMMIT;");

        {
            std::ofstream manifest(bundle_dir / "manifest.json");
            manifest << "{\n";
            manifest << "  \"artifact_family_id\": \"" << json_escape(options.artifact_family_id) << "\",\n";
            manifest << "  \"artifact_role\": \"practical_risk_reconciled\",\n";
            manifest << "  \"time_control_id\": \"" << json_escape(options.time_control_id) << "\",\n";
            manifest << "  \"band_order\": [";
            for (std::size_t i = 0; i < options.band_order.size(); ++i) {
                if (i > 0) manifest << ", ";
                manifest << "\"" << json_escape(options.band_order[i]) << "\"";
            }
            manifest << "],\n";
            manifest << "  \"source_stage_c_root\": \"" << json_escape(options.final_bundle_root.empty() ? std::string("from_final_bundle_list") : options.final_bundle_root.string()) << "\",\n";
            manifest << "  \"downward_propagation_enabled\": true,\n";
            manifest << "  \"upward_description_enabled\": true,\n";
            manifest << "  \"success_threads_emitted\": false,\n";
            manifest << "  \"failure_threads_emitted\": true,\n";
            manifest << "  \"family_label\": \"" << json_escape(options.sharp_family_label) << "\"\n";
            manifest << "}\n";
        }

        {
            std::ofstream summary(bundle_dir / "summary.json");
            summary << "{\n";
            summary << "  \"bands_processed\": " << options.band_order.size() << ",\n";
            summary << "  \"reconciliation_groups_processed\": " << groups_processed << ",\n";
            summary << "  \"total_reconciled_move_rows\": " << reconciled_rows_written << ",\n";
            summary << "  \"failure_rows_written\": " << failure_rows_written << ",\n";
            summary << "  \"root_summaries_finalized\": " << root_summaries_finalized << ",\n";
            summary << "  \"downward_inherited_count_good_accepted\": " << inherited_inclusive << ",\n";
            summary << "  \"downward_inherited_count_good_rejected\": " << inherited_exclusive << ",\n";
            summary << "  \"failure_explanation_count_good_accepted\": " << failure_count_inclusive << ",\n";
            summary << "  \"failure_explanation_count_good_rejected\": " << failure_count_exclusive << ",\n";
            summary << "  \"moves_with_upward_failure_boundary_good_accepted\": " << with_boundary_inclusive << ",\n";
            summary << "  \"moves_with_upward_failure_boundary_good_rejected\": " << with_boundary_exclusive << "\n";
            summary << "}\n";
        }

        {
            std::ofstream build_summary(bundle_dir / "build_summary.txt");
            build_summary << "practical-risk reconciled build complete\n";
            build_summary << "bands_processed=" << options.band_order.size() << "\n";
            build_summary << "reconciliation_groups_processed=" << groups_processed << "\n";
            build_summary << "total_reconciled_move_rows=" << reconciled_rows_written << "\n";
            build_summary << "failure_rows_written=" << failure_rows_written << "\n";
            build_summary << "root_summaries_finalized=" << root_summaries_finalized << "\n";
            build_summary << "downward_inherited_count_good_accepted=" << inherited_inclusive << "\n";
            build_summary << "downward_inherited_count_good_rejected=" << inherited_exclusive << "\n";
            build_summary << "failure_explanation_count_good_accepted=" << failure_count_inclusive << "\n";
            build_summary << "failure_explanation_count_good_rejected=" << failure_count_exclusive << "\n";
            build_summary << "moves_with_upward_failure_boundary_good_accepted=" << with_boundary_inclusive << "\n";
            build_summary << "moves_with_upward_failure_boundary_good_rejected=" << with_boundary_exclusive << "\n";
            build_summary << "working_sqlite_path=" << work_db_path.string() << "\n";
        }

        if (work_db) {
            sqlite3_close(work_db);
            work_db = nullptr;
        }
        if (out_db) {
            sqlite3_close(out_db);
            out_db = nullptr;
        }

        progress.stage_completed("write-artifacts complete");
        progress.stage_started(ProgressStage::Finalize, "finalize");
        progress.stage_completed("finalize complete");
        progress.finish();
        return 0;
    } catch (...) {
        if (out_db) {
            sqlite3_exec(out_db, "ROLLBACK;", nullptr, nullptr, nullptr);
            sqlite3_close(out_db);
            out_db = nullptr;
        }
        if (work_db) {
            sqlite3_exec(work_db, "ROLLBACK;", nullptr, nullptr, nullptr);
            sqlite3_close(work_db);
            work_db = nullptr;
        }
        progress.stage_failed("practical-risk-reconciled failed");
        progress.finish();
        throw;
    }
}

}  // namespace otcb
