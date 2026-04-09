#include "otcb/practical_risk/reconciled.hpp"

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <tuple>
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

struct FailureExplanationRow {
    std::string band_id;
    std::string position_key;
    std::string move_uci;
    std::string mode_id;
    std::string reason_code;
    std::string template_id;
    std::string family_label;
    std::string current_band_id;
    std::optional<std::string> max_practical_band_id;
    std::optional<std::string> first_failure_band_id;
    std::optional<std::string> toggle_state_required;
    std::string origin_flag;
    std::optional<std::string> rendered_preview;
};

struct RootSummaryRow {
    std::string band_id;
    std::string position_key;
    int local_admitted_count_good_accepted = 0;
    int local_admitted_count_good_rejected = 0;
    int reconciled_admitted_count_good_accepted = 0;
    int reconciled_admitted_count_good_rejected = 0;
    int inherited_count_good_accepted = 0;
    int inherited_count_good_rejected = 0;
    int failure_explanation_count_good_accepted = 0;
    int failure_explanation_count_good_rejected = 0;
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
    std::vector<ReconciledMoveRow*>& ladder,
    const std::vector<std::string>& band_order,
    const std::function<ModeState&(ReconciledMoveRow&)>& selector) {

    std::optional<std::string> highest_local;
    for (std::size_t i = 0; i < ladder.size(); ++i) {
        if (selector(*ladder[i]).local_admitted != 0) {
            highest_local = band_order[i];
            break;
        }
    }

    for (std::size_t i = 0; i < ladder.size(); ++i) {
        auto& state = selector(*ladder[i]);
        state.highest_locally_admitted_band = highest_local;

        bool has_lower_or_equal_local = false;
        for (std::size_t j = i; j < ladder.size(); ++j) {
            if (selector(*ladder[j]).local_admitted != 0) {
                has_lower_or_equal_local = true;
                break;
            }
        }

        state.first_failing_higher_band = std::nullopt;
        state.first_failure_reason = std::nullopt;
        if (!has_lower_or_equal_local) continue;

        for (std::size_t j = i; j > 0; --j) {
            auto& higher = selector(*ladder[j - 1]);
            if (higher.local_admitted == 0) {
                state.first_failing_higher_band = band_order[j - 1];
                state.first_failure_reason = higher.local_reason;
                break;
            }
        }
    }
}

std::pair<std::string, std::string> classify_failure_reason(
    const ReconciledMoveRow&,
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

    if (mode_id == "good_exclusive" && !exclusive_reconciled && inclusive_reconciled) {
        return {"disabled_by_sharp_toggle", "FAIL_SHARP_TOGGLE_OFF"};
    }

    return {"failed_below_threshold", "FAIL_BELOW_THRESHOLD"};
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

    std::unordered_map<std::string, std::unordered_map<std::string, std::unordered_map<std::string, StageCRow>>> stage_rows;
    CompatibilityState compatibility;

    std::vector<ReconciledMoveRow> reconciled_rows;
    std::vector<FailureExplanationRow> failure_rows;
    std::vector<RootSummaryRow> summary_rows;

    int inherited_inclusive = 0;
    int inherited_exclusive = 0;
    int failure_count_inclusive = 0;
    int failure_count_exclusive = 0;
    int with_boundary_inclusive = 0;
    int with_boundary_exclusive = 0;

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

        progress.stage_started(ProgressStage::LoadPrestockfish, "load-stage-c-family");
        for (const auto& [band_id, bundle] : band_bundles) {
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
            const auto rows_cb = [](void* data, int argc, char** argv, char**) -> int {
                if (argc < 10 || !argv[0] || !argv[1]) return 1;
                auto* payload = static_cast<std::pair<std::string, std::unordered_map<std::string, std::unordered_map<std::string, std::unordered_map<std::string, StageCRow>>>*>*>(data);
                const std::string band_id_local = payload->first;
                auto* all_rows = payload->second;
                StageCRow row;
                row.admitted_if_good_accepted = argv[2] ? std::stoi(argv[2]) : 0;
                row.admitted_if_good_rejected = argv[3] ? std::stoi(argv[3]) : 0;
                row.admission_reason_good_accepted = argv[4] ? argv[4] : "";
                row.admission_reason_good_rejected = argv[5] ? argv[5] : "";
                row.engine_quality_class = argv[6] ? argv[6] : "unknown";
                row.ceiling = argv[7] ? std::stod(argv[7]) : 0.0;
                if (argv[8]) row.good_inclusive_min_ceiling = std::stod(argv[8]);
                if (argv[9]) row.good_exclusive_min_ceiling = std::stod(argv[9]);
                (*all_rows)[argv[0]][argv[1]][band_id_local] = std::move(row);
                return 0;
            };
            auto payload = std::make_pair(band_id, &stage_rows);
            if (sqlite3_exec(db, "SELECT position_key, move_uci, admitted_if_good_accepted, admitted_if_good_rejected, admission_reason_good_accepted, admission_reason_good_rejected, engine_quality_class, ceiling, good_inclusive_min_ceiling, good_exclusive_min_ceiling FROM final_move_admissions", rows_cb, &payload, &err) != SQLITE_OK) {
                const std::string msg = err ? err : "sqlite error";
                sqlite3_free(err);
                throw std::runtime_error("failed reading final_move_admissions for band " + band_id + ": " + msg);
            }
        }
        progress.stage_completed("load-stage-c-family complete");

        progress.stage_started(ProgressStage::ComputeRiskyOverlay, "compute-downward-reconciliation");
        for (auto& [position_key, move_map] : stage_rows) {
            for (auto& [move_uci, by_band] : move_map) {
                std::vector<ReconciledMoveRow*> ladder;
                ladder.reserve(options.band_order.size());
                bool has_higher_inclusive = false;
                bool has_higher_exclusive = false;

                for (const auto& band_id : options.band_order) {
                    const auto it = by_band.find(band_id);
                    StageCRow row;
                    if (it != by_band.end()) row = it->second;
                    else {
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
                    if (out.inclusive.origin == "inherited_from_higher_band") ++inherited_inclusive;

                    out.exclusive.local_admitted = row.admitted_if_good_rejected;
                    out.exclusive.local_reason = row.admission_reason_good_rejected;
                    out.exclusive.reconciled_admitted = (row.admitted_if_good_rejected != 0 || has_higher_exclusive) ? 1 : 0;
                    out.exclusive.origin = row.admitted_if_good_rejected != 0 ? "local" : (has_higher_exclusive ? "inherited_from_higher_band" : "not_admitted");
                    if (out.exclusive.origin == "inherited_from_higher_band") ++inherited_exclusive;

                    reconciled_rows.push_back(std::move(out));
                    ladder.push_back(&reconciled_rows.back());

                    has_higher_inclusive = has_higher_inclusive || row.admitted_if_good_accepted != 0;
                    has_higher_exclusive = has_higher_exclusive || row.admitted_if_good_rejected != 0;
                }

                fill_mode_boundary(ladder, options.band_order, [](ReconciledMoveRow& row) -> ModeState& { return row.inclusive; });
                fill_mode_boundary(ladder, options.band_order, [](ReconciledMoveRow& row) -> ModeState& { return row.exclusive; });
            }
        }
        progress.stage_completed("compute-downward-reconciliation complete");

        progress.stage_started(ProgressStage::ComputeAcceptedPriors, "compute-upward-failure-boundaries");
        for (const auto& row : reconciled_rows) {
            if (row.inclusive.first_failing_higher_band.has_value()) ++with_boundary_inclusive;
            if (row.exclusive.first_failing_higher_band.has_value()) ++with_boundary_exclusive;
        }
        progress.stage_completed("compute-upward-failure-boundaries complete");

        progress.stage_started(ProgressStage::EngineBaselineScreen, "emit-failure-explanations");
        for (const auto& row : reconciled_rows) {
            const bool inclusive_reconciled = row.inclusive.reconciled_admitted != 0;
            const bool exclusive_reconciled = row.exclusive.reconciled_admitted != 0;

            if (!inclusive_reconciled) {
                const auto [reason_code, template_id] = classify_failure_reason(row, row.inclusive, "good_inclusive", inclusive_reconciled, exclusive_reconciled);
                failure_rows.push_back(FailureExplanationRow{
                    .band_id = row.band_id,
                    .position_key = row.position_key,
                    .move_uci = row.move_uci,
                    .mode_id = "good_inclusive",
                    .reason_code = reason_code,
                    .template_id = template_id,
                    .family_label = options.sharp_family_label,
                    .current_band_id = row.band_id,
                    .max_practical_band_id = row.inclusive.highest_locally_admitted_band,
                    .first_failure_band_id = row.inclusive.first_failing_higher_band,
                    .toggle_state_required = std::nullopt,
                    .origin_flag = row.inclusive.origin,
                    .rendered_preview = std::nullopt,
                });
                ++failure_count_inclusive;
            }

            if (!exclusive_reconciled) {
                auto [reason_code, template_id] = classify_failure_reason(row, row.exclusive, "good_exclusive", inclusive_reconciled, exclusive_reconciled);
                std::optional<std::string> toggle = std::nullopt;
                if (reason_code == "would_pass_if_sharp_toggle_enabled" || reason_code == "disabled_by_sharp_toggle" || reason_code == "strict_mode_rejects_good") {
                    toggle = "sharp_on";
                }
                failure_rows.push_back(FailureExplanationRow{
                    .band_id = row.band_id,
                    .position_key = row.position_key,
                    .move_uci = row.move_uci,
                    .mode_id = "good_exclusive",
                    .reason_code = reason_code,
                    .template_id = template_id,
                    .family_label = options.sharp_family_label,
                    .current_band_id = row.band_id,
                    .max_practical_band_id = row.exclusive.highest_locally_admitted_band,
                    .first_failure_band_id = row.exclusive.first_failing_higher_band,
                    .toggle_state_required = toggle,
                    .origin_flag = row.exclusive.origin,
                    .rendered_preview = std::nullopt,
                });
                ++failure_count_exclusive;
            }
        }
        progress.stage_completed("emit-failure-explanations complete");

        progress.stage_started(ProgressStage::WriteArtifacts, "write-artifacts");

        std::map<std::pair<std::string, std::string>, RootSummaryRow> root_summary_by_key;
        for (const auto& row : reconciled_rows) {
            auto& s = root_summary_by_key[{row.band_id, row.position_key}];
            s.band_id = row.band_id;
            s.position_key = row.position_key;
            s.local_admitted_count_good_accepted += row.inclusive.local_admitted;
            s.local_admitted_count_good_rejected += row.exclusive.local_admitted;
            s.reconciled_admitted_count_good_accepted += row.inclusive.reconciled_admitted;
            s.reconciled_admitted_count_good_rejected += row.exclusive.reconciled_admitted;
            if (row.inclusive.origin == "inherited_from_higher_band") ++s.inherited_count_good_accepted;
            if (row.exclusive.origin == "inherited_from_higher_band") ++s.inherited_count_good_rejected;
        }
        for (const auto& row : failure_rows) {
            auto& s = root_summary_by_key[{row.band_id, row.position_key}];
            s.band_id = row.band_id;
            s.position_key = row.position_key;
            if (row.mode_id == "good_inclusive") ++s.failure_explanation_count_good_accepted;
            else if (row.mode_id == "good_exclusive") ++s.failure_explanation_count_good_rejected;
        }
        summary_rows.reserve(root_summary_by_key.size());
        for (const auto& [_, row] : root_summary_by_key) summary_rows.push_back(row);

        sqlite3* out_db = nullptr;
        const auto out_db_path = bundle_dir / "practical_risk_reconciled.sqlite";
        if (sqlite3_open(out_db_path.string().c_str(), &out_db) != SQLITE_OK) {
            throw std::runtime_error("unable to open output sqlite");
        }
        std::unique_ptr<sqlite3, decltype(&sqlite3_close)> out_guard(out_db, sqlite3_close);

        sqlite_exec(out_db,
            "BEGIN IMMEDIATE;"
            "CREATE TABLE artifact_metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);"
            "CREATE TABLE reconciled_move_admissions(band_id TEXT NOT NULL, position_key TEXT NOT NULL, move_uci TEXT NOT NULL, local_admitted_if_good_accepted INTEGER NOT NULL, local_admitted_if_good_rejected INTEGER NOT NULL, local_reason_good_accepted TEXT NOT NULL, local_reason_good_rejected TEXT NOT NULL, reconciled_admitted_if_good_accepted INTEGER NOT NULL, reconciled_admitted_if_good_rejected INTEGER NOT NULL, admission_origin_good_accepted TEXT NOT NULL, admission_origin_good_rejected TEXT NOT NULL, engine_quality_class TEXT NOT NULL, ceiling REAL NOT NULL, good_inclusive_min_ceiling REAL NULL, good_exclusive_min_ceiling REAL NULL, highest_locally_admitted_band_good_accepted TEXT NULL, highest_locally_admitted_band_good_rejected TEXT NULL, first_failing_higher_band_good_accepted TEXT NULL, first_failing_higher_band_good_rejected TEXT NULL, first_failure_reason_good_accepted TEXT NULL, first_failure_reason_good_rejected TEXT NULL, PRIMARY KEY(band_id, position_key, move_uci));"
            "CREATE TABLE failure_explanations(band_id TEXT NOT NULL, position_key TEXT NOT NULL, move_uci TEXT NOT NULL, mode_id TEXT NOT NULL, reason_code TEXT NOT NULL, template_id TEXT NOT NULL, family_label TEXT NOT NULL, current_band_id TEXT NOT NULL, max_practical_band_id TEXT NULL, first_failure_band_id TEXT NULL, toggle_state_required TEXT NULL, origin_flag TEXT NOT NULL, rendered_preview TEXT NULL, PRIMARY KEY(band_id, position_key, move_uci, mode_id));"
            "CREATE TABLE reconciled_root_summaries(band_id TEXT NOT NULL, position_key TEXT NOT NULL, local_admitted_count_good_accepted INTEGER NOT NULL, local_admitted_count_good_rejected INTEGER NOT NULL, reconciled_admitted_count_good_accepted INTEGER NOT NULL, reconciled_admitted_count_good_rejected INTEGER NOT NULL, inherited_count_good_accepted INTEGER NOT NULL, inherited_count_good_rejected INTEGER NOT NULL, failure_explanation_count_good_accepted INTEGER NOT NULL, failure_explanation_count_good_rejected INTEGER NOT NULL, PRIMARY KEY(band_id, position_key));"
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
        sqlite3_finalize(meta_stmt);

        sqlite3_stmt* row_stmt = nullptr;
        sqlite3_prepare_v2(out_db,
            "INSERT INTO reconciled_move_admissions(band_id, position_key, move_uci, local_admitted_if_good_accepted, local_admitted_if_good_rejected, local_reason_good_accepted, local_reason_good_rejected, reconciled_admitted_if_good_accepted, reconciled_admitted_if_good_rejected, admission_origin_good_accepted, admission_origin_good_rejected, engine_quality_class, ceiling, good_inclusive_min_ceiling, good_exclusive_min_ceiling, highest_locally_admitted_band_good_accepted, highest_locally_admitted_band_good_rejected, first_failing_higher_band_good_accepted, first_failing_higher_band_good_rejected, first_failure_reason_good_accepted, first_failure_reason_good_rejected) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21)",
            -1,
            &row_stmt,
            nullptr);
        for (const auto& row : reconciled_rows) {
            sqlite3_bind_text(row_stmt, 1, row.band_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(row_stmt, 2, row.position_key.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(row_stmt, 3, row.move_uci.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(row_stmt, 4, row.inclusive.local_admitted);
            sqlite3_bind_int(row_stmt, 5, row.exclusive.local_admitted);
            sqlite3_bind_text(row_stmt, 6, row.inclusive.local_reason.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(row_stmt, 7, row.exclusive.local_reason.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(row_stmt, 8, row.inclusive.reconciled_admitted);
            sqlite3_bind_int(row_stmt, 9, row.exclusive.reconciled_admitted);
            sqlite3_bind_text(row_stmt, 10, row.inclusive.origin.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(row_stmt, 11, row.exclusive.origin.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(row_stmt, 12, row.engine_quality_class.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_double(row_stmt, 13, row.ceiling);
            if (row.good_inclusive_min_ceiling.has_value()) sqlite3_bind_double(row_stmt, 14, *row.good_inclusive_min_ceiling); else sqlite3_bind_null(row_stmt, 14);
            if (row.good_exclusive_min_ceiling.has_value()) sqlite3_bind_double(row_stmt, 15, *row.good_exclusive_min_ceiling); else sqlite3_bind_null(row_stmt, 15);
            if (row.inclusive.highest_locally_admitted_band.has_value()) sqlite3_bind_text(row_stmt, 16, row.inclusive.highest_locally_admitted_band->c_str(), -1, SQLITE_TRANSIENT); else sqlite3_bind_null(row_stmt, 16);
            if (row.exclusive.highest_locally_admitted_band.has_value()) sqlite3_bind_text(row_stmt, 17, row.exclusive.highest_locally_admitted_band->c_str(), -1, SQLITE_TRANSIENT); else sqlite3_bind_null(row_stmt, 17);
            if (row.inclusive.first_failing_higher_band.has_value()) sqlite3_bind_text(row_stmt, 18, row.inclusive.first_failing_higher_band->c_str(), -1, SQLITE_TRANSIENT); else sqlite3_bind_null(row_stmt, 18);
            if (row.exclusive.first_failing_higher_band.has_value()) sqlite3_bind_text(row_stmt, 19, row.exclusive.first_failing_higher_band->c_str(), -1, SQLITE_TRANSIENT); else sqlite3_bind_null(row_stmt, 19);
            if (row.inclusive.first_failure_reason.has_value()) sqlite3_bind_text(row_stmt, 20, row.inclusive.first_failure_reason->c_str(), -1, SQLITE_TRANSIENT); else sqlite3_bind_null(row_stmt, 20);
            if (row.exclusive.first_failure_reason.has_value()) sqlite3_bind_text(row_stmt, 21, row.exclusive.first_failure_reason->c_str(), -1, SQLITE_TRANSIENT); else sqlite3_bind_null(row_stmt, 21);
            if (sqlite3_step(row_stmt) != SQLITE_DONE) throw std::runtime_error("reconciled_move_admissions insert failed");
            sqlite3_reset(row_stmt);
            sqlite3_clear_bindings(row_stmt);
        }
        sqlite3_finalize(row_stmt);

        sqlite3_stmt* fail_stmt = nullptr;
        sqlite3_prepare_v2(out_db,
            "INSERT INTO failure_explanations(band_id, position_key, move_uci, mode_id, reason_code, template_id, family_label, current_band_id, max_practical_band_id, first_failure_band_id, toggle_state_required, origin_flag, rendered_preview) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
            -1,
            &fail_stmt,
            nullptr);
        for (const auto& row : failure_rows) {
            sqlite3_bind_text(fail_stmt, 1, row.band_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(fail_stmt, 2, row.position_key.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(fail_stmt, 3, row.move_uci.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(fail_stmt, 4, row.mode_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(fail_stmt, 5, row.reason_code.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(fail_stmt, 6, row.template_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(fail_stmt, 7, row.family_label.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(fail_stmt, 8, row.current_band_id.c_str(), -1, SQLITE_TRANSIENT);
            if (row.max_practical_band_id.has_value()) sqlite3_bind_text(fail_stmt, 9, row.max_practical_band_id->c_str(), -1, SQLITE_TRANSIENT); else sqlite3_bind_null(fail_stmt, 9);
            if (row.first_failure_band_id.has_value()) sqlite3_bind_text(fail_stmt, 10, row.first_failure_band_id->c_str(), -1, SQLITE_TRANSIENT); else sqlite3_bind_null(fail_stmt, 10);
            if (row.toggle_state_required.has_value()) sqlite3_bind_text(fail_stmt, 11, row.toggle_state_required->c_str(), -1, SQLITE_TRANSIENT); else sqlite3_bind_null(fail_stmt, 11);
            sqlite3_bind_text(fail_stmt, 12, row.origin_flag.c_str(), -1, SQLITE_TRANSIENT);
            if (row.rendered_preview.has_value()) sqlite3_bind_text(fail_stmt, 13, row.rendered_preview->c_str(), -1, SQLITE_TRANSIENT); else sqlite3_bind_null(fail_stmt, 13);
            if (sqlite3_step(fail_stmt) != SQLITE_DONE) throw std::runtime_error("failure_explanations insert failed");
            sqlite3_reset(fail_stmt);
            sqlite3_clear_bindings(fail_stmt);
        }
        sqlite3_finalize(fail_stmt);

        sqlite3_stmt* sum_stmt = nullptr;
        sqlite3_prepare_v2(out_db,
            "INSERT INTO reconciled_root_summaries(band_id, position_key, local_admitted_count_good_accepted, local_admitted_count_good_rejected, reconciled_admitted_count_good_accepted, reconciled_admitted_count_good_rejected, inherited_count_good_accepted, inherited_count_good_rejected, failure_explanation_count_good_accepted, failure_explanation_count_good_rejected) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            -1,
            &sum_stmt,
            nullptr);
        for (const auto& row : summary_rows) {
            sqlite3_bind_text(sum_stmt, 1, row.band_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(sum_stmt, 2, row.position_key.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(sum_stmt, 3, row.local_admitted_count_good_accepted);
            sqlite3_bind_int(sum_stmt, 4, row.local_admitted_count_good_rejected);
            sqlite3_bind_int(sum_stmt, 5, row.reconciled_admitted_count_good_accepted);
            sqlite3_bind_int(sum_stmt, 6, row.reconciled_admitted_count_good_rejected);
            sqlite3_bind_int(sum_stmt, 7, row.inherited_count_good_accepted);
            sqlite3_bind_int(sum_stmt, 8, row.inherited_count_good_rejected);
            sqlite3_bind_int(sum_stmt, 9, row.failure_explanation_count_good_accepted);
            sqlite3_bind_int(sum_stmt, 10, row.failure_explanation_count_good_rejected);
            if (sqlite3_step(sum_stmt) != SQLITE_DONE) throw std::runtime_error("reconciled_root_summaries insert failed");
            sqlite3_reset(sum_stmt);
            sqlite3_clear_bindings(sum_stmt);
        }
        sqlite3_finalize(sum_stmt);

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
            summary << "  \"total_reconciled_move_rows\": " << reconciled_rows.size() << ",\n";
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
            build_summary << "total_reconciled_move_rows=" << reconciled_rows.size() << "\n";
            build_summary << "downward_inherited_count_good_accepted=" << inherited_inclusive << "\n";
            build_summary << "downward_inherited_count_good_rejected=" << inherited_exclusive << "\n";
            build_summary << "failure_explanation_count_good_accepted=" << failure_count_inclusive << "\n";
            build_summary << "failure_explanation_count_good_rejected=" << failure_count_exclusive << "\n";
            build_summary << "moves_with_upward_failure_boundary_good_accepted=" << with_boundary_inclusive << "\n";
            build_summary << "moves_with_upward_failure_boundary_good_rejected=" << with_boundary_exclusive << "\n";
        }

        progress.stage_completed("write-artifacts complete");
        progress.stage_started(ProgressStage::Finalize, "finalize");
        progress.stage_completed("finalize complete");
        progress.finish();
        return 0;
    } catch (...) {
        progress.stage_failed("practical-risk-reconciled failed");
        progress.finish();
        throw;
    }
}

}  // namespace otcb
