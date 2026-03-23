#include "otcb/build_plan.hpp"

namespace otcb {

BuildPlan make_dry_run_build_plan() {
    BuildPlan plan;
    plan.mode = "dry_run_scaffold";
    plan.stages = {
        {"validate_configuration", "completed", "Validated required CLI parameters and scaffold constraints."},
        {"prepare_output_layout", "completed", "Create output, bundle, and data directories for scaffold artifacts."},
        {"scan_pgn_headers", "not_implemented", "Real PGN scanning/parsing is intentionally deferred in this baseline lane."},
        {"aggregate_positions", "not_implemented", "Position-keyed move-frequency aggregation is not implemented yet."},
        {"serialize_payload", "scaffold_placeholder", "Write a clearly non-final placeholder payload file."},
    };
    plan.not_yet_implemented = {
        "Full PGN parsing",
        "SAN move replay",
        "Worker-local aggregation",
        "Real position keys",
        "Final payload encoding",
    };
    plan.planning_completed = false;
    return plan;
}

BuildPlan make_preflight_build_plan(const SourcePreflightInfo& preflight_info, const bool include_range_plan, const RangePlan* range_plan) {
    BuildPlan plan;
    plan.mode = "preflight";
    plan.preflight_info = preflight_info;
    plan.planning_completed = include_range_plan && range_plan != nullptr;
    plan.stages = {
        {"validate_source", "completed", "Validated source readability, canonical path, and file metadata."},
        {"plan_ranges", include_range_plan ? "completed" : "skipped", include_range_plan ? "Deterministic range plan emitted on explicit request." : "Range planning skipped because --emit-range-plan was not requested."},
        {"serialize_payload", "not_implemented", "Final corpus payload emission remains intentionally unimplemented."},
    };
    plan.not_yet_implemented = {
        "Full PGN parsing",
        "SAN move replay",
        "Worker-local aggregation",
        "Final payload encoding",
    };
    if (range_plan != nullptr) {
        plan.range_plan = *range_plan;
        for (const auto& note : range_plan->plan_notes) {
            if (note.find("Conservative") != std::string::npos || note.find("Stopped") != std::string::npos) {
                plan.warnings.push_back(note);
            }
        }
    }
    return plan;
}

BuildPlan make_plan_ranges_build_plan(const SourcePreflightInfo& preflight_info, const RangePlan& range_plan) {
    BuildPlan plan;
    plan.mode = "plan_ranges";
    plan.preflight_info = preflight_info;
    plan.range_plan = range_plan;
    plan.planning_completed = true;
    plan.stages = {
        {"validate_source", "completed", "Validated source readability, canonical path, and file metadata."},
        {"plan_ranges", "completed", "Built deterministic range plan with explicit start-boundary reconciliation."},
        {"serialize_payload", "scaffold_placeholder", "Placeholder payload preserved; real corpus emission remains unimplemented."},
    };
    plan.not_yet_implemented = {
        "Full PGN parsing",
        "SAN move replay",
        "Worker-local aggregation",
        "Final payload encoding",
    };
    for (const auto& note : range_plan.plan_notes) {
        if (note.find("Conservative") != std::string::npos || note.find("Stopped") != std::string::npos) {
            plan.warnings.push_back(note);
        }
    }
    return plan;
}

BuildPlan make_scan_headers_build_plan(const SourcePreflightInfo& preflight_info, const RangePlan& range_plan) {
    BuildPlan plan;
    plan.mode = "scan_headers";
    plan.preflight_info = preflight_info;
    plan.range_plan = range_plan;
    plan.planning_completed = true;
    plan.stages = {
        {"validate_source", "completed", "Validated source readability, canonical path, and file metadata."},
        {"plan_ranges", "completed", "Reused deterministic safe-start range planner for execution ownership."},
        {"scan_headers", "completed", "Scanned game envelopes and classified headers under the explicit rating policy."},
        {"serialize_payload", "scaffold_placeholder", "Placeholder positions payload preserved; movetext replay remains deferred."},
    };
    plan.not_yet_implemented = {
        "SAN move replay",
        "Board-state reconstruction",
        "Position-key generation",
        "Move-frequency aggregation",
        "Final payload encoding",
    };
    for (const auto& note : range_plan.plan_notes) {
        if (note.find("Conservative") != std::string::npos || note.find("Stopped") != std::string::npos) {
            plan.warnings.push_back(note);
        }
    }
    return plan;
}

BuildPlan make_extract_openings_build_plan(const SourcePreflightInfo& preflight_info, const RangePlan& range_plan) {
    BuildPlan plan;
    plan.mode = "extract_openings";
    plan.preflight_info = preflight_info;
    plan.range_plan = range_plan;
    plan.planning_completed = true;
    plan.stages = {
        {"validate_source", "completed", "Validated source readability, canonical path, and file metadata."},
        {"plan_ranges", "completed", "Reused deterministic safe-start range planner for execution ownership."},
        {"scan_headers", "completed", "Reused header eligibility gate to identify accepted games before replay."},
        {"replay_openings", "completed", "Tokenized accepted-game movetext, replayed SAN, and emitted per-game opening sequences."},
        {"serialize_payload", "scaffold_placeholder", "Placeholder positions payload preserved; final aggregated counts remain deferred."},
    };
    plan.not_yet_implemented = {
        "Cross-game move-frequency aggregation",
        "Sparse-position handling",
        "Trainer-side consumption",
        "Final payload encoding",
    };
    for (const auto& note : range_plan.plan_notes) {
        if (note.find("Conservative") != std::string::npos || note.find("Stopped") != std::string::npos) {
            plan.warnings.push_back(note);
        }
    }
    return plan;
}

BuildPlan make_aggregate_counts_build_plan(const SourcePreflightInfo& preflight_info, const RangePlan& range_plan) {
    BuildPlan plan;
    plan.mode = "aggregate_counts";
    plan.preflight_info = preflight_info;
    plan.range_plan = range_plan;
    plan.planning_completed = true;
    plan.stages = {
        {"validate_source", "completed", "Validated source readability, canonical path, and file metadata."},
        {"plan_ranges", "completed", "Reused deterministic safe-start range planner for execution ownership."},
        {"scan_headers", "completed", "Reused header eligibility gate to identify accepted games before replay."},
        {"replay_openings", "completed", "Reused accepted-game SAN replay and early-ply extraction before aggregation."},
        {"aggregate_counts", "completed", "Aggregated canonical position->UCI raw counts with deterministic ordering and explicit min-position-count filtering."},
        {"serialize_payload", "completed", "Emitted inspectable aggregate payloads plus summaries; shaping and trainer-side semantics remain deferred."},
    };
    plan.not_yet_implemented = {
        "Sparse shaping",
        "Rare-move suppression",
        "Hybrid weighting",
        "Trainer-side consumption",
    };
    for (const auto& note : range_plan.plan_notes) {
        if (note.find("Conservative") != std::string::npos || note.find("Stopped") != std::string::npos) {
            plan.warnings.push_back(note);
        }
    }
    return plan;
}

}  // namespace otcb
