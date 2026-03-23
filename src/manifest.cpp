#include "otcb/manifest.hpp"

#include <sstream>

namespace otcb {
namespace {

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

}  // namespace

ManifestData make_manifest_data(const BuildConfig& config, const BuildPlan& plan, const std::string& artifact_id, const HeaderScanSummary* scan_summary, const ExtractionSummary* extraction_summary) {
    ManifestData manifest{};
    manifest.artifact_schema_version = extraction_summary ? "otcb_scaffold_v4" : (scan_summary ? "otcb_scaffold_v3" : "otcb_scaffold_v2");
    manifest.artifact_id = artifact_id;
    manifest.builder_mode = to_string(config.mode);
    manifest.build_status = extraction_summary ? "opening_extraction_complete" : (scan_summary ? "header_scan_complete" : (plan.planning_completed ? "planning_complete" : (config.mode == BuildMode::DryRun ? "scaffold_complete" : "preflight_complete")));
    manifest.source_corpus_identity = "single_input_pgn_scaffold_source";
    manifest.input_pgn_path = config.input_pgn.lexically_normal().generic_string();
    manifest.input_format = config.input_format;
    manifest.rating_lower_bound = config.min_rating;
    manifest.rating_upper_bound = config.max_rating;
    manifest.rating_policy = to_string(*config.rating_policy);
    manifest.retained_opening_ply = config.retained_ply;
    manifest.threads = config.threads;
    manifest.max_games = config.max_games;
    manifest.progress_interval = config.progress_interval;
    manifest.raw_counts_preserved = true;
    manifest.effective_weights_precomputed = false;
    manifest.position_key_format = "not_yet_implemented_scaffold_position_keys";
    manifest.move_key_format = "not_yet_implemented_scaffold_move_keys";
    manifest.planner_target_range_bytes = config.target_range_bytes;
    manifest.planner_boundary_scan_bytes = config.boundary_scan_bytes;
    manifest.planner_max_ranges = config.max_ranges;
    manifest.planner_algorithm = plan.range_plan ? plan.range_plan->planner_algorithm : "not_requested";
    manifest.range_plan_emitted = plan.range_plan.has_value();
    manifest.range_plan_file = plan.range_plan ? "plans/range_plan.json" : "";
    manifest.range_count = plan.range_plan ? static_cast<int>(plan.range_plan->ranges.size()) : 0;
    manifest.header_scan_preview_emitted = scan_summary != nullptr && config.emit_header_preview;
    manifest.header_scan_preview_file = manifest.header_scan_preview_emitted ? "data/header_scan_preview.jsonl" : "";
    manifest.extraction_preview_emitted = extraction_summary != nullptr && config.emit_extraction_preview;
    manifest.extraction_preview_file = manifest.extraction_preview_emitted ? "data/extraction_preview.jsonl" : "";
    manifest.movetext_replay_performed = extraction_summary != nullptr;
    manifest.include_fen_snapshots = config.include_fen_snapshots;
    manifest.include_uci_moves = config.include_uci_moves;
    manifest.extracted_sequence_file = extraction_summary ? "data/extracted_opening_sequences.jsonl" : "";
    manifest.scan_algorithm = scan_summary ? scan_summary->scan_algorithm : "not_requested";
    manifest.replay_algorithm = extraction_summary ? "accepted_game_streaming_tokenize_and_legal_san_replay_v1" : "not_requested";
    manifest.payload_files = {"data/positions_placeholder.jsonl"};
    if (extraction_summary) {
        manifest.payload_files.push_back("data/extracted_opening_sequences.jsonl");
        if (config.emit_extraction_preview) {
            manifest.payload_files.push_back("data/extraction_preview.jsonl");
        }
    }
    manifest.payload_status = "placeholder_non_final_payload";
    manifest.notes = {
        "This builder artifact still does not emit final aggregated position-to-move corpus payload counts.",
        "Deterministic range ownership and explicit rating-policy eligibility remain first-class artifact dimensions.",
    };
    if (plan.preflight_info) {
        manifest.input_pgn_path = plan.preflight_info->canonical_input_path.generic_string();
        manifest.source_file_size_bytes = plan.preflight_info->file_size_bytes;
        manifest.source_file_extension = plan.preflight_info->file_extension;
    }
    if (scan_summary) {
        manifest.games_scanned = scan_summary->total_games_scanned;
        manifest.games_accepted = scan_summary->total_games_accepted;
        manifest.games_rejected = scan_summary->total_games_rejected;
        manifest.eligibility_counts = scan_summary->global_rejection_counts;
        manifest.eligibility_counts["accepted"] = scan_summary->total_games_accepted;
    }
    if (extraction_summary) {
        manifest.replay_attempts = extraction_summary->total_replay_attempts;
        manifest.replay_successes = extraction_summary->total_replay_successes;
        manifest.replay_failures = extraction_summary->total_replay_failures;
        manifest.replay_failure_counts = extraction_summary->replay_failure_counts;
        manifest.notes.push_back("extract-openings performs accepted-game SAN replay and early-ply extraction only; cross-game aggregation remains deferred.");
    } else if (scan_summary) {
        manifest.notes.push_back("scan-headers mode performs header-only eligibility classification; movetext replay remains deferred.");
    }
    for (const auto& warning : plan.warnings) {
        manifest.notes.push_back("warning: " + warning);
    }
    return manifest;
}

std::string render_manifest_json(const ManifestData& manifest) {
    std::ostringstream output;
    output << "{\n";
    output << "  \"artifact_schema_version\": \"" << json_escape(manifest.artifact_schema_version) << "\",\n";
    output << "  \"artifact_id\": \"" << json_escape(manifest.artifact_id) << "\",\n";
    output << "  \"builder_mode\": \"" << json_escape(manifest.builder_mode) << "\",\n";
    output << "  \"build_status\": \"" << json_escape(manifest.build_status) << "\",\n";
    output << "  \"source_corpus_identity\": \"" << json_escape(manifest.source_corpus_identity) << "\",\n";
    output << "  \"input_pgn_path\": \"" << json_escape(manifest.input_pgn_path) << "\",\n";
    output << "  \"source_file_size_bytes\": " << manifest.source_file_size_bytes << ",\n";
    output << "  \"source_file_extension\": \"" << json_escape(manifest.source_file_extension) << "\",\n";
    output << "  \"input_format\": \"" << json_escape(manifest.input_format) << "\",\n";
    output << "  \"rating_lower_bound\": " << manifest.rating_lower_bound << ",\n";
    output << "  \"rating_upper_bound\": " << manifest.rating_upper_bound << ",\n";
    output << "  \"rating_policy\": \"" << json_escape(manifest.rating_policy) << "\",\n";
    output << "  \"retained_opening_ply\": " << manifest.retained_opening_ply << ",\n";
    output << "  \"threads\": " << manifest.threads << ",\n";
    output << "  \"max_games\": " << manifest.max_games << ",\n";
    output << "  \"progress_interval\": " << manifest.progress_interval << ",\n";
    output << "  \"planner_target_range_bytes\": " << manifest.planner_target_range_bytes << ",\n";
    output << "  \"planner_boundary_scan_bytes\": " << manifest.planner_boundary_scan_bytes << ",\n";
    output << "  \"planner_max_ranges\": " << manifest.planner_max_ranges << ",\n";
    output << "  \"planner_algorithm\": \"" << json_escape(manifest.planner_algorithm) << "\",\n";
    output << "  \"range_plan_emitted\": " << (manifest.range_plan_emitted ? "true" : "false") << ",\n";
    output << "  \"range_plan_file\": \"" << json_escape(manifest.range_plan_file) << "\",\n";
    output << "  \"range_count\": " << manifest.range_count << ",\n";
    output << "  \"games_scanned\": " << manifest.games_scanned << ",\n";
    output << "  \"games_accepted\": " << manifest.games_accepted << ",\n";
    output << "  \"games_rejected\": " << manifest.games_rejected << ",\n";
    output << "  \"replay_attempts\": " << manifest.replay_attempts << ",\n";
    output << "  \"replay_successes\": " << manifest.replay_successes << ",\n";
    output << "  \"replay_failures\": " << manifest.replay_failures << ",\n";
    output << "  \"header_scan_preview_emitted\": " << (manifest.header_scan_preview_emitted ? "true" : "false") << ",\n";
    output << "  \"header_scan_preview_file\": \"" << json_escape(manifest.header_scan_preview_file) << "\",\n";
    output << "  \"movetext_replay_performed\": " << (manifest.movetext_replay_performed ? "true" : "false") << ",\n";
    output << "  \"extracted_sequence_file\": \"" << json_escape(manifest.extracted_sequence_file) << "\",\n";
    output << "  \"extraction_preview_emitted\": " << (manifest.extraction_preview_emitted ? "true" : "false") << ",\n";
    output << "  \"extraction_preview_file\": \"" << json_escape(manifest.extraction_preview_file) << "\",\n";
    output << "  \"include_fen_snapshots\": " << (manifest.include_fen_snapshots ? "true" : "false") << ",\n";
    output << "  \"include_uci_moves\": " << (manifest.include_uci_moves ? "true" : "false") << ",\n";
    output << "  \"scan_algorithm\": \"" << json_escape(manifest.scan_algorithm) << "\",\n";
    output << "  \"replay_algorithm\": \"" << json_escape(manifest.replay_algorithm) << "\",\n";
    output << "  \"eligibility_counts\": {\n";
    for (auto it = manifest.eligibility_counts.begin(); it != manifest.eligibility_counts.end(); ++it) {
        output << "    \"" << json_escape(it->first) << "\": " << it->second << (std::next(it) != manifest.eligibility_counts.end() ? "," : "") << "\n";
    }
    output << "  },\n  \"replay_failure_counts\": {\n";
    for (auto it = manifest.replay_failure_counts.begin(); it != manifest.replay_failure_counts.end(); ++it) {
        output << "    \"" << json_escape(it->first) << "\": " << it->second << (std::next(it) != manifest.replay_failure_counts.end() ? "," : "") << "\n";
    }
    output << "  },\n  \"payload_files\": [\n";
    for (std::size_t i = 0; i < manifest.payload_files.size(); ++i) {
        output << "    \"" << json_escape(manifest.payload_files[i]) << "\"" << (i + 1 < manifest.payload_files.size() ? "," : "") << "\n";
    }
    output << "  ],\n  \"payload_status\": \"" << json_escape(manifest.payload_status) << "\",\n";
    output << "  \"notes\": [\n";
    for (std::size_t i = 0; i < manifest.notes.size(); ++i) {
        output << "    \"" << json_escape(manifest.notes[i]) << "\"" << (i + 1 < manifest.notes.size() ? "," : "") << "\n";
    }
    output << "  ]\n}\n";
    return output.str();
}

std::string render_build_summary(const BuildConfig& config, const BuildPlan& plan, const std::string& artifact_id, const HeaderScanSummary* scan_summary, const ExtractionSummary* extraction_summary) {
    std::ostringstream output;
    output << "opening-trainer-corpus-builder scaffold version: 0.1.0\n";
    output << "artifact id: " << artifact_id << "\n";
    output << "selected build mode: " << to_string(config.mode) << "\n";
    output << "validated input path: " << config.input_pgn.lexically_normal().generic_string() << "\n";
    output << "output path: " << config.output_dir.lexically_normal().generic_string() << "\n";
    output << "rating bounds: [" << config.min_rating << ", " << config.max_rating << "]\n";
    output << "selected rating policy: " << to_string(*config.rating_policy) << "\n";
    output << "retained ply: " << config.retained_ply << "\n";
    output << "threads: " << config.threads << "\n";
    output << "max games: " << config.max_games << "\n";
    output << "progress interval: " << config.progress_interval << "\n";
    output << "target range bytes: " << config.target_range_bytes << "\n";
    output << "boundary scan bytes: " << config.boundary_scan_bytes << "\n";
    output << "planning completed successfully: " << (plan.planning_completed ? "yes" : "no") << "\n";
    if (plan.preflight_info) {
        output << "validated source file size: " << plan.preflight_info->file_size_bytes << "\n";
        output << "source file extension: " << plan.preflight_info->file_extension << "\n";
        output << "input format: " << plan.preflight_info->input_format << "\n";
    }
    output << "number of planned ranges: " << (plan.range_plan ? plan.range_plan->ranges.size() : 0) << "\n";
    output << "executed range count: " << (scan_summary ? scan_summary->total_ranges_executed : (extraction_summary ? extraction_summary->total_ranges_executed : 0)) << "\n";
    if (scan_summary) {
        output << "total games scanned: " << scan_summary->total_games_scanned << "\n";
        output << "total accepted games: " << scan_summary->total_games_accepted << "\n";
        output << "total rejected games: " << scan_summary->total_games_rejected << "\n";
    }
    if (extraction_summary) {
        output << "replay attempts: " << extraction_summary->total_replay_attempts << "\n";
        output << "replay successes: " << extraction_summary->total_replay_successes << "\n";
        output << "replay failures: " << extraction_summary->total_replay_failures << "\n";
        output << "preview rows emitted: " << extraction_summary->preview_row_count_emitted << "\n";
        output << "include FEN snapshots: " << (config.include_fen_snapshots ? "yes" : "no") << "\n";
        output << "include UCI moves: " << (config.include_uci_moves ? "yes" : "no") << "\n";
        output << "replay failure counts by reason:\n";
        if (extraction_summary->replay_failure_counts.empty()) {
            output << "- none\n";
        } else {
            for (const auto& [reason, count] : extraction_summary->replay_failure_counts) {
                output << "- " << reason << ": " << count << "\n";
            }
        }
        output << "aggregation remains deferred: yes\n";
    } else if (scan_summary) {
        output << "preview rows emitted: " << scan_summary->preview_row_count_emitted << "\n";
        output << "movetext replay remains deferred: " << (scan_summary->movetext_replay_deferred ? "yes" : "no") << "\n";
    }
    output << "build plan mode: " << plan.mode << "\n";
    if (!plan.warnings.empty()) {
        output << "warnings:\n";
        for (const auto& warning : plan.warnings) {
            output << "- " << warning << "\n";
        }
    }
    output << "intentionally not implemented yet:\n";
    for (const auto& item : plan.not_yet_implemented) {
        output << "- " << item << "\n";
    }
    return output.str();
}

}  // namespace otcb
