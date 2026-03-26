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

std::string progress_mode_string(const BuildConfig& config) {
    return config.quiet_progress
        ? ((config.emit_progress_log || config.emit_status_json) ? "file_only" : "suppressed")
        : ((config.emit_progress_log || config.emit_status_json) ? "console_and_file" : "console_only");
}

}  // namespace

ManifestData make_manifest_data(const BuildConfig& config, const BuildPlan& plan, const std::string& artifact_id, const HeaderScanSummary* scan_summary, const ExtractionSummary* extraction_summary, const AggregationSummary* aggregation_summary) {
    ManifestData manifest{};
    manifest.artifact_schema_version = aggregation_summary ? "otcb_scaffold_v5" : (extraction_summary ? "otcb_scaffold_v4" : (scan_summary ? "otcb_scaffold_v3" : "otcb_scaffold_v2"));
    manifest.artifact_id = artifact_id;
    manifest.builder_repo_commit = "unknown";
    manifest.builder_mode = to_string(config.mode);
    manifest.build_status = aggregation_summary ? "aggregation_complete" : (extraction_summary ? "opening_extraction_complete" : (scan_summary ? "header_scan_complete" : (plan.planning_completed ? "planning_complete" : (config.mode == BuildMode::DryRun ? "scaffold_complete" : "preflight_complete"))));
    manifest.source_corpus_identity = "single_input_pgn_scaffold_source";
    manifest.input_pgn_path = config.input_pgn.lexically_normal().generic_string();
    manifest.input_format = config.input_format;
    manifest.rating_lower_bound = config.min_rating;
    manifest.rating_upper_bound = config.max_rating;
    manifest.rating_policy = to_string(*config.rating_policy);
    manifest.retained_opening_ply = config.retained_ply;
    manifest.retained_ply_depth = config.retained_ply;
    manifest.max_supported_player_moves = config.retained_ply / 2;
    manifest.time_control_id = config.time_control_id;
    manifest.filtered_time_controls = config.time_controls;
    manifest.initial_time_seconds = config.initial_time_seconds;
    manifest.increment_seconds = config.increment_seconds;
    manifest.time_format_label = config.time_format_label;
    manifest.threads = config.threads;
    manifest.max_games = config.max_games;
    manifest.progress_interval = config.progress_interval;
    manifest.heartbeat_seconds = config.heartbeat_seconds;
    manifest.progress_log_emitted = config.emit_progress_log;
    manifest.status_json_emitted = config.emit_status_json;
    manifest.progress_reporting_mode = progress_mode_string(config);
    manifest.raw_counts_preserved = true;
    manifest.effective_weights_precomputed = false;
    manifest.payload_format = to_string(config.payload_format);
    manifest.payload_version = config.payload_format == PayloadFormat::ExactSqliteV2Compact ? "2" : "1";
    manifest.payload_format_canonical = config.payload_format == PayloadFormat::ExactSqliteV2Compact ? "exact_sqlite_v2_compact" : to_string(config.payload_format);
    manifest.compatibility_payload_format = config.payload_format == PayloadFormat::ExactSqliteV2Compact && config.emit_legacy_sqlite_mirror ? "sqlite_v1_legacy_mirror" : "";
    manifest.compatibility_payload_emitted = config.payload_format == PayloadFormat::ExactSqliteV2Compact && config.emit_legacy_sqlite_mirror;
    manifest.compatibility_payload_semantics_identical = manifest.compatibility_payload_emitted;
    manifest.position_key_format = config.position_key_format.has_value() ? to_string(*config.position_key_format) : "not_yet_implemented_scaffold_position_keys";
    manifest.move_key_format = config.move_key_format.has_value() ? to_string(*config.move_key_format) : "not_yet_implemented_scaffold_move_keys";
    manifest.position_key_format_description = config.payload_format == PayloadFormat::ExactSqliteV2Compact ? "positions.position_key_compact packed binary with positions.position_key_inspect deterministic reconstruction" : "canonical normalized FEN-family text key";
    manifest.move_representation_description = config.payload_format == PayloadFormat::ExactSqliteV2Compact ? "moves dictionary table keyed by move_id with deterministic uci_text mapping" : "uci move key text";
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
    manifest.aggregate_preview_emitted = aggregation_summary != nullptr && config.emit_aggregate_preview;
    manifest.aggregate_preview_file = manifest.aggregate_preview_emitted ? "data/aggregate_preview.jsonl" : "";
    manifest.movetext_replay_performed = extraction_summary != nullptr || aggregation_summary != nullptr;
    manifest.include_fen_snapshots = config.include_fen_snapshots;
    manifest.include_uci_moves = config.include_uci_moves;
    manifest.extracted_sequence_file = extraction_summary != nullptr || aggregation_summary != nullptr ? "data/extracted_opening_sequences.jsonl" : "";
    manifest.aggregate_position_file = (aggregation_summary && config.payload_format == PayloadFormat::Jsonl) ? "data/aggregated_position_move_counts.jsonl" : "";
    manifest.aggregate_sqlite_file = (aggregation_summary && config.payload_format == PayloadFormat::Sqlite) ? "data/corpus.sqlite" : "";
    manifest.scan_algorithm = scan_summary ? scan_summary->scan_algorithm : "not_requested";
    manifest.replay_algorithm = extraction_summary || aggregation_summary ? "accepted_game_streaming_tokenize_and_legal_san_replay_v1" : "not_requested";
    manifest.aggregation_algorithm = aggregation_summary ? "deterministic_position_key_then_uci_raw_count_v1" : "not_requested";
    manifest.payload_files = {"data/positions_placeholder.jsonl"};
    manifest.payload_status = "placeholder_non_final_payload";
    manifest.notes = {"Deterministic range ownership and explicit rating-policy eligibility remain first-class artifact dimensions."};
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
        manifest.total_extracted_ply_events = extraction_summary->total_extracted_plies;
        manifest.replay_failure_counts = extraction_summary->replay_failure_counts;
        manifest.payload_files.push_back("data/extracted_opening_sequences.jsonl");
        if (config.emit_extraction_preview) manifest.payload_files.push_back("data/extraction_preview.jsonl");
        manifest.notes.push_back("extract-openings performs accepted-game SAN replay and early-ply extraction only; cross-game aggregation remains deferred.");
    }
    if (aggregation_summary) {
        manifest.replay_attempts = aggregation_summary->total_replay_attempts;
        manifest.replay_successes = aggregation_summary->total_replay_successes;
        manifest.total_extracted_ply_events = aggregation_summary->total_extracted_ply_events_consumed;
        manifest.unique_positions_emitted = aggregation_summary->total_unique_positions_emitted;
        manifest.aggregate_move_entries_emitted = aggregation_summary->total_aggregate_move_entries_emitted;
        manifest.raw_observations_emitted = aggregation_summary->total_raw_observations_emitted;
        manifest.min_position_count = aggregation_summary->min_position_count;
        manifest.games_rejected_by_rating_filter = aggregation_summary->games_rejected_by_rating_filter;
        manifest.games_rejected_time_control_mismatch = aggregation_summary->games_rejected_by_time_control_filter;
        manifest.games_rejected_invalid_time_control = aggregation_summary->games_rejected_invalid_time_control;
        manifest.payload_files.push_back("data/extracted_opening_sequences.jsonl");
        if (config.payload_format == PayloadFormat::Sqlite) {
            manifest.payload_files.push_back("data/corpus.sqlite");
            manifest.sqlite_positions_rows = aggregation_summary->sqlite_positions_rows;
            manifest.sqlite_moves_rows = aggregation_summary->sqlite_moves_rows;
        } else if (config.payload_format == PayloadFormat::ExactSqliteV2Compact) {
            manifest.payload_files.push_back("data/corpus_compact.sqlite");
            if (config.emit_legacy_sqlite_mirror) manifest.payload_files.push_back("data/corpus.sqlite");
            manifest.aggregate_sqlite_file = "data/corpus_compact.sqlite";
            manifest.sqlite_positions_rows = aggregation_summary->sqlite_positions_rows;
            manifest.sqlite_moves_rows = aggregation_summary->sqlite_moves_rows;
        } else {
            manifest.payload_files.push_back("data/aggregated_position_move_counts.jsonl");
            manifest.aggregate_sqlite_file.clear();
        }
        if (config.emit_aggregate_preview) manifest.payload_files.push_back("data/aggregate_preview.jsonl");
        manifest.payload_status = "raw_aggregate_counts_present_non_final_trainer_payload";
        manifest.total_accepted_games = aggregation_summary->total_games_accepted_upstream;
        manifest.total_emitted_positions = aggregation_summary->total_unique_positions_emitted;
        manifest.total_emitted_move_entries = aggregation_summary->total_aggregate_move_entries_emitted;
        manifest.notes.push_back("aggregate-counts emits raw aggregated position->move counts now, while shaping, suppression, weighting, and final trainer-side consumption remain deferred.");
    } else if (scan_summary) {
        manifest.notes.push_back("scan-headers mode performs header-only eligibility classification; movetext replay remains deferred.");
    } else {
        manifest.notes.push_back("This builder artifact still does not emit final aggregated position-to-move corpus payload counts.");
    }
    for (const auto& warning : plan.warnings) manifest.notes.push_back("warning: " + warning);
    return manifest;
}

std::string render_manifest_json(const ManifestData& manifest) {
    std::ostringstream output;
    output << "{\n";
    output << "  \"artifact_schema_version\": \"" << json_escape(manifest.artifact_schema_version) << "\",\n";
    output << "  \"artifact_id\": \"" << json_escape(manifest.artifact_id) << "\",\n";
    output << "  \"builder_repo_commit\": \"" << json_escape(manifest.builder_repo_commit) << "\",\n";
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
    output << "  \"target_rating_band\": {\"minimum\": " << manifest.rating_lower_bound << ", \"maximum\": " << manifest.rating_upper_bound << "},\n";
    output << "  \"retained_opening_ply\": " << manifest.retained_opening_ply << ",\n";
    output << "  \"retained_ply_depth\": " << manifest.retained_ply_depth << ",\n";
    output << "  \"max_supported_player_moves\": " << manifest.max_supported_player_moves << ",\n";
    output << "  \"time_control_id\": \"" << json_escape(manifest.time_control_id) << "\",\n";
    output << "  \"filtered_time_controls\": [";
    for (std::size_t i = 0; i < manifest.filtered_time_controls.size(); ++i) {
        output << "\"" << json_escape(manifest.filtered_time_controls[i]) << "\"";
        if (i + 1 < manifest.filtered_time_controls.size()) output << ", ";
    }
    output << "],\n";
    output << "  \"initial_time_seconds\": " << manifest.initial_time_seconds << ",\n";
    output << "  \"increment_seconds\": " << manifest.increment_seconds << ",\n";
    output << "  \"time_format_label\": \"" << json_escape(manifest.time_format_label) << "\",\n";
    output << "  \"threads\": " << manifest.threads << ",\n";
    output << "  \"max_games\": " << manifest.max_games << ",\n";
    output << "  \"progress_interval\": " << manifest.progress_interval << ",\n";
    output << "  \"heartbeat_seconds\": " << manifest.heartbeat_seconds << ",\n";
    output << "  \"progress_log_emitted\": " << (manifest.progress_log_emitted ? "true" : "false") << ",\n";
    output << "  \"status_json_emitted\": " << (manifest.status_json_emitted ? "true" : "false") << ",\n";
    output << "  \"progress_reporting_mode\": \"" << json_escape(manifest.progress_reporting_mode) << "\",\n";
    output << "  \"raw_counts_preserved\": " << (manifest.raw_counts_preserved ? "true" : "false") << ",\n";
    output << "  \"effective_weights_precomputed\": " << (manifest.effective_weights_precomputed ? "true" : "false") << ",\n";
    output << "  \"payload_format\": \"" << json_escape(manifest.payload_format) << "\",\n";
    output << "  \"payload_version\": \"" << json_escape(manifest.payload_version) << "\",\n";
    output << "  \"canonical_payload_format\": \"" << json_escape(manifest.payload_format_canonical) << "\",\n";
    output << "  \"compatibility_payload_format\": \"" << json_escape(manifest.compatibility_payload_format) << "\",\n";
    output << "  \"compatibility_payload_emitted\": " << (manifest.compatibility_payload_emitted ? "true" : "false") << ",\n";
    output << "  \"compatibility_payload_semantics_identical\": " << (manifest.compatibility_payload_semantics_identical ? "true" : "false") << ",\n";
    output << "  \"position_key_format\": \"" << json_escape(manifest.position_key_format) << "\",\n";
    output << "  \"move_key_format\": \"" << json_escape(manifest.move_key_format) << "\",\n";
    output << "  \"position_key_format_description\": \"" << json_escape(manifest.position_key_format_description) << "\",\n";
    output << "  \"move_representation_description\": \"" << json_escape(manifest.move_representation_description) << "\",\n";
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
    output << "  \"games_rejected_by_rating_filter\": " << manifest.games_rejected_by_rating_filter << ",\n";
    output << "  \"games_rejected_time_control_mismatch\": " << manifest.games_rejected_time_control_mismatch << ",\n";
    output << "  \"games_rejected_invalid_time_control\": " << manifest.games_rejected_invalid_time_control << ",\n";
    output << "  \"replay_attempts\": " << manifest.replay_attempts << ",\n";
    output << "  \"replay_successes\": " << manifest.replay_successes << ",\n";
    output << "  \"replay_failures\": " << manifest.replay_failures << ",\n";
    output << "  \"total_extracted_ply_events\": " << manifest.total_extracted_ply_events << ",\n";
    output << "  \"aggregate_position_file\": \"" << json_escape(manifest.aggregate_position_file) << "\",\n";
    output << "  \"aggregate_sqlite_file\": \"" << json_escape(manifest.aggregate_sqlite_file) << "\",\n";
    output << "  \"header_scan_preview_emitted\": " << (manifest.header_scan_preview_emitted ? "true" : "false") << ",\n";
    output << "  \"header_scan_preview_file\": \"" << json_escape(manifest.header_scan_preview_file) << "\",\n";
    output << "  \"movetext_replay_performed\": " << (manifest.movetext_replay_performed ? "true" : "false") << ",\n";
    output << "  \"extracted_sequence_file\": \"" << json_escape(manifest.extracted_sequence_file) << "\",\n";
    output << "  \"extraction_preview_emitted\": " << (manifest.extraction_preview_emitted ? "true" : "false") << ",\n";
    output << "  \"extraction_preview_file\": \"" << json_escape(manifest.extraction_preview_file) << "\",\n";
    output << "  \"aggregate_preview_emitted\": " << (manifest.aggregate_preview_emitted ? "true" : "false") << ",\n";
    output << "  \"aggregate_preview_file\": \"" << json_escape(manifest.aggregate_preview_file) << "\",\n";
    output << "  \"include_fen_snapshots\": " << (manifest.include_fen_snapshots ? "true" : "false") << ",\n";
    output << "  \"include_uci_moves\": " << (manifest.include_uci_moves ? "true" : "false") << ",\n";
    output << "  \"scan_algorithm\": \"" << json_escape(manifest.scan_algorithm) << "\",\n";
    output << "  \"replay_algorithm\": \"" << json_escape(manifest.replay_algorithm) << "\",\n";
    output << "  \"aggregation_algorithm\": \"" << json_escape(manifest.aggregation_algorithm) << "\",\n";
    output << "  \"unique_positions_emitted\": " << manifest.unique_positions_emitted << ",\n";
    output << "  \"aggregate_move_entries_emitted\": " << manifest.aggregate_move_entries_emitted << ",\n";
    output << "  \"total_accepted_games\": " << manifest.total_accepted_games << ",\n";
    output << "  \"total_emitted_positions\": " << manifest.total_emitted_positions << ",\n";
    output << "  \"total_emitted_move_entries\": " << manifest.total_emitted_move_entries << ",\n";
    output << "  \"raw_observations_emitted\": " << manifest.raw_observations_emitted << ",\n";
    output << "  \"min_position_count\": " << manifest.min_position_count << ",\n";
    output << "  \"sqlite_positions_rows\": " << manifest.sqlite_positions_rows << ",\n";
    output << "  \"sqlite_moves_rows\": " << manifest.sqlite_moves_rows << ",\n";
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

std::string render_build_summary(const BuildConfig& config, const BuildPlan& plan, const std::string& artifact_id, const HeaderScanSummary* scan_summary, const ExtractionSummary* extraction_summary, const AggregationSummary* aggregation_summary) {
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
    output << "heartbeat cadence seconds: " << config.heartbeat_seconds << "\n";
    output << "progress log emitted: " << (config.emit_progress_log ? "yes" : "no") << "\n";
    output << "rolling status json emitted: " << (config.emit_status_json ? "yes" : "no") << "\n";
    output << "progress reporting mode: " << progress_mode_string(config) << "\n";
    output << "target range bytes: " << config.target_range_bytes << "\n";
    output << "boundary scan bytes: " << config.boundary_scan_bytes << "\n";
    if (config.position_key_format.has_value()) output << "position key format: " << to_string(*config.position_key_format) << "\n";
    if (config.move_key_format.has_value()) output << "move key format: " << to_string(*config.move_key_format) << "\n";
    output << "min position count: " << config.min_position_count << "\n";
    output << "payload format: " << to_string(config.payload_format) << "\n";
    if (!config.time_controls.empty()) {
        output << "filtered time controls: ";
        for (std::size_t i = 0; i < config.time_controls.size(); ++i) {
            output << config.time_controls[i] << (i + 1 < config.time_controls.size() ? "," : "");
        }
        output << "\n";
    }
    output << "payload path: " << (config.payload_format == PayloadFormat::Sqlite ? "data/corpus.sqlite" : "data/aggregated_position_move_counts.jsonl") << "\n";
    output << "planning completed successfully: " << (plan.planning_completed ? "yes" : "no") << "\n";
    if (plan.preflight_info) {
        output << "source file size: " << plan.preflight_info->file_size_bytes << "\n";
        output << "source file extension: " << plan.preflight_info->file_extension << "\n";
        output << "input format: " << plan.preflight_info->input_format << "\n";
    }
    output << "number of planned ranges: " << (plan.range_plan ? plan.range_plan->ranges.size() : 0) << "\n";
    output << "planner parameters: target_range_bytes=" << config.target_range_bytes << ", boundary_scan_bytes=" << config.boundary_scan_bytes << ", max_ranges=" << config.max_ranges << "\n";
    output << "executed range count: " << (scan_summary ? scan_summary->total_ranges_executed : (extraction_summary ? extraction_summary->total_ranges_executed : (aggregation_summary ? aggregation_summary->total_ranges_executed : 0))) << "\n";
    if (scan_summary) {
        output << "total scanned games: " << scan_summary->total_games_scanned << "\n";
        output << "total accepted games: " << scan_summary->total_games_accepted << "\n";
        output << "total rejected games: " << scan_summary->total_games_rejected << "\n";
    }
    if (extraction_summary) {
        output << "replay attempts: " << extraction_summary->total_replay_attempts << "\n";
        output << "replay successes: " << extraction_summary->total_replay_successes << "\n";
        output << "replay failures: " << extraction_summary->total_replay_failures << "\n";
        output << "total extracted ply events consumed: " << extraction_summary->total_extracted_plies << "\n";
        output << "preview rows emitted: " << extraction_summary->preview_row_count_emitted << "\n";
        output << "aggregation remains deferred: yes\n";
    }
    if (aggregation_summary) {
        output << "replay attempts: " << aggregation_summary->total_replay_attempts << "\n";
        output << "replay successes: " << aggregation_summary->total_replay_successes << "\n";
        output << "total extracted ply events consumed: " << aggregation_summary->total_extracted_ply_events_consumed << "\n";
        output << "total unique positions emitted: " << aggregation_summary->total_unique_positions_emitted << "\n";
        output << "total raw observations emitted: " << aggregation_summary->total_raw_observations_emitted << "\n";
        if (config.payload_format == PayloadFormat::Sqlite) {
            output << "sqlite row counts: positions=" << aggregation_summary->sqlite_positions_rows << ", moves=" << aggregation_summary->sqlite_moves_rows << "\n";
        }
        output << "aggregate preview rows emitted: " << aggregation_summary->preview_row_count_emitted << "\n";
        output << "explicit shaping remains deferred: yes\n";
    } else if (scan_summary) {
        output << "preview rows emitted: " << scan_summary->preview_row_count_emitted << "\n";
        output << "movetext replay remains deferred: " << (scan_summary->movetext_replay_deferred ? "yes" : "no") << "\n";
    }
    output << "unknown eta encountered in some stages: yes\n";
    output << "limited progress inferability in some stages: yes\n";
    output << "build plan mode: " << plan.mode << "\n";
    if (!plan.warnings.empty()) {
        output << "warnings:\n";
        for (const auto& warning : plan.warnings) output << "- " << warning << "\n";
    }
    output << "intentionally not implemented yet:\n";
    for (const auto& item : plan.not_yet_implemented) output << "- " << item << "\n";
    return output.str();
}

}  // namespace otcb
