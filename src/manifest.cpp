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

ManifestData make_manifest_data(const BuildConfig& config, const BuildPlan& plan, const std::string& artifact_id) {
    ManifestData manifest{
        .artifact_schema_version = "otcb_scaffold_v2",
        .artifact_id = artifact_id,
        .builder_mode = to_string(config.mode),
        .build_status = plan.planning_completed ? "planning_complete" : (config.mode == BuildMode::DryRun ? "scaffold_complete" : "preflight_complete"),
        .source_corpus_identity = "single_input_pgn_scaffold_source",
        .input_pgn_path = config.input_pgn.lexically_normal().generic_string(),
        .source_file_size_bytes = 0,
        .source_file_extension = "",
        .input_format = config.input_format,
        .rating_lower_bound = config.min_rating,
        .rating_upper_bound = config.max_rating,
        .rating_policy = to_string(*config.rating_policy),
        .retained_opening_ply = config.retained_ply,
        .threads = config.threads,
        .max_games = config.max_games,
        .progress_interval = config.progress_interval,
        .raw_counts_preserved = true,
        .effective_weights_precomputed = false,
        .position_key_format = "not_yet_implemented_scaffold_position_keys",
        .move_key_format = "not_yet_implemented_scaffold_move_keys",
        .planner_target_range_bytes = config.target_range_bytes,
        .planner_boundary_scan_bytes = config.boundary_scan_bytes,
        .planner_max_ranges = config.max_ranges,
        .planner_algorithm = plan.range_plan ? plan.range_plan->planner_algorithm : "not_requested",
        .range_plan_emitted = plan.range_plan.has_value(),
        .range_plan_file = plan.range_plan ? "plans/range_plan.json" : "",
        .range_count = plan.range_plan ? static_cast<int>(plan.range_plan->ranges.size()) : 0,
        .payload_files = {"data/positions_placeholder.jsonl"},
        .payload_status = "placeholder_non_final_payload",
        .notes = {
            "This artifact bundle remains a bounded-scope builder artifact and does not yet contain final corpus payload data.",
            "Explicit rating policy and retained ply remain first-class artifact identity dimensions even before parsing is implemented.",
            "Range planning uses conservative forward scanning for aligned [Event headers instead of blind byte chunking.",
        },
    };

    if (plan.preflight_info) {
        manifest.input_pgn_path = plan.preflight_info->canonical_input_path.generic_string();
        manifest.source_file_size_bytes = plan.preflight_info->file_size_bytes;
        manifest.source_file_extension = plan.preflight_info->file_extension;
    }

    if (!plan.warnings.empty()) {
        for (const auto& warning : plan.warnings) {
            manifest.notes.push_back("warning: " + warning);
        }
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
    output << "  \"raw_counts_preserved\": " << (manifest.raw_counts_preserved ? "true" : "false") << ",\n";
    output << "  \"effective_weights_precomputed\": " << (manifest.effective_weights_precomputed ? "true" : "false") << ",\n";
    output << "  \"position_key_format\": \"" << json_escape(manifest.position_key_format) << "\",\n";
    output << "  \"move_key_format\": \"" << json_escape(manifest.move_key_format) << "\",\n";
    output << "  \"planner_target_range_bytes\": " << manifest.planner_target_range_bytes << ",\n";
    output << "  \"planner_boundary_scan_bytes\": " << manifest.planner_boundary_scan_bytes << ",\n";
    output << "  \"planner_max_ranges\": " << manifest.planner_max_ranges << ",\n";
    output << "  \"planner_algorithm\": \"" << json_escape(manifest.planner_algorithm) << "\",\n";
    output << "  \"range_plan_emitted\": " << (manifest.range_plan_emitted ? "true" : "false") << ",\n";
    output << "  \"range_plan_file\": \"" << json_escape(manifest.range_plan_file) << "\",\n";
    output << "  \"range_count\": " << manifest.range_count << ",\n";
    output << "  \"payload_files\": [\n";
    for (std::size_t index = 0; index < manifest.payload_files.size(); ++index) {
        output << "    \"" << json_escape(manifest.payload_files[index]) << "\"";
        output << (index + 1 < manifest.payload_files.size() ? "," : "") << "\n";
    }
    output << "  ],\n";
    output << "  \"payload_status\": \"" << json_escape(manifest.payload_status) << "\",\n";
    output << "  \"notes\": [\n";
    for (std::size_t index = 0; index < manifest.notes.size(); ++index) {
        output << "    \"" << json_escape(manifest.notes[index]) << "\"";
        output << (index + 1 < manifest.notes.size() ? "," : "") << "\n";
    }
    output << "  ]\n";
    output << "}\n";
    return output.str();
}

std::string render_build_summary(const BuildConfig& config, const BuildPlan& plan, const std::string& artifact_id) {
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
    output << "dry-run scaffold only: " << (config.mode == BuildMode::DryRun ? "yes" : "no") << "\n";
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
