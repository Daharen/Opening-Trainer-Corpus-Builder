#include "otcb/manifest.hpp"

#include <sstream>

namespace otcb {
namespace {

std::string json_escape(const std::string& input) {
    std::string escaped;
    escaped.reserve(input.size());
    for (const char ch : input) {
        switch (ch) {
            case '\\':
                escaped += "\\\\";
                break;
            case '"':
                escaped += "\\\"";
                break;
            case '\n':
                escaped += "\\n";
                break;
            case '\r':
                escaped += "\\r";
                break;
            case '\t':
                escaped += "\\t";
                break;
            default:
                escaped += ch;
                break;
        }
    }
    return escaped;
}

}  // namespace

ManifestData make_manifest_data(const BuildConfig& config, const BuildPlan&, const std::string& artifact_id) {
    return ManifestData{
        .artifact_schema_version = "otcb_scaffold_v1",
        .artifact_id = artifact_id,
        .builder_mode = config.dry_run ? "dry_run_scaffold" : "unsupported",
        .build_status = config.dry_run ? "scaffold_complete" : "not_started",
        .source_corpus_identity = "single_input_pgn_scaffold_source",
        .input_pgn_path = config.input_pgn.lexically_normal().generic_string(),
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
        .payload_files = {"data/positions_placeholder.jsonl"},
        .payload_status = "placeholder_non_final_payload",
        .notes = {
            "This artifact bundle is a deterministic scaffold emitted by the initial C++ baseline.",
            "Long-horizon data model remains position-keyed move-frequency data with raw counts preserved.",
            "The payload file is intentionally non-final and must not be treated as a completed corpus artifact.",
        },
    };
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
    output << "validated input path: " << config.input_pgn.lexically_normal().generic_string() << "\n";
    output << "output path: " << config.output_dir.lexically_normal().generic_string() << "\n";
    output << "rating bounds: [" << config.min_rating << ", " << config.max_rating << "]\n";
    output << "selected rating policy: " << to_string(*config.rating_policy) << "\n";
    output << "retained ply: " << config.retained_ply << "\n";
    output << "threads: " << config.threads << "\n";
    output << "max games: " << config.max_games << "\n";
    output << "progress interval: " << config.progress_interval << "\n";
    output << "dry-run scaffold only: " << (config.dry_run ? "yes" : "no") << "\n";
    output << "build plan mode: " << plan.mode << "\n";
    output << "intentionally not implemented yet:\n";
    for (const auto& item : plan.not_yet_implemented) {
        output << "- " << item << "\n";
    }
    return output.str();
}

}  // namespace otcb
