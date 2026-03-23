#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "otcb/build_plan.hpp"
#include "otcb/config.hpp"

namespace otcb {

struct ManifestData {
    std::string artifact_schema_version;
    std::string artifact_id;
    std::string builder_mode;
    std::string build_status;
    std::string source_corpus_identity;
    std::string input_pgn_path;
    std::uint64_t source_file_size_bytes = 0;
    std::string source_file_extension;
    std::string input_format;
    int rating_lower_bound = 0;
    int rating_upper_bound = 0;
    std::string rating_policy;
    int retained_opening_ply = 0;
    int threads = 0;
    int max_games = 0;
    int progress_interval = 0;
    bool raw_counts_preserved = false;
    bool effective_weights_precomputed = false;
    std::string position_key_format;
    std::string move_key_format;
    std::uint64_t planner_target_range_bytes = 0;
    std::uint64_t planner_boundary_scan_bytes = 0;
    int planner_max_ranges = 0;
    std::string planner_algorithm;
    bool range_plan_emitted = false;
    std::string range_plan_file;
    int range_count = 0;
    std::vector<std::string> payload_files;
    std::string payload_status;
    std::vector<std::string> notes;
};

ManifestData make_manifest_data(const BuildConfig& config, const BuildPlan& plan, const std::string& artifact_id);
std::string render_manifest_json(const ManifestData& manifest);
std::string render_build_summary(const BuildConfig& config, const BuildPlan& plan, const std::string& artifact_id);

}  // namespace otcb
