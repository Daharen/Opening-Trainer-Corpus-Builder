#pragma once

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
    std::vector<std::string> payload_files;
    std::string payload_status;
    std::vector<std::string> notes;
};

ManifestData make_manifest_data(const BuildConfig& config, const BuildPlan& plan, const std::string& artifact_id);
std::string render_manifest_json(const ManifestData& manifest);
std::string render_build_summary(const BuildConfig& config, const BuildPlan& plan, const std::string& artifact_id);

}  // namespace otcb
