#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "otcb/aggregation.hpp"
#include "otcb/build_plan.hpp"
#include "otcb/config.hpp"
#include "otcb/game_envelope.hpp"
#include "otcb/opening_extraction.hpp"

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
    int games_scanned = 0;
    int games_accepted = 0;
    int games_rejected = 0;
    int replay_attempts = 0;
    int replay_successes = 0;
    int replay_failures = 0;
    int total_extracted_ply_events = 0;
    std::map<std::string, int> replay_failure_counts;
    bool header_scan_preview_emitted = false;
    std::string header_scan_preview_file;
    bool extraction_preview_emitted = false;
    std::string extraction_preview_file;
    bool aggregate_preview_emitted = false;
    std::string aggregate_preview_file;
    bool movetext_replay_performed = false;
    bool include_fen_snapshots = false;
    bool include_uci_moves = false;
    std::string extracted_sequence_file;
    std::string aggregate_position_file;
    std::string scan_algorithm;
    std::string replay_algorithm;
    std::string aggregation_algorithm;
    std::map<std::string, int> eligibility_counts;
    int unique_positions_emitted = 0;
    int aggregate_move_entries_emitted = 0;
    int raw_observations_emitted = 0;
    int min_position_count = 0;
    std::vector<std::string> payload_files;
    std::string payload_status;
    std::vector<std::string> notes;
};

ManifestData make_manifest_data(const BuildConfig& config, const BuildPlan& plan, const std::string& artifact_id, const HeaderScanSummary* scan_summary = nullptr, const ExtractionSummary* extraction_summary = nullptr, const AggregationSummary* aggregation_summary = nullptr);
std::string render_manifest_json(const ManifestData& manifest);
std::string render_build_summary(const BuildConfig& config, const BuildPlan& plan, const std::string& artifact_id, const HeaderScanSummary* scan_summary = nullptr, const ExtractionSummary* extraction_summary = nullptr, const AggregationSummary* aggregation_summary = nullptr);

}  // namespace otcb
