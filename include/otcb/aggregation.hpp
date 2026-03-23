#pragma once

#include <string>
#include <vector>

#include "otcb/config.hpp"
#include "otcb/opening_extraction.hpp"
#include "otcb/preflight.hpp"
#include "otcb/range_plan.hpp"

namespace otcb {

struct AggregatedMoveCount {
    std::string move_key;
    int raw_count = 0;
    std::string example_san;
};

struct AggregatedPositionRecord {
    std::string position_key;
    std::string side_to_move;
    int candidate_move_count = 0;
    int total_observations = 0;
    std::vector<AggregatedMoveCount> candidate_moves;
};

struct AggregationSummary {
    std::string source_path;
    std::uint64_t source_size_bytes = 0;
    std::string planner_algorithm;
    std::uint64_t target_range_bytes = 0;
    std::uint64_t boundary_scan_bytes = 0;
    std::string rating_policy;
    int min_rating = 0;
    int max_rating = 0;
    int retained_ply = 0;
    int total_ranges_executed = 0;
    int total_games_scanned = 0;
    int total_games_accepted_upstream = 0;
    int total_replay_attempts = 0;
    int total_replay_successes = 0;
    int total_extracted_ply_events_consumed = 0;
    int total_unique_positions_emitted = 0;
    int total_aggregate_move_entries_emitted = 0;
    int total_raw_observations_emitted = 0;
    int min_position_count = 0;
    int positions_filtered_by_min_count = 0;
    int observations_filtered_by_min_count = 0;
    int preview_row_count_emitted = 0;
    std::string position_key_format;
    std::string move_key_format;
    std::vector<std::string> notes;
};

struct AggregationResult {
    ExtractionResult extraction_result;
    AggregationSummary summary;
    std::vector<AggregatedPositionRecord> positions;
    std::vector<AggregatedPositionRecord> preview_rows;
};

class ProgressReporter;
AggregationResult aggregate_counts(const BuildConfig& config, const SourcePreflightInfo& preflight_info, const RangePlan& range_plan, ProgressReporter* progress = nullptr);
std::string render_aggregated_position_move_counts_jsonl(const std::vector<AggregatedPositionRecord>& positions, const BuildConfig& config);
std::string render_aggregate_preview_jsonl(const std::vector<AggregatedPositionRecord>& preview_rows, const BuildConfig& config);
std::string render_aggregation_summary_json(const AggregationSummary& summary);
std::string render_aggregation_summary_text(const BuildConfig& config, const AggregationSummary& summary);

}  // namespace otcb
