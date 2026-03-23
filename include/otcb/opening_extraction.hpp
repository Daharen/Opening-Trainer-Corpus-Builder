#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "otcb/config.hpp"
#include "otcb/game_envelope.hpp"
#include "otcb/header_scan.hpp"
#include "otcb/preflight.hpp"
#include "otcb/range_plan.hpp"

namespace otcb {

enum class ExtractionFailureReason {
    None,
    TokenizationFailed,
    UnsupportedMovetextToken,
    IllegalSanMove,
    AmbiguousSanResolutionFailed,
    PromotionParseFailed,
    CastlingParseFailed,
    GameTerminatedBeforeRetainedPly,
    HeaderScanRejectedUpstream,
    UnexpectedReplayState,
};

std::string to_string(ExtractionFailureReason reason);

struct ExtractedPlyEvent {
    int ply_index = 0;
    std::string san;
    std::optional<std::string> uci;
    std::optional<std::string> fen_before;
    std::optional<std::string> fen_after;
    std::string side_to_move_before;
    bool move_was_legal = false;
    std::optional<std::string> termination_after_move;
};

struct ExtractedOpeningSequence {
    int range_index = 0;
    std::uint64_t header_start_byte = 0;
    std::uint64_t game_end_byte = 0;
    std::optional<int> white_elo;
    std::optional<int> black_elo;
    std::string result;
    std::string event;
    std::string site;
    std::string time_control;
    std::string classification;
    bool replayed_successfully = false;
    std::string failure_reason;
    int plies_requested = 0;
    int plies_extracted = 0;
    std::vector<ExtractedPlyEvent> ply_events;
};

struct ExtractionSummary {
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
    int total_replay_failures = 0;
    int total_extracted_plies = 0;
    std::map<std::string, int> replay_failure_counts;
    int preview_row_count_emitted = 0;
    std::vector<std::string> notes;
};

struct ExtractionResult {
    HeaderScanResult scan_result;
    ExtractionSummary summary;
    std::vector<ExtractedOpeningSequence> sequences;
    std::vector<ExtractedOpeningSequence> preview_rows;
};

ExtractionResult extract_openings(const BuildConfig& config, const SourcePreflightInfo& preflight_info, const RangePlan& range_plan);
std::string render_extracted_opening_sequences_jsonl(const std::vector<ExtractedOpeningSequence>& sequences, bool include_fen_snapshots, bool include_uci_moves);
std::string render_extraction_preview_jsonl(const std::vector<ExtractedOpeningSequence>& preview_rows);
std::string render_extraction_summary_json(const ExtractionSummary& summary);
std::string render_extraction_summary_text(const BuildConfig& config, const ExtractionSummary& summary);

}  // namespace otcb
