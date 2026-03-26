#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace otcb {

enum class HeaderScanClassification {
    Accepted,
    RejectedMissingWhiteElo,
    RejectedMissingBlackElo,
    RejectedInvalidWhiteElo,
    RejectedInvalidBlackElo,
    RejectedPolicyMismatch,
    RejectedInvalidTimeControl,
    RejectedTimeControlMismatch,
    RejectedIncompleteHeaderBlock,
    RejectedIncompleteGameEnvelope,
    RejectedNonstandardOrUnsupportedHeaderShape,
};

std::string to_string(HeaderScanClassification classification);

struct ParsedGameHeaders {
    std::optional<std::string> event;
    std::optional<std::string> site;
    std::optional<std::string> date;
    std::optional<std::string> white;
    std::optional<std::string> black;
    std::optional<std::string> white_elo_raw;
    std::optional<std::string> black_elo_raw;
    std::optional<int> white_elo;
    std::optional<int> black_elo;
    std::optional<std::string> result;
    std::optional<std::string> termination;
    std::optional<std::string> time_control;
    std::optional<std::string> variant;
    std::optional<std::string> eco;
    std::map<std::string, std::string> extra_tags;
};

struct GameEnvelope {
    int range_index = 0;
    std::uint64_t header_start_byte = 0;
    std::uint64_t header_end_byte = 0;
    std::uint64_t movetext_start_byte = 0;
    std::uint64_t movetext_end_byte = 0;
    std::uint64_t game_end_byte = 0;
    ParsedGameHeaders headers;
    HeaderScanClassification classification = HeaderScanClassification::Accepted;
    std::optional<std::string> rejection_reason;
};

struct RangeScanSummary {
    int range_index = 0;
    std::uint64_t range_start_byte = 0;
    std::uint64_t range_end_byte = 0;
    int games_scanned = 0;
    int games_accepted = 0;
    int games_rejected = 0;
    std::map<std::string, int> rejection_counts;
    std::uint64_t bytes_scanned = 0;
    std::vector<std::string> notes;
};

struct HeaderScanSummary {
    std::string source_path;
    std::uint64_t source_size_bytes = 0;
    std::string planner_algorithm;
    std::uint64_t target_range_bytes = 0;
    std::uint64_t boundary_scan_bytes = 0;
    int retained_ply = 0;
    std::string rating_policy;
    int min_rating = 0;
    int max_rating = 0;
    int total_ranges_executed = 0;
    int total_games_scanned = 0;
    int total_games_accepted = 0;
    int total_games_rejected = 0;
    std::map<std::string, int> global_rejection_counts;
    int preview_row_count_emitted = 0;
    bool movetext_replay_deferred = true;
    std::string scan_algorithm;
    std::vector<RangeScanSummary> range_summaries;
    std::vector<std::string> notes;
};

}  // namespace otcb
