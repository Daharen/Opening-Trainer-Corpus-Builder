#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

namespace otcb {

enum class RatingPolicy {
    BothInBand,
    AverageInBand,
    WhiteInBand,
    BlackInBand,
};

enum class BuildMode {
    DryRun,
    Preflight,
    PlanRanges,
    ScanHeaders,
    ExtractOpenings,
    AggregateCounts,
};

enum class PositionKeyFormat {
    FenNormalized,
    FenFull,
};

enum class MoveKeyFormat {
    Uci,
};

enum class PayloadFormat {
    Jsonl,
    Sqlite,
    ExactSqliteV2Compact,
};

std::string to_string(RatingPolicy policy);
std::optional<RatingPolicy> parse_rating_policy(const std::string& value);
std::string rating_policy_help();

std::string to_string(BuildMode mode);
std::optional<BuildMode> parse_build_mode(const std::string& value);
std::string to_string(PositionKeyFormat format);
std::optional<PositionKeyFormat> parse_position_key_format(const std::string& value);
std::string to_string(MoveKeyFormat format);
std::optional<MoveKeyFormat> parse_move_key_format(const std::string& value);
std::string to_string(PayloadFormat format);
std::optional<PayloadFormat> parse_payload_format(const std::string& value);

struct BuildConfig {
    std::filesystem::path input_pgn;
    std::filesystem::path output_dir;
    int min_rating = 0;
    int max_rating = 0;
    std::optional<RatingPolicy> rating_policy;
    int retained_ply = 0;
    int threads = 1;
    int max_games = 0;
    std::optional<std::string> artifact_id;
    int progress_interval = 1;
    int heartbeat_seconds = 30;
    bool dry_run = false;
    bool help_requested = false;
    BuildMode mode = BuildMode::DryRun;
    std::uint64_t target_range_bytes = 64ULL * 1024ULL * 1024ULL;
    std::uint64_t boundary_scan_bytes = 1ULL * 1024ULL * 1024ULL;
    int max_ranges = 0;
    bool emit_range_plan = false;
    int header_preview_limit = 0;
    bool emit_header_preview = false;
    bool strict_header_scan = false;
    int extraction_preview_limit = 0;
    bool emit_extraction_preview = false;
    bool strict_san_replay = false;
    bool include_fen_snapshots = false;
    bool include_uci_moves = false;
    int aggregate_preview_limit = 0;
    bool emit_aggregate_preview = false;
    std::optional<PositionKeyFormat> position_key_format;
    std::optional<MoveKeyFormat> move_key_format;
    int min_position_count = 1;
    PayloadFormat payload_format = PayloadFormat::Jsonl;
    bool emit_legacy_sqlite_mirror = true;
    std::string time_control_id = "600+0";
    int initial_time_seconds = 600;
    int increment_seconds = 0;
    std::string time_format_label = "Rapid";
    std::string input_format = "pgn";
    bool emit_progress_log = false;
    bool emit_status_json = false;
    bool quiet_progress = false;
};

std::string derive_artifact_id(const BuildConfig& config);
std::vector<std::string> validate_config(const BuildConfig& config);

}  // namespace otcb
