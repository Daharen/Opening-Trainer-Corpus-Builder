#include "otcb/config.hpp"

#include <sstream>

namespace otcb {

std::string to_string(RatingPolicy policy) {
    switch (policy) {
        case RatingPolicy::BothInBand:
            return "both_in_band";
        case RatingPolicy::AverageInBand:
            return "average_in_band";
        case RatingPolicy::WhiteInBand:
            return "white_in_band";
        case RatingPolicy::BlackInBand:
            return "black_in_band";
    }
    return "unknown";
}

std::optional<RatingPolicy> parse_rating_policy(const std::string& value) {
    if (value == "both_in_band") {
        return RatingPolicy::BothInBand;
    }
    if (value == "average_in_band") {
        return RatingPolicy::AverageInBand;
    }
    if (value == "white_in_band") {
        return RatingPolicy::WhiteInBand;
    }
    if (value == "black_in_band") {
        return RatingPolicy::BlackInBand;
    }
    return std::nullopt;
}

std::string rating_policy_help() {
    return
        "  both_in_band     Require both players to have ratings inside [min-rating, max-rating].\n"
        "  average_in_band  Require the average of white/black ratings to be inside the band.\n"
        "  white_in_band    Require only White's rating to be inside the band.\n"
        "  black_in_band    Require only Black's rating to be inside the band.";
}

std::string to_string(BuildMode mode) {
    switch (mode) {
        case BuildMode::DryRun:
            return "dry-run";
        case BuildMode::Preflight:
            return "preflight";
        case BuildMode::PlanRanges:
            return "plan-ranges";
        case BuildMode::ScanHeaders:
            return "scan-headers";
        case BuildMode::ExtractOpenings:
            return "extract-openings";
        case BuildMode::AggregateCounts:
            return "aggregate-counts";
    }
    return "unknown";
}

std::optional<BuildMode> parse_build_mode(const std::string& value) {
    if (value == "dry-run") {
        return BuildMode::DryRun;
    }
    if (value == "preflight") {
        return BuildMode::Preflight;
    }
    if (value == "plan-ranges") {
        return BuildMode::PlanRanges;
    }
    if (value == "scan-headers") {
        return BuildMode::ScanHeaders;
    }
    if (value == "extract-openings") {
        return BuildMode::ExtractOpenings;
    }
    if (value == "aggregate-counts") {
        return BuildMode::AggregateCounts;
    }
    return std::nullopt;
}

std::string to_string(const PositionKeyFormat format) {
    switch (format) {
        case PositionKeyFormat::FenNormalized:
            return "fen_normalized";
        case PositionKeyFormat::FenFull:
            return "fen_full";
    }
    return "unknown";
}

std::optional<PositionKeyFormat> parse_position_key_format(const std::string& value) {
    if (value == "fen_normalized") {
        return PositionKeyFormat::FenNormalized;
    }
    if (value == "fen_full") {
        return PositionKeyFormat::FenFull;
    }
    return std::nullopt;
}

std::string to_string(const MoveKeyFormat format) {
    switch (format) {
        case MoveKeyFormat::Uci:
            return "uci";
    }
    return "unknown";
}

std::optional<MoveKeyFormat> parse_move_key_format(const std::string& value) {
    if (value == "uci") {
        return MoveKeyFormat::Uci;
    }
    return std::nullopt;
}

std::string derive_artifact_id(const BuildConfig& config) {
    std::ostringstream builder;
    builder << "scaffold_";
    builder << config.min_rating << "-" << config.max_rating;
    builder << "_" << to_string(*config.rating_policy);
    builder << "_ply" << config.retained_ply;
    builder << "_g" << config.max_games;
    if (config.mode != BuildMode::DryRun) {
        builder << "_" << to_string(config.mode);
        builder << "_r" << config.target_range_bytes;
        builder << "_b" << config.boundary_scan_bytes;
        if (config.max_ranges > 0) {
            builder << "_m" << config.max_ranges;
        }
    }
    if (config.mode == BuildMode::AggregateCounts) {
        builder << "_pk" << to_string(*config.position_key_format);
        builder << "_mk" << to_string(*config.move_key_format);
        builder << "_mpc" << config.min_position_count;
    }
    return builder.str();
}

std::vector<std::string> validate_config(const BuildConfig& config) {
    std::vector<std::string> errors;

    if (!config.rating_policy.has_value()) {
        errors.emplace_back("--rating-policy is required and must be explicit.");
    }
    if (config.min_rating > config.max_rating) {
        errors.emplace_back("--min-rating must be less than or equal to --max-rating.");
    }
    if (config.retained_ply <= 0) {
        errors.emplace_back("--retained-ply must be greater than 0.");
    }
    if (config.threads < 1) {
        errors.emplace_back("--threads must be at least 1.");
    }
    if (config.max_games < 0) {
        errors.emplace_back("--max-games must be greater than or equal to 0.");
    }
    if (config.progress_interval < 1) {
        errors.emplace_back("--progress-interval must be at least 1.");
    }
    if (config.target_range_bytes == 0) {
        errors.emplace_back("--target-range-bytes must be greater than 0.");
    }
    if (config.boundary_scan_bytes == 0) {
        errors.emplace_back("--boundary-scan-bytes must be greater than 0.");
    }
    if (config.max_ranges < 0) {
        errors.emplace_back("--max-ranges must be greater than or equal to 0.");
    }
    if (config.header_preview_limit < 0) {
        errors.emplace_back("--header-preview-limit must be greater than or equal to 0.");
    }
    if (config.extraction_preview_limit < 0) {
        errors.emplace_back("--extraction-preview-limit must be greater than or equal to 0.");
    }
    if (config.aggregate_preview_limit < 0) {
        errors.emplace_back("--aggregate-preview-limit must be greater than or equal to 0.");
    }
    if (config.min_position_count < 1) {
        errors.emplace_back("--min-position-count must be at least 1.");
    }
    if (config.input_format != "pgn") {
        errors.emplace_back("--input-format currently only supports 'pgn'.");
    }

    const bool input_required = config.dry_run || config.mode == BuildMode::Preflight || config.mode == BuildMode::PlanRanges || config.mode == BuildMode::ScanHeaders || config.mode == BuildMode::ExtractOpenings || config.mode == BuildMode::AggregateCounts;
    const bool output_required = config.dry_run || config.mode == BuildMode::PlanRanges || config.mode == BuildMode::ScanHeaders || config.mode == BuildMode::ExtractOpenings || config.mode == BuildMode::AggregateCounts;

    if (input_required && config.input_pgn.empty()) {
        errors.emplace_back("--input-pgn is required for the selected build mode.");
    }
    if (output_required && config.output_dir.empty()) {
        errors.emplace_back("--output-dir is required for the selected build mode.");
    }
    if (config.mode == BuildMode::AggregateCounts && !config.position_key_format.has_value()) {
        errors.emplace_back("--position-key-format is required for aggregate-counts and must be explicit.");
    }
    if (config.mode == BuildMode::AggregateCounts && !config.move_key_format.has_value()) {
        errors.emplace_back("--move-key-format is required for aggregate-counts and must be explicit.");
    }

    return errors;
}

}  // namespace otcb
