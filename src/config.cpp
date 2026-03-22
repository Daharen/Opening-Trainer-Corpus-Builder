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

std::string derive_artifact_id(const BuildConfig& config) {
    std::ostringstream builder;
    builder << "scaffold_";
    builder << config.min_rating << "-" << config.max_rating;
    builder << "_" << to_string(*config.rating_policy);
    builder << "_ply" << config.retained_ply;
    builder << "_g" << config.max_games;
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
    if (config.dry_run) {
        if (config.input_pgn.empty()) {
            errors.emplace_back("--input-pgn is required for --dry-run.");
        }
        if (config.output_dir.empty()) {
            errors.emplace_back("--output-dir is required for --dry-run.");
        }
    }

    return errors;
}

}  // namespace otcb
