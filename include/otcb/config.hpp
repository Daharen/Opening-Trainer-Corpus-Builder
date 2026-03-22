#pragma once

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

std::string to_string(RatingPolicy policy);
std::optional<RatingPolicy> parse_rating_policy(const std::string& value);
std::string rating_policy_help();

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
    bool dry_run = false;
    bool help_requested = false;
};

std::string derive_artifact_id(const BuildConfig& config);
std::vector<std::string> validate_config(const BuildConfig& config);

}  // namespace otcb
