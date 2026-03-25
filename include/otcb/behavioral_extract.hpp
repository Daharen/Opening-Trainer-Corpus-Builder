#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include "otcb/config.hpp"

namespace otcb {

struct BehavioralExtractOptions {
    std::vector<std::filesystem::path> input_paths;
    std::filesystem::path output_path;
    std::vector<std::string> time_controls;
    std::vector<std::string> elo_bands;
    RatingPolicy rating_policy = RatingPolicy::BothInBand;
    std::optional<std::string> month_override;
    int max_games = 0;
    bool resume = false;
    bool overwrite = false;
    int workers = 1;
    int log_every = 1000;
    bool emit_invalid_report = false;
    std::optional<std::string> source_label;
    bool strict = false;
};

struct BehavioralExtractCounters {
    int files_processed = 0;
    int games_seen = 0;
    int games_accepted = 0;
    int games_rejected = 0;
    int move_events_emitted = 0;
    int rows_skipped_missing_clock = 0;
    int rows_skipped_invalid_time_control = 0;
};

BehavioralExtractOptions parse_behavioral_extract_cli(int argc, char** argv);
BehavioralExtractCounters build_behavioral_training_extract(const BehavioralExtractOptions& options);
void print_behavioral_extract_usage(const std::string& program_name);

}  // namespace otcb
