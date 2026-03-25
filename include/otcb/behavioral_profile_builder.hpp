#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <vector>

namespace otcb {

struct BehavioralProfileBuildOptions {
    std::vector<std::filesystem::path> input_extract_paths;
    std::filesystem::path output_path;
    std::vector<std::string> time_controls;
    std::vector<std::string> elo_bands;
    std::optional<std::string> month_window;
    int max_examples = 0;
    bool overwrite = false;
    int log_every = 10000;
    int seed_context_limit = 0;
    int min_support = 10;
    double merge_threshold = 0.12;
    bool strict = false;
    bool emit_fit_diagnostics = false;
    bool emit_invalid_report = false;
};

struct BehavioralProfileBuildCounters {
    int extract_files_loaded = 0;
    int raw_move_events_seen = 0;
    int training_examples_accepted = 0;
    int contexts_fitted = 0;
    int candidate_profiles_created = 0;
    int profiles_merged = 0;
    int final_move_pressure_profiles_emitted = 0;
    int final_think_time_profiles_emitted = 0;
};

BehavioralProfileBuildOptions parse_behavioral_profile_build_cli(int argc, char** argv);
BehavioralProfileBuildCounters build_behavioral_profiles(const BehavioralProfileBuildOptions& options);
void print_behavioral_profile_builder_usage(const std::string& program_name);

}  // namespace otcb
