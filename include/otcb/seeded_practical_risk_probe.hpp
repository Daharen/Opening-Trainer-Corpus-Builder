#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "otcb/config.hpp"

namespace otcb {

struct SeededPracticalRiskProbeDefinition {
    std::string probe_id;
    std::string position_key;
    std::string side_to_optimize;
    std::vector<std::string> candidate_moves;
    std::vector<std::string> baseline_moves;
    int horizon_plies = 0;
    int search_max_ply = 30;
    int entry_min_support = 1;
    int self_move_min_support = 1;
    int reply_min_support = 1;
    double prior_weight = 0.0;
    double delta_max = 0.0;
};

struct SeededPracticalRiskProbeOptions {
    std::filesystem::path input_pgn;
    std::filesystem::path output_dir;
    std::string artifact_id;
    int min_rating = 0;
    int max_rating = 0;
    RatingPolicy rating_policy = RatingPolicy::BothInBand;
    std::vector<std::string> time_controls;
    std::string time_control_id;
    int initial_time_seconds = 0;
    int increment_seconds = 0;
    std::string time_format_label;
    std::filesystem::path probe_spec;
    bool emit_progress_log = false;
    bool emit_status_json = false;
    bool quiet_progress = false;
    int heartbeat_seconds = 30;
};

SeededPracticalRiskProbeOptions parse_seeded_practical_risk_probe_cli(int argc, char** argv);
void print_seeded_practical_risk_probe_usage(const std::string& program_name);
int run_seeded_practical_risk_probe(const SeededPracticalRiskProbeOptions& options);

}  // namespace otcb
