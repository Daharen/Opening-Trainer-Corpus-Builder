#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "otcb/config.hpp"

namespace otcb {

struct PracticalRiskScreenOptions {
    std::filesystem::path input_pgn;
    std::filesystem::path output_dir;
    std::string artifact_id;
    int min_rating = 0;
    int max_rating = 0;
    RatingPolicy rating_policy = RatingPolicy::BothInBand;
    int retained_ply = 20;
    std::vector<std::string> time_controls;
    std::string time_control_id;
    int initial_time_seconds = 0;
    int increment_seconds = 0;
    std::string time_format_label;

    int candidate_min_support = 100;
    int baseline_min_support = 100;
    int root_min_support = 300;
    std::filesystem::path engine_path;
    int engine_movetime_ms = 50;
    int engine_hash_mb = 64;
    int engine_threads = 1;
    int baseline_prefix_limit = 8;
    int candidate_prefix_limit = 8;
    bool emit_progress_log = false;
    bool emit_status_json = false;
    int heartbeat_seconds = 30;
    bool quiet_progress = false;

    std::string engine_accept_policy = "max_loss_cp";
    int engine_max_loss_cp = 80;
    std::string engine_reference_mode = "root_best";
};

PracticalRiskScreenOptions parse_practical_risk_screen_cli(int argc, char** argv);
void print_practical_risk_screen_usage(const std::string& program_name);
int run_practical_risk_screen(const PracticalRiskScreenOptions& options);

}  // namespace otcb
