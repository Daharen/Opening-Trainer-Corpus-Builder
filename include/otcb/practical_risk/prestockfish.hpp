#pragma once

#include <filesystem>
#include <string>
#include <vector>

#include "otcb/config.hpp"

namespace otcb {

struct PracticalRiskPrestockfishOptions {
    std::filesystem::path input_pgn;
    std::filesystem::path output_dir;
    std::string artifact_id;
    int min_rating = 0;
    int max_rating = 0;
    RatingPolicy rating_policy = RatingPolicy::BothInBand;
    int retained_ply = 0;
    std::vector<std::string> time_controls;
    std::string time_control_id;
    int initial_time_seconds = 0;
    int increment_seconds = 0;
    std::string time_format_label;

    int root_min_support = 100;
    int move_min_support = 100;
    int deep_line_min_support = 100;
    int deep_total_plies = 6;
    int deep_own_plies = 3;
    int sigma_global_min_lines = 3;

    bool emit_progress_log = false;
    bool emit_status_json = false;
    int heartbeat_seconds = 30;
    bool quiet_progress = false;
};

PracticalRiskPrestockfishOptions parse_practical_risk_prestockfish_cli(int argc, char** argv);
void print_practical_risk_prestockfish_usage(const std::string& program_name);
int run_practical_risk_prestockfish(const PracticalRiskPrestockfishOptions& options);

}  // namespace otcb
