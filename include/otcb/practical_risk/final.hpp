#pragma once

#include <filesystem>
#include <string>

namespace otcb {

struct PracticalRiskFinalOptions {
    std::filesystem::path prestockfish_bundle;
    std::filesystem::path stockfish_overlay_bundle;
    std::filesystem::path output_dir;
    std::string artifact_id;
    bool emit_progress_log = false;
    bool emit_status_json = false;
    int heartbeat_seconds = 30;
    bool quiet_progress = false;
};

PracticalRiskFinalOptions parse_practical_risk_final_cli(int argc, char** argv);
void print_practical_risk_final_usage(const std::string& program_name);
int run_practical_risk_final(const PracticalRiskFinalOptions& options);

}  // namespace otcb
