#pragma once

#include <filesystem>
#include <string>

namespace otcb {

struct PracticalRiskStockfishOverlayOptions {
    std::filesystem::path prestockfish_bundle;
    std::filesystem::path output_dir;
    std::string artifact_id;
    std::filesystem::path engine_path;
    int engine_movetime_ms = 200;
    int engine_hash_mb = 256;
    int engine_threads = 4;
    std::string engine_accept_policy = "max_loss_cp";
    int engine_max_loss_cp = 40;
    std::string engine_reference_mode = "root_best";
    int baseline_prefix_limit = 8;
    int candidate_prefix_limit = 8;
    bool emit_progress_log = false;
    bool emit_status_json = false;
    int heartbeat_seconds = 30;
    bool quiet_progress = false;
};

PracticalRiskStockfishOverlayOptions parse_practical_risk_stockfish_overlay_cli(int argc, char** argv);
void print_practical_risk_stockfish_overlay_usage(const std::string& program_name);
int run_practical_risk_stockfish_overlay(const PracticalRiskStockfishOverlayOptions& options);

}  // namespace otcb
