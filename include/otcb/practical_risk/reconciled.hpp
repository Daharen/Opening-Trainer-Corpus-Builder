#pragma once

#include <filesystem>
#include <string>
#include <vector>

namespace otcb {

struct PracticalRiskReconciledOptions {
    std::filesystem::path final_bundle_root;
    std::vector<std::filesystem::path> final_bundles;
    std::filesystem::path output_dir;
    std::string artifact_family_id;
    std::string time_control_id;
    std::vector<std::string> band_order;
    std::string sharp_family_label = "sharp/gambit";
    bool emit_progress_log = false;
    bool emit_status_json = false;
    int heartbeat_seconds = 30;
    bool quiet_progress = false;
};

PracticalRiskReconciledOptions parse_practical_risk_reconciled_cli(int argc, char** argv);
void print_practical_risk_reconciled_usage(const std::string& program_name);
int run_practical_risk_reconciled(const PracticalRiskReconciledOptions& options);

}  // namespace otcb
