#pragma once

#include <filesystem>
#include <string>
#include <vector>

namespace otcb {

struct TimingConditionedBundleOptions {
    std::filesystem::path input_corpus_bundle;
    std::filesystem::path input_profile_set;
    std::filesystem::path output;
    bool overwrite = false;
    std::string artifact_id_override;
    std::string prototype_label;
    std::vector<std::string> time_controls;
    std::vector<std::string> elo_bands;
    int log_every = 10000;
    bool strict_compatibility = false;
    bool allow_prototype_mismatch = false;
    bool embed_fit_diagnostics = false;
    bool emit_progress_log = false;
    bool emit_status_json = false;
};

struct TimingConditionedBundleCounters {
    int corpus_artifacts_loaded = 0;
    int profile_artifacts_loaded = 0;
    int positions_examined = 0;
    int move_rows_examined = 0;
    int contexts_mapped = 0;
    int profiles_referenced = 0;
    int compatibility_warnings = 0;
    std::filesystem::path emitted_bundle_path;
};

TimingConditionedBundleOptions parse_timing_conditioned_bundle_cli(int argc, char** argv);
TimingConditionedBundleCounters build_timing_conditioned_corpus_bundle(const TimingConditionedBundleOptions& options);
void print_timing_conditioned_bundle_usage(const std::string& program_name);

}  // namespace otcb
