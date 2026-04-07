#pragma once

#include <filesystem>
#include <string>
#include <vector>

#include "otcb/aggregation.hpp"
#include "otcb/config.hpp"

namespace otcb {

struct GambitCompanionWriteStats {
    std::string payload_file;
    int ordinary_rows = 0;
    int baseline_rows = 0;
    int family_rows = 0;
    int metrics_rows = 0;
    int acceptance_rows = 0;
    int unresolved_rows = 0;
    int rejected_rows = 0;
    int admitted_rows = 0;
    int pooling_events = 0;
};

class ProgressReporter;
GambitCompanionWriteStats write_gambit_companion_sqlite(
    const std::filesystem::path& sqlite_path,
    const BuildConfig& config,
    const AggregationSummary& summary,
    const std::vector<AggregatedPositionRecord>& positions,
    ProgressReporter* progress = nullptr);

}  // namespace otcb
