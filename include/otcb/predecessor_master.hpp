#pragma once

#include <filesystem>

#include "otcb/config.hpp"
#include "otcb/progress.hpp"

namespace otcb {

struct PredecessorMasterResult {
    std::filesystem::path master_output;
    int total_sources = 0;
    std::int64_t rows_scanned = 0;
    std::int64_t rows_inserted = 0;
    std::int64_t rows_skipped_existing = 0;
};

PredecessorMasterResult build_predecessor_master(const BuildConfig& config, ProgressReporter* reporter);

}  // namespace otcb
