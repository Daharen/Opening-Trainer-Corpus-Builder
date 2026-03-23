#pragma once

#include <string>
#include <vector>

#include "otcb/config.hpp"
#include "otcb/game_envelope.hpp"
#include "otcb/preflight.hpp"
#include "otcb/range_plan.hpp"

namespace otcb {

struct HeaderScanResult {
    HeaderScanSummary summary;
    std::vector<GameEnvelope> accepted_games;
    std::vector<GameEnvelope> preview_rows;
};

class ProgressReporter;
HeaderScanResult scan_headers(const BuildConfig& config, const SourcePreflightInfo& preflight_info, const RangePlan& range_plan, ProgressReporter* progress = nullptr);
std::string render_range_execution_summary_json(const HeaderScanSummary& summary);
std::string render_range_execution_summary_text(const BuildConfig& config, const HeaderScanSummary& summary);
std::string render_header_scan_preview_jsonl(const std::vector<GameEnvelope>& preview_rows);

}  // namespace otcb
