#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "otcb/config.hpp"
#include "otcb/preflight.hpp"

namespace otcb {

struct PlannedRange {
    int range_index = 0;
    std::uint64_t nominal_start_byte = 0;
    std::uint64_t adjusted_start_byte = 0;
    std::uint64_t nominal_end_byte = 0;
    std::optional<std::uint64_t> adjusted_end_byte;
    std::string start_adjustment_reason;
    std::string boundary_confidence;
    std::uint64_t estimated_span_bytes = 0;
};

struct RangePlan {
    std::string artifact_id;
    SourcePreflightInfo source_info;
    std::uint64_t target_range_bytes = 0;
    std::uint64_t boundary_scan_bytes = 0;
    int max_ranges = 0;
    std::string planner_algorithm;
    std::vector<PlannedRange> ranges;
    std::vector<std::string> plan_notes;
    bool deterministic = true;
};

class ProgressReporter;
RangePlan make_range_plan(const BuildConfig& config, const std::string& artifact_id, const SourcePreflightInfo& source_info, ProgressReporter* progress = nullptr);
std::string render_range_plan_json(const RangePlan& plan);
std::string render_range_plan_text(const RangePlan& plan);

}  // namespace otcb
