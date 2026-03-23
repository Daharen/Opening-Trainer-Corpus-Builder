#pragma once

#include <optional>
#include <string>
#include <vector>

#include "otcb/preflight.hpp"
#include "otcb/range_plan.hpp"

namespace otcb {

struct BuildPlanStage {
    std::string name;
    std::string status;
    std::string details;
};

struct BuildPlan {
    std::string mode;
    std::vector<BuildPlanStage> stages;
    std::vector<std::string> not_yet_implemented;
    std::optional<SourcePreflightInfo> preflight_info;
    std::optional<RangePlan> range_plan;
    bool planning_completed = false;
    std::vector<std::string> warnings;
};

BuildPlan make_dry_run_build_plan();
BuildPlan make_preflight_build_plan(const SourcePreflightInfo& preflight_info, bool include_range_plan, const RangePlan* range_plan);
BuildPlan make_plan_ranges_build_plan(const SourcePreflightInfo& preflight_info, const RangePlan& range_plan);
BuildPlan make_scan_headers_build_plan(const SourcePreflightInfo& preflight_info, const RangePlan& range_plan);

BuildPlan make_extract_openings_build_plan(const SourcePreflightInfo& preflight_info, const RangePlan& range_plan);

}  // namespace otcb
