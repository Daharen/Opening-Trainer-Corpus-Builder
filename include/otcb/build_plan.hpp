#pragma once

#include <string>
#include <vector>

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
};

BuildPlan make_dry_run_build_plan();

}  // namespace otcb
