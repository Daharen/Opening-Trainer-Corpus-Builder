#pragma once

#include <filesystem>
#include <string>

#include "otcb/config.hpp"
#include "otcb/preflight.hpp"
#include "otcb/range_plan.hpp"

namespace otcb {

struct BundleWriteResult {
    std::filesystem::path bundle_root;
    std::string artifact_id;
};

BundleWriteResult write_dry_run_bundle(const BuildConfig& config);
BundleWriteResult write_preflight_bundle(const BuildConfig& config, const SourcePreflightInfo& preflight_info, const RangePlan* range_plan = nullptr);
BundleWriteResult write_plan_ranges_bundle(const BuildConfig& config, const SourcePreflightInfo& preflight_info, const RangePlan& range_plan);

}  // namespace otcb
