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
struct HeaderScanResult;
struct ExtractionResult;
struct AggregationResult;

BundleWriteResult write_plan_ranges_bundle(const BuildConfig& config, const SourcePreflightInfo& preflight_info, const RangePlan& range_plan);
BundleWriteResult write_scan_headers_bundle(const BuildConfig& config, const SourcePreflightInfo& preflight_info, const RangePlan& range_plan, const HeaderScanResult& scan_result);
BundleWriteResult write_extract_openings_bundle(const BuildConfig& config, const SourcePreflightInfo& preflight_info, const RangePlan& range_plan, const ExtractionResult& extraction_result);
BundleWriteResult write_aggregate_counts_bundle(const BuildConfig& config, const SourcePreflightInfo& preflight_info, const RangePlan& range_plan, const AggregationResult& aggregation_result);

}  // namespace otcb
