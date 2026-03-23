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

class ProgressReporter;
BundleWriteResult write_dry_run_bundle(const BuildConfig& config, ProgressReporter* progress = nullptr);
BundleWriteResult write_preflight_bundle(const BuildConfig& config, const SourcePreflightInfo& preflight_info, const RangePlan* range_plan = nullptr, ProgressReporter* progress = nullptr);
struct HeaderScanResult;
struct ExtractionResult;
struct AggregationResult;

BundleWriteResult write_plan_ranges_bundle(const BuildConfig& config, const SourcePreflightInfo& preflight_info, const RangePlan& range_plan, ProgressReporter* progress = nullptr);
BundleWriteResult write_scan_headers_bundle(const BuildConfig& config, const SourcePreflightInfo& preflight_info, const RangePlan& range_plan, const HeaderScanResult& scan_result, ProgressReporter* progress = nullptr);
BundleWriteResult write_extract_openings_bundle(const BuildConfig& config, const SourcePreflightInfo& preflight_info, const RangePlan& range_plan, const ExtractionResult& extraction_result, ProgressReporter* progress = nullptr);
BundleWriteResult write_aggregate_counts_bundle(const BuildConfig& config, const SourcePreflightInfo& preflight_info, const RangePlan& range_plan, const AggregationResult& aggregation_result, ProgressReporter* progress = nullptr);

}  // namespace otcb
