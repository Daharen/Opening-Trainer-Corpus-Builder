#include <exception>
#include <iostream>

#include "otcb/aggregation.hpp"
#include "otcb/bundle_writer.hpp"
#include "otcb/cli.hpp"
#include "otcb/header_scan.hpp"
#include "otcb/opening_extraction.hpp"
#include "otcb/preflight.hpp"
#include "otcb/range_plan.hpp"

int main(int argc, char** argv) {
    const auto parsed = otcb::parse_cli(argc, argv);

    if (parsed.config.help_requested) {
        otcb::print_usage(std::cout, argc > 0 ? argv[0] : "opening-trainer-corpus-builder");
        return parsed.exit_code;
    }

    if (!parsed.ok) {
        for (const auto& error : parsed.errors) {
            std::cerr << "error: " << error << '\n';
        }
        std::cerr << '\n';
        otcb::print_usage(std::cerr, argc > 0 ? argv[0] : "opening-trainer-corpus-builder");
        return parsed.exit_code;
    }

    try {
        if (parsed.config.mode == otcb::BuildMode::DryRun) {
            const auto result = otcb::write_dry_run_bundle(parsed.config);
            std::cout << "Scaffold artifact bundle written to: " << result.bundle_root.lexically_normal().generic_string() << '\n';
            std::cout << "Artifact id: " << result.artifact_id << '\n';
            return 0;
        }

        const auto preflight = otcb::run_source_preflight(parsed.config);
        std::cout << otcb::render_preflight_summary(preflight);

        if (parsed.config.mode == otcb::BuildMode::Preflight) {
            if (!parsed.config.output_dir.empty()) {
                otcb::RangePlan range_plan;
                otcb::RangePlan* range_plan_ptr = nullptr;
                if (parsed.config.emit_range_plan) {
                    range_plan = otcb::make_range_plan(parsed.config, parsed.config.artifact_id.value_or(otcb::derive_artifact_id(parsed.config)), preflight);
                    range_plan_ptr = &range_plan;
                    std::cout << otcb::render_range_plan_text(range_plan);
                }
                const auto result = otcb::write_preflight_bundle(parsed.config, preflight, range_plan_ptr);
                std::cout << "Preflight artifact bundle written to: " << result.bundle_root.lexically_normal().generic_string() << '\n';
            }
            return 0;
        }

        const auto artifact_id = parsed.config.artifact_id.value_or(otcb::derive_artifact_id(parsed.config));
        const auto range_plan = otcb::make_range_plan(parsed.config, artifact_id, preflight);
        std::cout << otcb::render_range_plan_text(range_plan);

        if (parsed.config.mode == otcb::BuildMode::PlanRanges) {
            const auto result = otcb::write_plan_ranges_bundle(parsed.config, preflight, range_plan);
            std::cout << "Range plan artifact bundle written to: " << result.bundle_root.lexically_normal().generic_string() << '\n';
            std::cout << "Artifact id: " << result.artifact_id << '\n';
            return 0;
        }

        if (parsed.config.mode == otcb::BuildMode::ScanHeaders) {
            const auto scan_result = otcb::scan_headers(parsed.config, preflight, range_plan);
            std::cout << otcb::render_range_execution_summary_text(parsed.config, scan_result.summary);
            const auto result = otcb::write_scan_headers_bundle(parsed.config, preflight, range_plan, scan_result);
            std::cout << "Header scan artifact bundle written to: " << result.bundle_root.lexically_normal().generic_string() << '\n';
            std::cout << "Artifact id: " << result.artifact_id << '\n';
            return 0;
        }

        if (parsed.config.mode == otcb::BuildMode::ExtractOpenings) {
            const auto extraction_result = otcb::extract_openings(parsed.config, preflight, range_plan);
            std::cout << otcb::render_range_execution_summary_text(parsed.config, extraction_result.scan_result.summary);
            std::cout << otcb::render_extraction_summary_text(parsed.config, extraction_result.summary);
            const auto result = otcb::write_extract_openings_bundle(parsed.config, preflight, range_plan, extraction_result);
            std::cout << "Opening extraction artifact bundle written to: " << result.bundle_root.lexically_normal().generic_string() << '\n';
            std::cout << "Artifact id: " << result.artifact_id << '\n';
            return 0;
        }

        const auto aggregation_result = otcb::aggregate_counts(parsed.config, preflight, range_plan);
        std::cout << otcb::render_range_execution_summary_text(parsed.config, aggregation_result.extraction_result.scan_result.summary);
        std::cout << otcb::render_extraction_summary_text(parsed.config, aggregation_result.extraction_result.summary);
        std::cout << otcb::render_aggregation_summary_text(parsed.config, aggregation_result.summary);
        const auto result = otcb::write_aggregate_counts_bundle(parsed.config, preflight, range_plan, aggregation_result);
        std::cout << "Aggregate counts artifact bundle written to: " << result.bundle_root.lexically_normal().generic_string() << '\n';
        std::cout << "Artifact id: " << result.artifact_id << '\n';
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "error: " << ex.what() << '\n';
        return 1;
    }
}
