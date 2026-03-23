#include "otcb/range_plan.hpp"

#include <algorithm>
#include <sstream>

#include "otcb/source_boundaries.hpp"

namespace otcb {
namespace {

std::string json_escape(const std::string& input) {
    std::string escaped;
    escaped.reserve(input.size());
    for (const char ch : input) {
        switch (ch) {
            case '\\': escaped += "\\\\"; break;
            case '"': escaped += "\\\""; break;
            case '\n': escaped += "\\n"; break;
            case '\r': escaped += "\\r"; break;
            case '\t': escaped += "\\t"; break;
            default: escaped += ch; break;
        }
    }
    return escaped;
}

}  // namespace

RangePlan make_range_plan(const BuildConfig& config, const std::string& artifact_id, const SourcePreflightInfo& source_info) {
    RangePlan plan;
    plan.artifact_id = artifact_id;
    plan.source_info = source_info;
    plan.target_range_bytes = config.target_range_bytes;
    plan.boundary_scan_bytes = config.boundary_scan_bytes;
    plan.max_ranges = config.max_ranges;
    plan.planner_algorithm = "nominal_fixed_span_with_forward_event_header_alignment_v1";
    plan.plan_notes.push_back("Deterministic planning depends on the same input bytes and planner configuration.");
    plan.plan_notes.push_back("Nonzero nominal starts are scanned forward for a plausible [Event header before acceptance.");

    if (source_info.file_size_bytes == 0) {
        plan.plan_notes.push_back("Source file is empty; no ranges emitted.");
        return plan;
    }

    std::uint64_t nominal_start = 0;
    int range_index = 0;
    while (nominal_start < source_info.file_size_bytes) {
        if (config.max_ranges > 0 && range_index >= config.max_ranges) {
            plan.plan_notes.push_back("Stopped planning after reaching explicit max-ranges limit.");
            break;
        }

        const std::uint64_t nominal_end_exclusive = std::min(source_info.file_size_bytes, nominal_start + config.target_range_bytes);
        const auto boundary = resolve_range_start_boundary(
            source_info.canonical_input_path,
            nominal_start,
            config.boundary_scan_bytes);

        if (nominal_start != 0 && !boundary.found_boundary) {
            std::ostringstream message;
            message << "Conservative planning stopped at range " << range_index
                    << " because boundary reconciliation failed near byte " << nominal_start << ".";
            plan.plan_notes.push_back(message.str());
            break;
        }

        PlannedRange range;
        range.range_index = range_index;
        range.nominal_start_byte = nominal_start;
        range.adjusted_start_byte = boundary.adjusted_start_byte;
        range.nominal_end_byte = nominal_end_exclusive == 0 ? 0 : nominal_end_exclusive - 1;
        range.start_adjustment_reason = boundary.reason;
        range.boundary_confidence = boundary.confidence;
        range.adjusted_end_byte = nominal_end_exclusive == 0 ? std::optional<std::uint64_t>{} : std::optional<std::uint64_t>{nominal_end_exclusive - 1};
        range.estimated_span_bytes = (nominal_end_exclusive > range.adjusted_start_byte)
            ? (nominal_end_exclusive - range.adjusted_start_byte)
            : 0;
        plan.ranges.push_back(range);

        if (nominal_end_exclusive >= source_info.file_size_bytes) {
            break;
        }

        nominal_start = nominal_end_exclusive;
        ++range_index;
    }

    return plan;
}

std::string render_range_plan_json(const RangePlan& plan) {
    std::ostringstream output;
    output << "{\n";
    output << "  \"artifact_id\": \"" << json_escape(plan.artifact_id) << "\",\n";
    output << "  \"source_path\": \"" << json_escape(plan.source_info.canonical_input_path.generic_string()) << "\",\n";
    output << "  \"source_size_bytes\": " << plan.source_info.file_size_bytes << ",\n";
    output << "  \"target_range_bytes\": " << plan.target_range_bytes << ",\n";
    output << "  \"boundary_scan_bytes\": " << plan.boundary_scan_bytes << ",\n";
    output << "  \"max_ranges\": " << plan.max_ranges << ",\n";
    output << "  \"planner_algorithm\": \"" << json_escape(plan.planner_algorithm) << "\",\n";
    output << "  \"deterministic\": " << (plan.deterministic ? "true" : "false") << ",\n";
    output << "  \"total_planned_ranges\": " << plan.ranges.size() << ",\n";
    output << "  \"ranges\": [\n";
    for (std::size_t index = 0; index < plan.ranges.size(); ++index) {
        const auto& range = plan.ranges[index];
        output << "    {\n";
        output << "      \"range_index\": " << range.range_index << ",\n";
        output << "      \"nominal_start_byte\": " << range.nominal_start_byte << ",\n";
        output << "      \"adjusted_start_byte\": " << range.adjusted_start_byte << ",\n";
        output << "      \"nominal_end_byte\": " << range.nominal_end_byte << ",\n";
        output << "      \"adjusted_end_byte\": " << range.adjusted_end_byte.value_or(range.nominal_end_byte) << ",\n";
        output << "      \"boundary_reason\": \"" << json_escape(range.start_adjustment_reason) << "\",\n";
        output << "      \"boundary_confidence\": \"" << json_escape(range.boundary_confidence) << "\",\n";
        output << "      \"estimated_span_bytes\": " << range.estimated_span_bytes << "\n";
        output << "    }" << (index + 1 < plan.ranges.size() ? "," : "") << "\n";
    }
    output << "  ],\n";
    output << "  \"plan_notes\": [\n";
    for (std::size_t index = 0; index < plan.plan_notes.size(); ++index) {
        output << "    \"" << json_escape(plan.plan_notes[index]) << "\"";
        output << (index + 1 < plan.plan_notes.size() ? "," : "") << "\n";
    }
    output << "  ]\n";
    output << "}\n";
    return output.str();
}

std::string render_range_plan_text(const RangePlan& plan) {
    std::ostringstream output;
    output << "Range planning summary\n";
    output << "source path: " << plan.source_info.canonical_input_path.generic_string() << "\n";
    output << "source size bytes: " << plan.source_info.file_size_bytes << "\n";
    output << "target range bytes: " << plan.target_range_bytes << "\n";
    output << "boundary scan bytes: " << plan.boundary_scan_bytes << "\n";
    output << "max ranges: " << plan.max_ranges << "\n";
    output << "planner algorithm: " << plan.planner_algorithm << "\n";
    output << "deterministic: " << (plan.deterministic ? "yes" : "no") << "\n";
    output << "emitted ranges: " << plan.ranges.size() << "\n";
    output << "first ranges:\n";
    const std::size_t preview_count = std::min<std::size_t>(5, plan.ranges.size());
    for (std::size_t index = 0; index < preview_count; ++index) {
        const auto& range = plan.ranges[index];
        output << "- range " << range.range_index
               << ": nominal_start=" << range.nominal_start_byte
               << ", adjusted_start=" << range.adjusted_start_byte
               << ", nominal_end=" << range.nominal_end_byte
               << ", estimated_span=" << range.estimated_span_bytes
               << ", confidence=" << range.boundary_confidence
               << ", reason=" << range.start_adjustment_reason << "\n";
    }
    if (!plan.plan_notes.empty()) {
        output << "notes:\n";
        for (const auto& note : plan.plan_notes) {
            output << "- " << note << "\n";
        }
    }
    return output.str();
}

}  // namespace otcb
