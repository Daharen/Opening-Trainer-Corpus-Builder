#include "otcb/source_boundaries.hpp"

#include <algorithm>
#include <fstream>
#include <stdexcept>
#include <string>

namespace otcb {
namespace {

bool starts_with(const std::string& text, const std::string& prefix) {
    return text.rfind(prefix, 0) == 0;
}

}  // namespace

BoundaryResolution resolve_range_start_boundary(
    const std::filesystem::path& source_path,
    const std::uint64_t nominal_start_byte,
    const std::uint64_t boundary_scan_bytes) {
    BoundaryResolution resolution;
    resolution.nominal_start_byte = nominal_start_byte;

    if (nominal_start_byte == 0) {
        resolution.adjusted_start_byte = 0;
        resolution.reason = "accepted file start";
        resolution.confidence = "exact_file_start";
        resolution.found_boundary = true;
        return resolution;
    }

    std::ifstream input(source_path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("Failed to open source file for boundary scan: " + source_path.string());
    }

    input.seekg(0, std::ios::end);
    const auto end_pos = static_cast<std::uint64_t>(input.tellg());
    if (nominal_start_byte >= end_pos) {
        resolution.adjusted_start_byte = end_pos;
        resolution.reason = "nominal start reached end of file";
        resolution.confidence = "end_of_file";
        resolution.found_boundary = true;
        return resolution;
    }

    input.seekg(static_cast<std::streamoff>(nominal_start_byte), std::ios::beg);
    std::string line;
    std::uint64_t line_start = nominal_start_byte;
    if (nominal_start_byte > 0) {
        std::getline(input, line);
        line_start = static_cast<std::uint64_t>(input.tellg());
        if (!input) {
            line_start = end_pos;
        }
    }

    while (input && line_start < end_pos && line_start <= nominal_start_byte + boundary_scan_bytes) {
        const std::streampos record_pos = input.tellg();
        if (!std::getline(input, line)) {
            break;
        }

        const std::string normalized = (!line.empty() && line.back() == '\r') ? line.substr(0, line.size() - 1) : line;
        if (starts_with(normalized, "[Event ")) {
            std::string next_line;
            std::streampos next_pos = input.tellg();
            std::getline(input, next_line);
            const std::string next_normalized = (!next_line.empty() && next_line.back() == '\r') ? next_line.substr(0, next_line.size() - 1) : next_line;
            input.clear();
            input.seekg(next_pos);

            resolution.adjusted_start_byte = line_start;
            resolution.found_boundary = true;
            if (starts_with(next_normalized, "[Site ") || starts_with(next_normalized, "[Date ")) {
                resolution.reason = "advanced to aligned [Event header with supporting metadata header";
                resolution.confidence = "high";
            } else {
                resolution.reason = "advanced to aligned [Event header within boundary scan window";
                resolution.confidence = "medium";
            }
            return resolution;
        }

        line_start = record_pos == std::streampos(-1)
            ? end_pos
            : static_cast<std::uint64_t>(input.tellg());
        if (!input) {
            line_start = end_pos;
        }
    }

    resolution.adjusted_start_byte = nominal_start_byte;
    resolution.reason = "no plausible [Event header found within boundary scan window";
    resolution.confidence = "conservative_failure";
    resolution.found_boundary = false;
    return resolution;
}

}  // namespace otcb
