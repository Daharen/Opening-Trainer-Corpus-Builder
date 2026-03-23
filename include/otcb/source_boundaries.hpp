#pragma once

#include <cstdint>
#include <filesystem>
#include <string>

namespace otcb {

struct BoundaryResolution {
    std::uint64_t nominal_start_byte = 0;
    std::uint64_t adjusted_start_byte = 0;
    std::string reason;
    std::string confidence;
    bool found_boundary = false;
};

BoundaryResolution resolve_range_start_boundary(
    const std::filesystem::path& source_path,
    std::uint64_t nominal_start_byte,
    std::uint64_t boundary_scan_bytes);

}  // namespace otcb
