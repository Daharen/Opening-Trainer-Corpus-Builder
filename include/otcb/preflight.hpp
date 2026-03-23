#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>

#include "otcb/config.hpp"

namespace otcb {

struct SourcePreflightInfo {
    std::filesystem::path canonical_input_path;
    std::uint64_t file_size_bytes = 0;
    std::string file_extension;
    bool readable = false;
    std::string input_format;
    bool inferred_large_file = false;
    std::optional<std::string> timestamp_utc;
};

class ProgressReporter;
SourcePreflightInfo run_source_preflight(const BuildConfig& config, ProgressReporter* progress = nullptr);
std::string render_preflight_summary(const SourcePreflightInfo& info);

}  // namespace otcb
