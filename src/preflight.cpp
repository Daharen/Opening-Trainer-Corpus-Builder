#include "otcb/preflight.hpp"

#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>

#include "otcb/progress.hpp"

namespace otcb {
namespace {

std::optional<std::string> timestamp_to_utc(const std::filesystem::file_time_type& file_time) {
    using namespace std::chrono;
    const auto system_now = system_clock::now();
    const auto file_now = std::filesystem::file_time_type::clock::now();
    const auto translated = time_point_cast<system_clock::duration>(file_time - file_now + system_now);
    const std::time_t timestamp = system_clock::to_time_t(translated);
    std::tm utc_tm{};
#if defined(_WIN32)
    if (gmtime_s(&utc_tm, &timestamp) != 0) {
        return std::nullopt;
    }
#else
    if (gmtime_r(&timestamp, &utc_tm) == nullptr) {
        return std::nullopt;
    }
#endif
    std::ostringstream output;
    output << std::put_time(&utc_tm, "%Y-%m-%dT%H:%M:%SZ");
    return output.str();
}

}  // namespace

SourcePreflightInfo run_source_preflight(const BuildConfig& config, ProgressReporter* progress) {
    if (progress) {
        progress->stage_started(ProgressStage::Preflight, "validating input path and source metadata");
    }

    if (config.input_pgn.empty()) {
        throw std::runtime_error("Preflight requires a non-empty input path.");
    }

    std::error_code error;
    const auto status = std::filesystem::status(config.input_pgn, error);
    if (error || !std::filesystem::exists(status)) {
        throw std::runtime_error("Input PGN does not exist: " + config.input_pgn.string());
    }
    if (std::filesystem::is_directory(status)) {
        throw std::runtime_error("Input PGN path is a directory, not a file: " + config.input_pgn.string());
    }

    const auto canonical = std::filesystem::weakly_canonical(config.input_pgn, error);
    if (error) {
        throw std::runtime_error("Failed to resolve canonical input path: " + config.input_pgn.string());
    }

    std::ifstream input(canonical, std::ios::binary);
    if (!input) {
        throw std::runtime_error("Input PGN is not readable: " + canonical.string());
    }

    const auto file_size = std::filesystem::file_size(canonical, error);
    if (error) {
        throw std::runtime_error("Failed to read file size for input PGN: " + canonical.string());
    }

    SourcePreflightInfo info;
    info.canonical_input_path = canonical;
    info.file_size_bytes = file_size;
    info.file_extension = canonical.extension().generic_string();
    info.readable = true;
    info.input_format = config.input_format;
    info.inferred_large_file = file_size >= config.target_range_bytes;
    const auto write_time = std::filesystem::last_write_time(canonical, error);
    if (!error) {
        info.timestamp_utc = timestamp_to_utc(write_time);
    }
    if (progress) {
        progress->update([&](ProgressSnapshot& snapshot) {
            snapshot.source_file_size = info.file_size_bytes;
            snapshot.source_bytes_scanned = info.file_size_bytes;
            snapshot.bytes_covered = info.file_size_bytes;
            snapshot.percent_complete = 100.0;
        });
        progress->stage_completed("preflight metadata captured");
    }
    return info;
}

std::string render_preflight_summary(const SourcePreflightInfo& info) {
    std::ostringstream output;
    output << "Preflight summary\n";
    output << "  canonical_input_path: " << info.canonical_input_path.generic_string() << "\n";
    output << "  file_size_bytes: " << info.file_size_bytes << "\n";
    output << "  file_extension: " << info.file_extension << "\n";
    output << "  readable: " << (info.readable ? "yes" : "no") << "\n";
    output << "  input_format: " << info.input_format << "\n";
    output << "  inferred_large_file: " << (info.inferred_large_file ? "yes" : "no") << "\n";
    output << "  source_timestamp_utc: " << info.timestamp_utc.value_or("unavailable") << "\n";
    return output.str();
}

}  // namespace otcb
