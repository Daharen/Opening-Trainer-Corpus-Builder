#include "otcb/progress.hpp"

#include <chrono>
#include <ctime>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <thread>

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

std::string format_wall_clock(const std::chrono::system_clock::time_point& point) {
    const std::time_t t = std::chrono::system_clock::to_time_t(point);
    std::tm utc_tm{};
#if defined(_WIN32)
    gmtime_s(&utc_tm, &t);
#else
    gmtime_r(&t, &utc_tm);
#endif
    std::ostringstream out;
    out << std::put_time(&utc_tm, "%Y-%m-%dT%H:%M:%SZ");
    return out.str();
}

std::string format_duration(const std::chrono::seconds duration) {
    const auto total = duration.count();
    const auto hours = total / 3600;
    const auto minutes = (total % 3600) / 60;
    const auto seconds = total % 60;
    std::ostringstream out;
    out << std::setfill('0') << std::setw(2) << hours << ':' << std::setw(2) << minutes << ':' << std::setw(2) << seconds;
    return out.str();
}

void append_metric(std::ostringstream& out, const std::string& key, const std::string& value) {
    if (!value.empty()) {
        out << ' ' << key << '=' << value;
    }
}

}  // namespace

std::string to_string(const ProgressStage stage) {
    switch (stage) {
        case ProgressStage::Idle: return "idle";
        case ProgressStage::Preflight: return "preflight";
        case ProgressStage::PlanRanges: return "plan-ranges";
        case ProgressStage::ScanHeaders: return "scan-headers";
        case ProgressStage::ExtractOpenings: return "extract-openings";
        case ProgressStage::AggregateCounts: return "aggregate-counts";
        case ProgressStage::WriteArtifacts: return "write-artifacts";
        case ProgressStage::Finalize: return "finalize";
    }
    return "unknown";
}

ProgressReporter::ProgressReporter(ProgressReporterOptions options, std::ostream& stream)
    : options_(std::move(options)),
      stream_(stream),
      run_started_system_(std::chrono::system_clock::now()),
      run_started_steady_(std::chrono::steady_clock::now()) {
    snapshot_.run_started_at = run_started_system_;
    snapshot_.snapshot_time = run_started_system_;
    snapshot_.stage_started_at = run_started_steady_;
}

ProgressReporter::~ProgressReporter() {
    finish();
}

void ProgressReporter::start() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (running_) {
        return;
    }
    running_ = true;
    stop_requested_ = false;
    heartbeat_thread_ = std::thread([this]() { heartbeat_loop(); });
}

void ProgressReporter::finish() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_) {
            return;
        }
        stop_requested_ = true;
    }
    if (heartbeat_thread_.joinable()) {
        heartbeat_thread_.join();
    }
    std::lock_guard<std::mutex> lock(mutex_);
    running_ = false;
}

void ProgressReporter::stage_started(ProgressStage stage, std::string message, std::optional<std::uint64_t> source_file_size) {
    std::lock_guard<std::mutex> lock(mutex_);
    snapshot_.stage = stage;
    snapshot_.stage_active = true;
    snapshot_.stage_failed = false;
    snapshot_.stage_started_at = std::chrono::steady_clock::now();
    snapshot_.snapshot_time = std::chrono::system_clock::now();
    snapshot_.last_event_message = std::move(message);
    if (source_file_size.has_value()) {
        snapshot_.source_file_size = source_file_size;
    }
    emit_locked("stage-start");
}

void ProgressReporter::stage_completed(std::string message) {
    std::lock_guard<std::mutex> lock(mutex_);
    snapshot_.snapshot_time = std::chrono::system_clock::now();
    snapshot_.stage_active = false;
    snapshot_.stage_failed = false;
    snapshot_.last_event_message = std::move(message);
    emit_locked("stage-complete");
}

void ProgressReporter::stage_failed(std::string message) {
    std::lock_guard<std::mutex> lock(mutex_);
    snapshot_.snapshot_time = std::chrono::system_clock::now();
    snapshot_.stage_active = false;
    snapshot_.stage_failed = true;
    snapshot_.last_event_message = std::move(message);
    emit_locked("stage-failed");
}

void ProgressReporter::note_event(std::string message) {
    std::lock_guard<std::mutex> lock(mutex_);
    snapshot_.snapshot_time = std::chrono::system_clock::now();
    snapshot_.last_event_message = std::move(message);
}

void ProgressReporter::update(const std::function<void(ProgressSnapshot&)>& updater) {
    std::lock_guard<std::mutex> lock(mutex_);
    updater(snapshot_);
    snapshot_.snapshot_time = std::chrono::system_clock::now();
}

ProgressSnapshot ProgressReporter::snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return snapshot_;
}

void ProgressReporter::heartbeat_loop() {
    using namespace std::chrono_literals;
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(options_.heartbeat_seconds));
        std::lock_guard<std::mutex> lock(mutex_);
        if (stop_requested_) {
            return;
        }
        if (snapshot_.stage_active) {
            snapshot_.snapshot_time = std::chrono::system_clock::now();
            emit_locked("heartbeat");
        }
    }
}

void ProgressReporter::ensure_progress_dir_locked() {
    if (options_.artifact_bundle_root.empty()) {
        return;
    }
    if (!progress_dir_.has_value()) {
        progress_dir_ = options_.artifact_bundle_root / "progress";
        std::filesystem::create_directories(*progress_dir_);
    }
}

std::string ProgressReporter::format_console_line_locked(const char* kind) const {
    const auto now_steady = std::chrono::steady_clock::now();
    const auto run_elapsed = std::chrono::duration_cast<std::chrono::seconds>(now_steady - run_started_steady_);
    const auto stage_elapsed = std::chrono::duration_cast<std::chrono::seconds>(now_steady - snapshot_.stage_started_at);

    std::ostringstream out;
    out << '[' << format_wall_clock(snapshot_.snapshot_time) << "] [" << to_string(snapshot_.stage) << "]";
    out << ' ' << kind;
    append_metric(out, "elapsed", format_duration(run_elapsed));
    append_metric(out, "stage_elapsed", format_duration(stage_elapsed));
    if (snapshot_.current_range_index.has_value()) {
        append_metric(out, "current_range", std::to_string(*snapshot_.current_range_index));
    }
    if (snapshot_.ranges_planned > 0 || snapshot_.ranges_completed > 0) {
        append_metric(out, "ranges", std::to_string(snapshot_.ranges_completed) + "/" + std::to_string(snapshot_.ranges_planned));
    }
    if (snapshot_.nominal_starts_processed > 0) {
        append_metric(out, "nominal_starts_processed", std::to_string(snapshot_.nominal_starts_processed));
    }
    if (snapshot_.safe_boundaries_found > 0) {
        append_metric(out, "safe_boundaries_found", std::to_string(snapshot_.safe_boundaries_found));
    }
    if (snapshot_.source_bytes_scanned > 0) {
        append_metric(out, "bytes_scanned", std::to_string(snapshot_.source_bytes_scanned));
    }
    if (snapshot_.bytes_covered > 0) {
        append_metric(out, "bytes_covered", std::to_string(snapshot_.bytes_covered));
    }
    if (snapshot_.source_file_size.has_value()) {
        append_metric(out, "source_size", std::to_string(*snapshot_.source_file_size));
    }
    if (snapshot_.percent_complete.has_value()) {
        std::ostringstream pct;
        pct << std::fixed << std::setprecision(1) << *snapshot_.percent_complete;
        append_metric(out, "percent", pct.str());
    }
    if (snapshot_.games_scanned > 0) append_metric(out, "games_scanned", std::to_string(snapshot_.games_scanned));
    if (snapshot_.games_accepted > 0) append_metric(out, "games_accepted", std::to_string(snapshot_.games_accepted));
    if (snapshot_.games_rejected > 0) append_metric(out, "games_rejected", std::to_string(snapshot_.games_rejected));
    if (snapshot_.replay_attempts > 0) append_metric(out, "replay_attempts", std::to_string(snapshot_.replay_attempts));
    if (snapshot_.replay_successes > 0) append_metric(out, "replay_successes", std::to_string(snapshot_.replay_successes));
    if (snapshot_.replay_failures > 0) append_metric(out, "replay_failures", std::to_string(snapshot_.replay_failures));
    if (snapshot_.extracted_plies > 0) append_metric(out, "extracted_plies", std::to_string(snapshot_.extracted_plies));
    if (snapshot_.aggregated_positions > 0) append_metric(out, "positions", std::to_string(snapshot_.aggregated_positions));
    if (snapshot_.raw_observations > 0) append_metric(out, "observations", std::to_string(snapshot_.raw_observations));
    if (snapshot_.aggregate_move_entries > 0) append_metric(out, "aggregate_move_entries", std::to_string(snapshot_.aggregate_move_entries));
    if (snapshot_.max_games.has_value()) append_metric(out, "max_games", std::to_string(*snapshot_.max_games));
    if (snapshot_.throughput_per_second.has_value()) {
        std::ostringstream t;
        t << std::fixed << std::setprecision(2) << *snapshot_.throughput_per_second;
        append_metric(out, "throughput_per_sec", t.str());
    }
    if (snapshot_.eta.has_value()) {
        append_metric(out, "eta", format_duration(*snapshot_.eta));
    }
    append_metric(out, "status", snapshot_.last_event_message.empty() ? std::string("still-active") : snapshot_.last_event_message);
    return out.str();
}

std::string ProgressReporter::format_status_json_locked() const {
    const auto now_steady = std::chrono::steady_clock::now();
    const auto run_elapsed = std::chrono::duration_cast<std::chrono::seconds>(now_steady - run_started_steady_).count();
    const auto stage_elapsed = std::chrono::duration_cast<std::chrono::seconds>(now_steady - snapshot_.stage_started_at).count();
    std::ostringstream out;
    out << "{\n";
    out << "  \"timestamp_utc\": \"" << json_escape(format_wall_clock(snapshot_.snapshot_time)) << "\",\n";
    out << "  \"stage\": \"" << json_escape(to_string(snapshot_.stage)) << "\",\n";
    out << "  \"stage_active\": " << (snapshot_.stage_active ? "true" : "false") << ",\n";
    out << "  \"stage_failed\": " << (snapshot_.stage_failed ? "true" : "false") << ",\n";
    out << "  \"elapsed_seconds\": " << run_elapsed << ",\n";
    out << "  \"stage_elapsed_seconds\": " << stage_elapsed << ",\n";
    out << "  \"source_bytes_scanned\": " << snapshot_.source_bytes_scanned << ",\n";
    out << "  \"source_file_size\": " << (snapshot_.source_file_size ? std::to_string(*snapshot_.source_file_size) : "null") << ",\n";
    out << "  \"percent_complete\": " << (snapshot_.percent_complete ? std::to_string(*snapshot_.percent_complete) : "null") << ",\n";
    out << "  \"ranges_planned\": " << snapshot_.ranges_planned << ",\n";
    out << "  \"ranges_completed\": " << snapshot_.ranges_completed << ",\n";
    out << "  \"current_range_index\": " << (snapshot_.current_range_index ? std::to_string(*snapshot_.current_range_index) : "null") << ",\n";
    out << "  \"games_scanned\": " << snapshot_.games_scanned << ",\n";
    out << "  \"games_accepted\": " << snapshot_.games_accepted << ",\n";
    out << "  \"games_rejected\": " << snapshot_.games_rejected << ",\n";
    out << "  \"replay_attempts\": " << snapshot_.replay_attempts << ",\n";
    out << "  \"replay_successes\": " << snapshot_.replay_successes << ",\n";
    out << "  \"replay_failures\": " << snapshot_.replay_failures << ",\n";
    out << "  \"extracted_plies\": " << snapshot_.extracted_plies << ",\n";
    out << "  \"aggregated_positions\": " << snapshot_.aggregated_positions << ",\n";
    out << "  \"raw_observations\": " << snapshot_.raw_observations << ",\n";
    out << "  \"aggregate_move_entries\": " << snapshot_.aggregate_move_entries << ",\n";
    out << "  \"throughput_per_second\": " << (snapshot_.throughput_per_second ? std::to_string(*snapshot_.throughput_per_second) : "null") << ",\n";
    out << "  \"eta_seconds\": " << (snapshot_.eta ? std::to_string(snapshot_.eta->count()) : "null") << ",\n";
    out << "  \"last_event_message\": \"" << json_escape(snapshot_.last_event_message) << "\"\n";
    out << "}\n";
    return out.str();
}

void ProgressReporter::emit_locked(const char* kind) {
    const std::string line = format_console_line_locked(kind);
    if (!options_.quiet) {
        stream_ << line << '\n';
        stream_.flush();
    }
    ensure_progress_dir_locked();
    if (progress_dir_.has_value() && options_.emit_progress_log) {
        std::ofstream log(*progress_dir_ / "progress.log", std::ios::app | std::ios::binary);
        log << line << '\n';
        log.flush();
    }
    if (progress_dir_.has_value() && options_.emit_status_json) {
        std::ofstream status(*progress_dir_ / "latest_status.json", std::ios::binary);
        status << format_status_json_locked();
        status.flush();
    }
}

}  // namespace otcb
