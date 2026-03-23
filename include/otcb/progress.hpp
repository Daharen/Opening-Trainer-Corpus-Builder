#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <iostream>
#include <mutex>
#include <optional>
#include <ostream>
#include <string>
#include <thread>

namespace otcb {

enum class ProgressStage {
    Idle,
    Preflight,
    PlanRanges,
    ScanHeaders,
    ExtractOpenings,
    AggregateCounts,
    WriteArtifacts,
    Finalize,
};

std::string to_string(ProgressStage stage);

struct ProgressSnapshot {
    ProgressStage stage = ProgressStage::Idle;
    bool stage_active = false;
    bool stage_failed = false;
    std::chrono::system_clock::time_point run_started_at{};
    std::chrono::steady_clock::time_point stage_started_at{};
    std::chrono::system_clock::time_point snapshot_time{};
    std::uint64_t source_bytes_scanned = 0;
    std::optional<std::uint64_t> source_file_size;
    std::optional<double> percent_complete;
    int ranges_planned = 0;
    int ranges_completed = 0;
    std::optional<int> current_range_index;
    std::uint64_t nominal_starts_processed = 0;
    std::uint64_t safe_boundaries_found = 0;
    std::uint64_t bytes_covered = 0;
    int games_scanned = 0;
    int games_accepted = 0;
    int games_rejected = 0;
    int replay_attempts = 0;
    int replay_successes = 0;
    int replay_failures = 0;
    int extracted_plies = 0;
    int aggregated_positions = 0;
    int raw_observations = 0;
    int aggregate_move_entries = 0;
    std::optional<int> max_games;
    std::optional<double> throughput_per_second;
    std::optional<std::chrono::seconds> eta;
    std::string last_event_message;
};

struct ProgressReporterOptions {
    bool quiet = false;
    bool emit_progress_log = false;
    bool emit_status_json = false;
    int heartbeat_seconds = 30;
    std::filesystem::path artifact_bundle_root;
};

class ProgressReporter {
public:
    explicit ProgressReporter(ProgressReporterOptions options, std::ostream& stream = std::cout);
    ~ProgressReporter();

    ProgressReporter(const ProgressReporter&) = delete;
    ProgressReporter& operator=(const ProgressReporter&) = delete;

    void start();
    void finish();

    void stage_started(ProgressStage stage, std::string message, std::optional<std::uint64_t> source_file_size = std::nullopt);
    void stage_completed(std::string message);
    void stage_failed(std::string message);
    void note_event(std::string message);
    void update(const std::function<void(ProgressSnapshot&)>& updater);
    ProgressSnapshot snapshot() const;

private:
    void heartbeat_loop();
    void emit_locked(const char* kind);
    void ensure_progress_dir_locked();
    std::string format_console_line_locked(const char* kind) const;
    std::string format_status_json_locked() const;

    ProgressReporterOptions options_;
    std::ostream& stream_;
    std::chrono::system_clock::time_point run_started_system_;
    std::chrono::steady_clock::time_point run_started_steady_;
    mutable std::mutex mutex_;
    ProgressSnapshot snapshot_;
    std::optional<std::filesystem::path> progress_dir_;
    bool running_ = false;
    bool stop_requested_ = false;
    std::thread heartbeat_thread_;
};

}  // namespace otcb
