#include "otcb/cli.hpp"

#include <iostream>
#include <sstream>
#include <stdexcept>
#include <vector>

namespace otcb {
namespace {

int parse_int_argument(const std::string& option, const std::string& value) {
    try {
        std::size_t index = 0;
        const int parsed = std::stoi(value, &index);
        if (index != value.size()) {
            throw std::invalid_argument("trailing characters");
        }
        return parsed;
    } catch (const std::exception&) {
        throw std::runtime_error("Invalid integer for " + option + ": " + value);
    }
}

std::uint64_t parse_uint64_argument(const std::string& option, const std::string& value) {
    try {
        std::size_t index = 0;
        const auto parsed = std::stoull(value, &index);
        if (index != value.size()) {
            throw std::invalid_argument("trailing characters");
        }
        return parsed;
    } catch (const std::exception&) {
        throw std::runtime_error("Invalid unsigned integer for " + option + ": " + value);
    }
}

std::string require_value(int argc, char** argv, int& index, const std::string& option) {
    if (index + 1 >= argc) {
        throw std::runtime_error("Missing value for " + option);
    }
    ++index;
    return argv[index];
}

std::vector<std::string> parse_csv_list(const std::string& value) {
    std::vector<std::string> items;
    std::string current;
    for (const char ch : value) {
        if (ch == ',') {
            if (!current.empty()) {
                items.push_back(current);
                current.clear();
            }
            continue;
        }
        if (ch != ' ' && ch != '\t' && ch != '\n' && ch != '\r') {
            current.push_back(ch);
        }
    }
    if (!current.empty()) {
        items.push_back(current);
    }
    return items;
}

}  // namespace

CliParseResult parse_cli(int argc, char** argv) {
    CliParseResult result;

    if (argc <= 1) {
        result.ok = true;
        result.should_exit = true;
        result.exit_code = 0;
        result.config.help_requested = true;
        return result;
    }

    try {
        for (int index = 1; index < argc; ++index) {
            const std::string arg = argv[index];
            if (arg == "--help") {
                result.config.help_requested = true;
                result.ok = true;
                result.should_exit = true;
                result.exit_code = 0;
                return result;
            }
            if (arg == "--dry-run") {
                result.config.dry_run = true;
                result.config.mode = BuildMode::DryRun;
                continue;
            }
            if (arg == "--mode") {
                const auto parsed = parse_build_mode(require_value(argc, argv, index, arg));
                if (!parsed.has_value()) {
                    throw std::runtime_error("Invalid value for --mode. Supported: dry-run, preflight, plan-ranges, scan-headers, extract-openings, aggregate-counts.");
                }
                result.config.mode = *parsed;
                result.config.dry_run = result.config.mode == BuildMode::DryRun;
                continue;
            }
            if (arg == "--input-pgn") { result.config.input_pgn = require_value(argc, argv, index, arg); continue; }
            if (arg == "--output-dir") { result.config.output_dir = require_value(argc, argv, index, arg); continue; }
            if (arg == "--min-rating") { result.config.min_rating = parse_int_argument(arg, require_value(argc, argv, index, arg)); continue; }
            if (arg == "--max-rating") { result.config.max_rating = parse_int_argument(arg, require_value(argc, argv, index, arg)); continue; }
            if (arg == "--rating-policy") {
                const auto parsed = parse_rating_policy(require_value(argc, argv, index, arg));
                if (!parsed.has_value()) throw std::runtime_error("Invalid value for --rating-policy. See --help for supported values.");
                result.config.rating_policy = parsed;
                continue;
            }
            if (arg == "--retained-ply") { result.config.retained_ply = parse_int_argument(arg, require_value(argc, argv, index, arg)); continue; }
            if (arg == "--threads") { result.config.threads = parse_int_argument(arg, require_value(argc, argv, index, arg)); continue; }
            if (arg == "--max-games") { result.config.max_games = parse_int_argument(arg, require_value(argc, argv, index, arg)); continue; }
            if (arg == "--artifact-id") { result.config.artifact_id = require_value(argc, argv, index, arg); continue; }
            if (arg == "--progress-interval") { result.config.progress_interval = parse_int_argument(arg, require_value(argc, argv, index, arg)); continue; }
            if (arg == "--heartbeat-seconds") { result.config.heartbeat_seconds = parse_int_argument(arg, require_value(argc, argv, index, arg)); continue; }
            if (arg == "--emit-progress-log") { result.config.emit_progress_log = true; continue; }
            if (arg == "--emit-status-json") { result.config.emit_status_json = true; continue; }
            if (arg == "--quiet-progress") { result.config.quiet_progress = true; continue; }
            if (arg == "--target-range-bytes") { result.config.target_range_bytes = parse_uint64_argument(arg, require_value(argc, argv, index, arg)); continue; }
            if (arg == "--boundary-scan-bytes") { result.config.boundary_scan_bytes = parse_uint64_argument(arg, require_value(argc, argv, index, arg)); continue; }
            if (arg == "--max-ranges") { result.config.max_ranges = parse_int_argument(arg, require_value(argc, argv, index, arg)); continue; }
            if (arg == "--emit-range-plan") { result.config.emit_range_plan = true; continue; }
            if (arg == "--header-preview-limit") { result.config.header_preview_limit = parse_int_argument(arg, require_value(argc, argv, index, arg)); continue; }
            if (arg == "--emit-header-preview") { result.config.emit_header_preview = true; continue; }
            if (arg == "--strict-header-scan") { result.config.strict_header_scan = true; continue; }
            if (arg == "--extraction-preview-limit") { result.config.extraction_preview_limit = parse_int_argument(arg, require_value(argc, argv, index, arg)); continue; }
            if (arg == "--emit-extraction-preview") { result.config.emit_extraction_preview = true; continue; }
            if (arg == "--strict-san-replay") { result.config.strict_san_replay = true; continue; }
            if (arg == "--include-fen-snapshots") { result.config.include_fen_snapshots = true; continue; }
            if (arg == "--include-uci-moves") { result.config.include_uci_moves = true; continue; }
            if (arg == "--emit-aggregate-preview") { result.config.emit_aggregate_preview = true; continue; }
            if (arg == "--aggregate-preview-limit") { result.config.aggregate_preview_limit = parse_int_argument(arg, require_value(argc, argv, index, arg)); continue; }
            if (arg == "--position-key-format") {
                const auto parsed = parse_position_key_format(require_value(argc, argv, index, arg));
                if (!parsed.has_value()) throw std::runtime_error("Invalid value for --position-key-format. Supported: fen_normalized, fen_full.");
                result.config.position_key_format = parsed;
                continue;
            }
            if (arg == "--move-key-format") {
                const auto parsed = parse_move_key_format(require_value(argc, argv, index, arg));
                if (!parsed.has_value()) throw std::runtime_error("Invalid value for --move-key-format. Supported: uci.");
                result.config.move_key_format = parsed;
                continue;
            }
            if (arg == "--min-position-count") { result.config.min_position_count = parse_int_argument(arg, require_value(argc, argv, index, arg)); continue; }
            if (arg == "--payload-format") {
                const auto parsed = parse_payload_format(require_value(argc, argv, index, arg));
                if (!parsed.has_value()) throw std::runtime_error("Invalid value for --payload-format. Supported: jsonl, sqlite, exact_sqlite_v2_compact.");
                result.config.payload_format = *parsed;
                continue;
            }
            if (arg == "--no-legacy-sqlite-mirror") { result.config.emit_legacy_sqlite_mirror = false; continue; }
            if (arg == "--emit-canonical-predecessors") { result.config.emit_canonical_predecessors = true; continue; }
            if (arg == "--no-canonical-predecessors") { result.config.emit_canonical_predecessors = false; continue; }
            if (arg == "--time-controls") { result.config.time_controls = parse_csv_list(require_value(argc, argv, index, arg)); continue; }
            if (arg == "--time-control-id") { result.config.time_control_id = require_value(argc, argv, index, arg); continue; }
            if (arg == "--initial-time-seconds") { result.config.initial_time_seconds = parse_int_argument(arg, require_value(argc, argv, index, arg)); continue; }
            if (arg == "--increment-seconds") { result.config.increment_seconds = parse_int_argument(arg, require_value(argc, argv, index, arg)); continue; }
            if (arg == "--time-format-label") { result.config.time_format_label = require_value(argc, argv, index, arg); continue; }
            if (arg == "--input-format") { result.config.input_format = require_value(argc, argv, index, arg); continue; }
            throw std::runtime_error("Unknown argument: " + arg);
        }
    } catch (const std::exception& ex) {
        result.errors.emplace_back(ex.what());
        result.should_exit = true;
        result.exit_code = 1;
        return result;
    }

    result.errors = validate_config(result.config);
    result.ok = result.errors.empty();
    result.should_exit = !result.ok;
    result.exit_code = result.ok ? 0 : 1;
    return result;
}

void print_usage(std::ostream& stream, const std::string& program_name) {
    stream
        << "Usage: " << program_name << " [options]\n\n"
        << "C++ corpus-builder scaffold with explicit preflight, deterministic PGN range planning, header-only game-envelope scanning, accepted-game movetext replay, and raw-count-only aggregation. aggregate-counts emits inspectable position->move raw counts only; shaping, suppression, weighting, and trainer-side load semantics remain deferred.\n\n"
        << "Options:\n"
        << "  --mode <dry-run|preflight|plan-ranges|scan-headers|extract-openings|aggregate-counts>  Select builder mode. Default: dry-run.\n"
        << "  --input-pgn <path>                      Path to the source PGN file. Required for preflight, plan-ranges, scan-headers, extract-openings, aggregate-counts, and dry-run.\n"
        << "  --output-dir <path>                     Directory where the artifact bundle will be created. Required for plan-ranges, scan-headers, extract-openings, aggregate-counts, and dry-run.\n"
        << "  --input-format <pgn>                    Explicit source format. Currently only 'pgn' is supported.\n"
        << "  --target-range-bytes <uint64>           Nominal target size for each planned byte range. Must be > 0.\n"
        << "  --boundary-scan-bytes <uint64>          Forward scan window used to align nonzero starts to safe boundaries. Must be > 0.\n"
        << "  --max-ranges <int>                      Optional maximum number of planned ranges. Use 0 for no explicit limit.\n"
        << "  --emit-range-plan                       Emit range plan files during preflight when explicitly requested.\n"
        << "  --header-preview-limit <int>            Maximum preview rows to emit for scan-headers. Must be >= 0.\n"
        << "  --emit-header-preview                   Emit bounded header preview JSONL rows during scan-headers.\n"
        << "  --strict-header-scan                    Reject malformed or non-tag header lines during scan-headers.\n"
        << "  --min-rating <int>                      Inclusive lower rating bound.\n"
        << "  --max-rating <int>                      Inclusive upper rating bound.\n"
        << "  --rating-policy <value>                 Explicit rating eligibility policy. Required.\n"
        << rating_policy_help() << "\n"
        << "  --retained-ply <int>                    Number of opening plies to retain. Must be > 0.\n"
        << "  --threads <int>                         Requested worker thread count. Must be >= 1.\n"
        << "  --max-games <int>                       Maximum games to consider. Must be >= 0.\n"
        << "  --artifact-id <string>                  Optional explicit artifact bundle identifier.\n"
        << "  --progress-interval <int>               Backward-compatible work-unit progress interval. Must be >= 1.\n"
        << "  --heartbeat-seconds <int>               Guaranteed live heartbeat cadence for active stages. Must be between 1 and 60. Default: 30.\n"
        << "  --emit-progress-log                     Append live human-readable heartbeat lines to progress/progress.log inside the artifact bundle.\n"
        << "  --emit-status-json                      Overwrite progress/latest_status.json on each heartbeat with the latest machine-readable snapshot.\n"
        << "  --quiet-progress                        Suppress console heartbeat lines while still allowing file-based progress output for tests/fixtures.\n"
        << "  --strict-san-replay                     Preserve explicit SAN replay strictness during extraction/aggregation.\n"
        << "  --include-fen-snapshots                 Include FEN snapshots in extracted opening sequence rows.\n"
        << "  --include-uci-moves                     Include UCI move encodings in extracted opening sequence rows.\n"
        << "  --emit-extraction-preview               Emit bounded extraction preview JSONL rows during extract-openings.\n"
        << "  --extraction-preview-limit <int>        Maximum extraction preview rows to emit when requested. Must be >= 0.\n"
        << "  --emit-aggregate-preview                Emit bounded aggregate preview JSONL rows during aggregate-counts.\n"
        << "  --aggregate-preview-limit <int>         Maximum aggregate preview rows to emit when requested. Must be >= 0.\n"
        << "  --position-key-format <fen_normalized|fen_full>  Explicit aggregate position identity semantics. Required for aggregate-counts.\n"
        << "  --move-key-format <uci>                 Explicit aggregate move identity semantics. Required for aggregate-counts.\n"
        << "  --min-position-count <int>              Filter aggregated positions after counting. Must be >= 1.\n"
        << "  --payload-format <jsonl|sqlite|exact_sqlite_v2_compact>  Aggregate payload encoding. Default: jsonl. Only valid for aggregate-counts.\n"
        << "  --no-legacy-sqlite-mirror               Disable transitional legacy data/corpus.sqlite mirror when using compact v2 payload.\n"
        << "  --emit-canonical-predecessors           Emit canonical predecessor edge companion payload (default on for aggregate-counts).\n"
        << "  --no-canonical-predecessors             Disable canonical predecessor edge companion payload emission.\n"
        << "  --time-controls <value[,value...]>      Exact PGN TimeControl filter(s) enforced during ingestion for aggregate-counts (for example, 600+0).\n"
        << "  --time-control-id <value>               Canonical time-control contract id (for example, 600+0).\n"
        << "  --initial-time-seconds <int>            Canonical initial clock time in seconds.\n"
        << "  --increment-seconds <int>               Canonical increment in seconds.\n"
        << "  --time-format-label <label>             Display-only broad time label (for example, Rapid).\n"
        << "  --dry-run                               Legacy alias for --mode dry-run.\n"
        << "  --help                                  Print this help message and exit.\n"
        << "\nLive progress guarantee: during active stages the builder emits a flushed heartbeat at least once every --heartbeat-seconds (default 30, max 60), even when work-unit counters are changing slowly.\n";
}

}  // namespace otcb
