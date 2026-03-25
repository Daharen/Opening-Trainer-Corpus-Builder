#include "otcb/timing_conditioned_bundle_builder.hpp"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "otcb/rating_filter.hpp"
#include <sqlite3.h>

namespace otcb {
namespace {

constexpr const char* kSchemaVersion = "timing_conditioned_corpus_bundle_v1";
constexpr const char* kEmitterVersion = "otcb_timing_conditioned_bundle_emitter_v1";
constexpr const char* kOverlayPolicyVersion = "timing_overlay_policy_v1";
constexpr const char* kContextContractVersion = "context_contract_v1";

struct CorpusInfo {
    std::filesystem::path source_manifest_path;
    std::filesystem::path source_sqlite_path;
    std::string artifact_id;
    std::string source_identity;
    std::string position_key_format;
    std::string move_key_format;
    std::string payload_encoding_family;
    std::string rating_policy;
    std::string source_window;
    int rating_lower_bound = 0;
    int rating_upper_bound = 0;
    int retained_opening_ply = 0;
    bool raw_counts_preserved = false;
    int positions = 0;
    int moves = 0;
};

struct ProfileInfo {
    std::filesystem::path source_sqlite_path;
    std::string artifact_id;
    std::string source_month_window_summary;
    std::string time_control_scope;
    std::string rating_policy_version;
    std::string fitting_method_version;
    std::string merge_metric_version;
    double merge_threshold = 0.0;
    bool has_move_pressure_profiles = false;
    bool has_think_time_profiles = false;
    bool has_context_map = false;
    int move_pressure_profiles = 0;
    int think_time_profiles = 0;
    int contexts = 0;
    std::set<std::string> context_time_controls;
    std::set<std::string> context_elo_bands;
};

struct CompatibilityResult {
    std::vector<std::string> warnings;
    std::vector<std::string> errors;
};

struct JsonManifest {
    std::map<std::string, std::string> string_fields;
    std::map<std::string, int> int_fields;
    std::map<std::string, bool> bool_fields;
};

static std::string read_file(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open file: " + path.string());
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

static std::optional<std::string> extract_json_string(const std::string& json, const std::string& key) {
    const std::string marker = "\"" + key + "\"";
    const auto key_pos = json.find(marker);
    if (key_pos == std::string::npos) return std::nullopt;
    auto colon = json.find(':', key_pos + marker.size());
    if (colon == std::string::npos) return std::nullopt;
    auto first_quote = json.find('"', colon + 1);
    if (first_quote == std::string::npos) return std::nullopt;
    std::string out;
    for (std::size_t i = first_quote + 1; i < json.size(); ++i) {
        const char ch = json[i];
        if (ch == '\\' && i + 1 < json.size()) {
            out.push_back(json[i + 1]);
            ++i;
            continue;
        }
        if (ch == '"') return out;
        out.push_back(ch);
    }
    return std::nullopt;
}

static std::optional<int> extract_json_int(const std::string& json, const std::string& key) {
    const std::string marker = "\"" + key + "\"";
    const auto key_pos = json.find(marker);
    if (key_pos == std::string::npos) return std::nullopt;
    auto colon = json.find(':', key_pos + marker.size());
    if (colon == std::string::npos) return std::nullopt;
    std::size_t i = colon + 1;
    while (i < json.size() && std::isspace(static_cast<unsigned char>(json[i]))) ++i;
    std::size_t j = i;
    if (j < json.size() && (json[j] == '-' || json[j] == '+')) ++j;
    while (j < json.size() && std::isdigit(static_cast<unsigned char>(json[j]))) ++j;
    if (j == i) return std::nullopt;
    return std::stoi(json.substr(i, j - i));
}

static std::optional<bool> extract_json_bool(const std::string& json, const std::string& key) {
    const std::string marker = "\"" + key + "\"";
    const auto key_pos = json.find(marker);
    if (key_pos == std::string::npos) return std::nullopt;
    auto colon = json.find(':', key_pos + marker.size());
    if (colon == std::string::npos) return std::nullopt;
    std::size_t i = colon + 1;
    while (i < json.size() && std::isspace(static_cast<unsigned char>(json[i]))) ++i;
    if (json.compare(i, 4, "true") == 0) return true;
    if (json.compare(i, 5, "false") == 0) return false;
    return std::nullopt;
}

static std::string json_escape(const std::string& input) {
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

static std::uint64_t fnv1a64(const std::string& text) {
    std::uint64_t h = 1469598103934665603ULL;
    for (unsigned char ch : text) {
        h ^= static_cast<std::uint64_t>(ch);
        h *= 1099511628211ULL;
    }
    return h;
}

static std::string make_hex(std::uint64_t value) {
    std::ostringstream oss;
    oss << std::hex << value;
    return oss.str();
}

static std::string join(const std::vector<std::string>& parts, const std::string& sep) {
    std::ostringstream oss;
    for (std::size_t i = 0; i < parts.size(); ++i) {
        if (i > 0) oss << sep;
        oss << parts[i];
    }
    return oss.str();
}

static std::string read_git_commit_if_available() {
    const auto head_path = std::filesystem::current_path() / ".git" / "HEAD";
    if (!std::filesystem::exists(head_path)) return "unknown";
    const std::string head = read_file(head_path);
    const std::string ref_prefix = "ref:";
    if (head.rfind(ref_prefix, 0) == 0) {
        std::string ref = head.substr(ref_prefix.size());
        while (!ref.empty() && std::isspace(static_cast<unsigned char>(ref.front()))) ref.erase(ref.begin());
        while (!ref.empty() && std::isspace(static_cast<unsigned char>(ref.back()))) ref.pop_back();
        const auto ref_path = std::filesystem::current_path() / ".git" / ref;
        if (!std::filesystem::exists(ref_path)) return "unknown";
        std::string sha = read_file(ref_path);
        while (!sha.empty() && std::isspace(static_cast<unsigned char>(sha.back()))) sha.pop_back();
        return sha.empty() ? "unknown" : sha;
    }
    std::string sha = head;
    while (!sha.empty() && std::isspace(static_cast<unsigned char>(sha.back()))) sha.pop_back();
    return sha.empty() ? "unknown" : sha;
}

static void exec_sql(sqlite3* db, const std::string& sql) {
    char* errmsg = nullptr;
    if (sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &errmsg) != SQLITE_OK) {
        std::string msg = errmsg ? errmsg : "sqlite exec failure";
        sqlite3_free(errmsg);
        throw std::runtime_error(msg);
    }
}

static int one_int(sqlite3* db, const std::string& sql) {
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) throw std::runtime_error("prepare failed");
    std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> guard(stmt, sqlite3_finalize);
    if (sqlite3_step(stmt) != SQLITE_ROW) return 0;
    return sqlite3_column_int(stmt, 0);
}

static std::string one_text(sqlite3* db, const std::string& sql, const std::string& fallback = "") {
    std::string out = fallback;
    auto cb = [](void* user, int argc, char** argv, char**) -> int {
        if (argc > 0 && argv[0]) {
            *static_cast<std::string*>(user) = argv[0];
        }
        return 0;
    };
    char* errmsg = nullptr;
    if (sqlite3_exec(db, sql.c_str(), cb, &out, &errmsg) != SQLITE_OK) {
        if (errmsg) sqlite3_free(errmsg);
        return fallback;
    }
    if (errmsg) sqlite3_free(errmsg);
    return out;
}

static bool table_exists(sqlite3* db, const std::string& table) {
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", -1, &stmt, nullptr) != SQLITE_OK) return false;
    std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> guard(stmt, sqlite3_finalize);
    sqlite3_bind_text(stmt, 1, table.c_str(), -1, SQLITE_TRANSIENT);
    return sqlite3_step(stmt) == SQLITE_ROW;
}

static std::filesystem::path resolve_corpus_sqlite(const std::filesystem::path& input, CorpusInfo& info) {
    if (std::filesystem::is_directory(input)) {
        info.source_manifest_path = input / "manifest.json";
        if (!std::filesystem::exists(info.source_manifest_path)) throw std::runtime_error("input corpus bundle missing manifest.json");
        const auto json = read_file(info.source_manifest_path);
        info.artifact_id = extract_json_string(json, "artifact_id").value_or("unknown_corpus_artifact");
        info.position_key_format = extract_json_string(json, "position_key_format").value_or("unknown");
        info.move_key_format = extract_json_string(json, "move_key_format").value_or("unknown");
        info.retained_opening_ply = extract_json_int(json, "retained_opening_ply").value_or(0);
        info.rating_lower_bound = extract_json_int(json, "rating_lower_bound").value_or(0);
        info.rating_upper_bound = extract_json_int(json, "rating_upper_bound").value_or(0);
        info.rating_policy = extract_json_string(json, "rating_policy").value_or("unknown");
        info.raw_counts_preserved = extract_json_bool(json, "raw_counts_preserved").value_or(false);
        info.payload_encoding_family = extract_json_string(json, "payload_format").value_or("unknown");
        info.source_identity = extract_json_string(json, "source_corpus_identity").value_or("unknown");
        info.source_window = extract_json_string(json, "input_pgn_path").value_or("unknown");
        const auto sqlite_rel = extract_json_string(json, "aggregate_sqlite_file").value_or("data/corpus.sqlite");
        const auto sqlite_path = input / sqlite_rel;
        if (!std::filesystem::exists(sqlite_path)) throw std::runtime_error("input corpus sqlite payload missing: " + sqlite_path.string());
        return sqlite_path;
    }
    if (!std::filesystem::exists(input)) throw std::runtime_error("input corpus artifact does not exist");
    info.source_manifest_path.clear();
    info.source_sqlite_path = input;
    info.artifact_id = input.filename().string();
    info.payload_encoding_family = "sqlite";
    info.source_identity = input.filename().string();
    info.source_window = "unknown";
    return input;
}

static CorpusInfo read_corpus_info(const std::filesystem::path& input) {
    CorpusInfo info;
    const auto sqlite_path = resolve_corpus_sqlite(input, info);
    info.source_sqlite_path = sqlite_path;

    sqlite3* db = nullptr;
    if (sqlite3_open(sqlite_path.string().c_str(), &db) != SQLITE_OK) throw std::runtime_error("failed to open corpus sqlite");
    std::unique_ptr<sqlite3, decltype(&sqlite3_close)> guard(db, sqlite3_close);

    if (!table_exists(db, "positions") || !table_exists(db, "moves")) {
        throw std::runtime_error("input corpus sqlite missing positions/moves tables");
    }
    if (table_exists(db, "artifact_metadata")) {
        auto get_meta = [&](const std::string& key, const std::string& fallback = "") {
            return one_text(db, "SELECT value FROM artifact_metadata WHERE key='" + key + "' LIMIT 1;", fallback);
        };
        info.position_key_format = info.position_key_format == "unknown" ? get_meta("position_key_format", info.position_key_format) : info.position_key_format;
        info.move_key_format = info.move_key_format == "unknown" ? get_meta("move_key_format", info.move_key_format) : info.move_key_format;
        info.rating_policy = info.rating_policy == "unknown" ? get_meta("rating_policy", info.rating_policy) : info.rating_policy;
        if (!info.raw_counts_preserved) info.raw_counts_preserved = get_meta("raw_counts_preserved", "false") == "true";
        if (info.retained_opening_ply == 0) info.retained_opening_ply = std::stoi(get_meta("retained_ply", "0"));
        if (info.rating_lower_bound == 0) info.rating_lower_bound = std::stoi(get_meta("min_rating", "0"));
        if (info.rating_upper_bound == 0) info.rating_upper_bound = std::stoi(get_meta("max_rating", "0"));
    }

    info.positions = one_int(db, "SELECT COUNT(*) FROM positions");
    info.moves = one_int(db, "SELECT COUNT(*) FROM moves");
    return info;
}

static ProfileInfo read_profile_info(const std::filesystem::path& path) {
    if (!std::filesystem::exists(path)) throw std::runtime_error("input profile set does not exist");
    ProfileInfo info;
    info.source_sqlite_path = path;

    sqlite3* db = nullptr;
    if (sqlite3_open(path.string().c_str(), &db) != SQLITE_OK) throw std::runtime_error("failed to open profile sqlite");
    std::unique_ptr<sqlite3, decltype(&sqlite3_close)> guard(db, sqlite3_close);

    for (const auto& table : {"profile_manifest", "move_pressure_profiles", "think_time_profiles", "context_profile_map"}) {
        if (!table_exists(db, table)) throw std::runtime_error("input profile set missing table: " + std::string(table));
    }

    info.artifact_id = one_text(db, "SELECT builder_version FROM profile_manifest LIMIT 1", "unknown_profile_artifact");
    info.source_month_window_summary = one_text(db, "SELECT source_month_window_summary FROM profile_manifest LIMIT 1", "unknown");
    info.rating_policy_version = one_text(db, "SELECT rating_policy_version FROM profile_manifest LIMIT 1", "unknown");
    info.fitting_method_version = one_text(db, "SELECT fitting_method_version FROM profile_manifest LIMIT 1", "unknown");
    info.merge_metric_version = one_text(db, "SELECT merge_metric_version FROM profile_manifest LIMIT 1", "unknown");
    info.merge_threshold = std::stod(one_text(db, "SELECT merge_threshold_value FROM profile_manifest LIMIT 1", "0"));

    info.move_pressure_profiles = one_int(db, "SELECT COUNT(*) FROM move_pressure_profiles");
    info.think_time_profiles = one_int(db, "SELECT COUNT(*) FROM think_time_profiles");
    info.contexts = one_int(db, "SELECT COUNT(*) FROM context_profile_map");
    info.has_move_pressure_profiles = info.move_pressure_profiles > 0;
    info.has_think_time_profiles = info.think_time_profiles > 0;
    info.has_context_map = info.contexts > 0;

    auto cb = [](void* user, int argc, char** argv, char**) -> int {
        auto* info = static_cast<ProfileInfo*>(user);
        if (argc >= 2) {
            if (argv[0]) info->context_time_controls.insert(argv[0]);
            if (argv[1]) info->context_elo_bands.insert(argv[1]);
        }
        return 0;
    };
    char* errmsg = nullptr;
    if (sqlite3_exec(db, "SELECT DISTINCT time_control_id,mover_elo_band FROM context_profile_map ORDER BY time_control_id,mover_elo_band;", cb, &info, &errmsg) != SQLITE_OK) {
        std::string msg = errmsg ? errmsg : "failed reading context_profile_map";
        if (errmsg) sqlite3_free(errmsg);
        throw std::runtime_error(msg);
    }
    if (errmsg) sqlite3_free(errmsg);
    info.time_control_scope = info.context_time_controls.empty() ? "none" : *info.context_time_controls.begin();
    return info;
}

static CompatibilityResult check_compatibility(const CorpusInfo& corpus, const ProfileInfo& profile, const TimingConditionedBundleOptions& options) {
    CompatibilityResult result;

    if (corpus.position_key_format != "fen_normalized") {
        result.warnings.push_back("position_key_format expected fen_normalized but got '" + corpus.position_key_format + "'");
    }
    if (corpus.move_key_format != "uci") {
        result.warnings.push_back("move_key_format expected uci but got '" + corpus.move_key_format + "'");
    }
    if (!profile.has_move_pressure_profiles || !profile.has_think_time_profiles || !profile.has_context_map) {
        result.errors.push_back("profile artifact is missing required profile/context payload tables");
    }
    if (corpus.retained_opening_ply > 0 && corpus.retained_opening_ply < 10) {
        result.errors.push_back("retained_opening_ply is below minimum envelope (10) for opening_ply_band context compatibility");
    }

    if (!options.time_controls.empty()) {
        for (const auto& tc : options.time_controls) {
            if (!profile.context_time_controls.count(tc)) {
                result.errors.push_back("requested --time-controls '" + tc + "' not present in profile context map");
            }
        }
    }

    std::vector<EloRange> requested;
    for (const auto& band : options.elo_bands) {
        const auto parsed = parse_elo_range(band);
        if (!parsed.has_value()) throw std::runtime_error("Invalid --elo-bands range: " + band);
        requested.push_back(*parsed);
    }
    if (!requested.empty()) {
        bool has_overlap = false;
        for (const auto& range : requested) {
            if (!(range.hi < corpus.rating_lower_bound || range.lo > corpus.rating_upper_bound)) {
                has_overlap = true;
                break;
            }
        }
        if (!has_overlap) {
            result.errors.push_back("requested --elo-bands do not overlap corpus rating bounds");
        }
    }

    if (corpus.rating_policy == "unknown") {
        result.warnings.push_back("corpus rating_policy unknown; runtime rating-policy compatibility cannot be fully proven");
    }
    if (profile.rating_policy_version == "unknown") {
        result.warnings.push_back("profile rating_policy_version unknown");
    }

    return result;
}

TimingConditionedBundleOptions parse_or_throw(int argc, char** argv) {
    TimingConditionedBundleOptions opts;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto need = [&](const std::string& flag) {
            if (i + 1 >= argc) throw std::runtime_error("Missing value for " + flag);
            return std::string(argv[++i]);
        };
        if (arg == "--help") {
            print_timing_conditioned_bundle_usage(argc > 0 ? argv[0] : "build-timing-conditioned-corpus-bundle");
            std::exit(0);
        }
        if (arg == "--input-corpus-bundle") { opts.input_corpus_bundle = need(arg); continue; }
        if (arg == "--input-profile-set") { opts.input_profile_set = need(arg); continue; }
        if (arg == "--output") { opts.output = need(arg); continue; }
        if (arg == "--overwrite") { opts.overwrite = true; continue; }
        if (arg == "--artifact-id") { opts.artifact_id_override = need(arg); continue; }
        if (arg == "--prototype-label") { opts.prototype_label = need(arg); continue; }
        if (arg == "--time-controls") { opts.time_controls.push_back(need(arg)); continue; }
        if (arg == "--elo-bands") { opts.elo_bands.push_back(need(arg)); continue; }
        if (arg == "--log-every") { opts.log_every = std::stoi(need(arg)); continue; }
        if (arg == "--strict-compatibility") { opts.strict_compatibility = true; continue; }
        if (arg == "--allow-prototype-mismatch") { opts.allow_prototype_mismatch = true; continue; }
        if (arg == "--embed-fit-diagnostics") { opts.embed_fit_diagnostics = true; continue; }
        if (arg == "--emit-progress-log") { opts.emit_progress_log = true; continue; }
        if (arg == "--emit-status-json") { opts.emit_status_json = true; continue; }
        throw std::runtime_error("Unknown argument: " + arg);
    }
    if (opts.input_corpus_bundle.empty()) throw std::runtime_error("--input-corpus-bundle is required");
    if (opts.input_profile_set.empty()) throw std::runtime_error("--input-profile-set is required");
    if (opts.output.empty()) throw std::runtime_error("--output is required");
    return opts;
}

}  // namespace

TimingConditionedBundleOptions parse_timing_conditioned_bundle_cli(int argc, char** argv) {
    return parse_or_throw(argc, argv);
}

void print_timing_conditioned_bundle_usage(const std::string& program_name) {
    std::cout
        << "Usage: " << program_name << " --input-corpus-bundle <bundle_or_sqlite> --input-profile-set <sqlite> --output <dir> [options]\n"
        << "Options:\n"
        << "  --overwrite                           Replace existing output directory.\n"
        << "  --artifact-id <id>                    Optional artifact id override.\n"
        << "  --prototype-label <label>             Optional prototype/test label.\n"
        << "  --time-controls <initial+increment>   Optional narrowing filter; repeatable.\n"
        << "  --elo-bands <lo-hi>                   Optional narrowing filter; repeatable inclusive ranges.\n"
        << "  --log-every <n>                       Progress cadence for long stages.\n"
        << "  --strict-compatibility                Fail on any compatibility warning.\n"
        << "  --allow-prototype-mismatch            Allow compatibility mismatches only for prototype output.\n"
        << "  --embed-fit-diagnostics               Copy optional fit_diagnostics table when present.\n"
        << "  --emit-progress-log                   Emit progress log file into output bundle.\n"
        << "  --emit-status-json                    Emit status json file into output bundle.\n";
}

TimingConditionedBundleCounters build_timing_conditioned_corpus_bundle(const TimingConditionedBundleOptions& options) {
    TimingConditionedBundleCounters counters;
    const CorpusInfo corpus = read_corpus_info(options.input_corpus_bundle);
    ++counters.corpus_artifacts_loaded;
    counters.positions_examined = corpus.positions;
    counters.move_rows_examined = corpus.moves;

    const ProfileInfo profile = read_profile_info(options.input_profile_set);
    ++counters.profile_artifacts_loaded;
    counters.contexts_mapped = profile.contexts;
    counters.profiles_referenced = profile.move_pressure_profiles + profile.think_time_profiles;

    const CompatibilityResult compatibility = check_compatibility(corpus, profile, options);
    counters.compatibility_warnings = static_cast<int>(compatibility.warnings.size());

    if (!compatibility.errors.empty()) {
        if (!(options.allow_prototype_mismatch && !options.prototype_label.empty())) {
            throw std::runtime_error("Compatibility check failed: " + join(compatibility.errors, " | "));
        }
    }
    if (options.strict_compatibility && !compatibility.warnings.empty()) {
        throw std::runtime_error("Strict compatibility enabled and warnings are present: " + join(compatibility.warnings, " | "));
    }

    std::filesystem::path output_root = options.output;
    if (std::filesystem::exists(output_root)) {
        if (!options.overwrite) throw std::runtime_error("output already exists; pass --overwrite");
        std::filesystem::remove_all(output_root);
    }
    std::filesystem::create_directories(output_root / "data");

    const std::string artifact_id = options.artifact_id_override.empty()
        ? (std::string("timing_conditioned_") + make_hex(fnv1a64(corpus.artifact_id + "|" + profile.artifact_id + "|" + join(options.time_controls, ",") + "|" + join(options.elo_bands, ",") + "|" + options.prototype_label)))
        : options.artifact_id_override;

    const auto corpus_copy = output_root / "data" / "exact_corpus.sqlite";
    const auto profile_copy = output_root / "data" / "behavioral_profile_set.sqlite";
    std::filesystem::copy_file(corpus.source_sqlite_path, corpus_copy, std::filesystem::copy_options::overwrite_existing);
    std::filesystem::copy_file(profile.source_sqlite_path, profile_copy, std::filesystem::copy_options::overwrite_existing);

    if (options.embed_fit_diagnostics) {
        sqlite3* src = nullptr;
        sqlite3* dst = nullptr;
        if (sqlite3_open(profile.source_sqlite_path.string().c_str(), &src) == SQLITE_OK && sqlite3_open(profile_copy.string().c_str(), &dst) == SQLITE_OK) {
            if (table_exists(src, "fit_diagnostics") && !table_exists(dst, "fit_diagnostics")) {
                exec_sql(dst, "ATTACH DATABASE '" + profile.source_sqlite_path.string() + "' AS src;");
                exec_sql(dst, "CREATE TABLE fit_diagnostics AS SELECT * FROM src.fit_diagnostics;");
                exec_sql(dst, "DETACH DATABASE src;");
            }
        }
        if (src) sqlite3_close(src);
        if (dst) sqlite3_close(dst);
    }

    const std::string git_commit = read_git_commit_if_available();
    std::ofstream manifest(output_root / "manifest.json", std::ios::binary);
    manifest << "{\n";
    manifest << "  \"artifact_schema_version\": \"" << kSchemaVersion << "\",\n";
    manifest << "  \"artifact_id\": \"" << json_escape(artifact_id) << "\",\n";
    manifest << "  \"emitter_version\": \"" << kEmitterVersion << "\",\n";
    manifest << "  \"builder_repo_commit\": \"" << json_escape(git_commit) << "\",\n";
    manifest << "  \"build_timestamp_utc\": \"deterministic\",\n";
    manifest << "  \"exact_corpus_artifact_identity\": \"" << json_escape(corpus.artifact_id) << "\",\n";
    manifest << "  \"behavioral_profile_set_artifact_identity\": \"" << json_escape(profile.artifact_id) << "\",\n";
    manifest << "  \"exact_corpus_source_window_or_identity\": \"" << json_escape(corpus.source_window) << "\",\n";
    manifest << "  \"behavioral_profile_source_month_window_summary\": \"" << json_escape(profile.source_month_window_summary) << "\",\n";
    manifest << "  \"rating_lower_bound\": " << corpus.rating_lower_bound << ",\n";
    manifest << "  \"rating_upper_bound\": " << corpus.rating_upper_bound << ",\n";
    manifest << "  \"rating_eligibility_policy\": \"" << json_escape(corpus.rating_policy) << "\",\n";
    manifest << "  \"retained_opening_depth\": " << corpus.retained_opening_ply << ",\n";
    manifest << "  \"position_key_format_description\": \"" << json_escape(corpus.position_key_format) << "\",\n";
    manifest << "  \"move_representation_description\": \"" << json_escape(corpus.move_key_format) << "\",\n";
    manifest << "  \"payload_encoding_family\": \"" << json_escape(corpus.payload_encoding_family) << "\",\n";
    manifest << "  \"raw_counts_preserved\": " << (corpus.raw_counts_preserved ? "true" : "false") << ",\n";
    manifest << "  \"timing_overlay_prototype_only\": " << (options.prototype_label.empty() ? "false" : "true") << ",\n";
    manifest << "  \"timing_overlay_policy_version\": \"" << kOverlayPolicyVersion << "\",\n";
    manifest << "  \"context_key_contract_version\": \"" << kContextContractVersion << "\",\n";
    manifest << "  \"precomputed_effective_weights_present\": false,\n";
    manifest << "  \"context_contract_dimensions\": \"time_control_id,mover_elo_band,clock_pressure_bucket,prev_opp_think_bucket,opening_ply_band\",\n";
    manifest << "  \"time_controls_filter\": \"" << json_escape(join(options.time_controls, ",")) << "\",\n";
    manifest << "  \"elo_bands_filter\": \"" << json_escape(join(options.elo_bands, ",")) << "\",\n";
    manifest << "  \"prototype_label\": \"" << json_escape(options.prototype_label) << "\",\n";
    manifest << "  \"payload_files\": [\"data/exact_corpus.sqlite\",\"data/behavioral_profile_set.sqlite\"],\n";
    manifest << "  \"compatibility_warnings\": [";
    for (std::size_t i = 0; i < compatibility.warnings.size(); ++i) {
        if (i > 0) manifest << ',';
        manifest << "\"" << json_escape(compatibility.warnings[i]) << "\"";
    }
    manifest << "],\n";
    manifest << "  \"compatibility_errors_allowed\": [";
    for (std::size_t i = 0; i < compatibility.errors.size(); ++i) {
        if (i > 0) manifest << ',';
        manifest << "\"" << json_escape(compatibility.errors[i]) << "\"";
    }
    manifest << "]\n";
    manifest << "}\n";

    if (options.emit_progress_log) {
        std::ofstream log(output_root / "progress.log", std::ios::binary);
        log << "corpus_artifacts_loaded=" << counters.corpus_artifacts_loaded << "\n";
        log << "profile_artifacts_loaded=" << counters.profile_artifacts_loaded << "\n";
        log << "contexts_mapped=" << counters.contexts_mapped << "\n";
        log << "profiles_referenced=" << counters.profiles_referenced << "\n";
    }
    if (options.emit_status_json) {
        std::ofstream status(output_root / "status.json", std::ios::binary);
        status << "{\"status\":\"ok\",\"positions_examined\":" << counters.positions_examined << ",\"move_rows_examined\":" << counters.move_rows_examined << "}\n";
    }

    counters.emitted_bundle_path = output_root;
    return counters;
}

}  // namespace otcb
