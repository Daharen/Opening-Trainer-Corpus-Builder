#include "otcb/behavioral_profile_builder.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <numeric>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

#include "otcb/rating_filter.hpp"
#include <sqlite3.h>

namespace otcb {
namespace {

constexpr const char* kProfileSchemaVersion = "1";
constexpr const char* kBuilderVersion = "otcb_behavioral_profiles_v1";
constexpr const char* kFitMethodVersion = "v1_weighted_context_means";
constexpr const char* kMergeMetricVersion = "v1_l1_parameter_distance";

struct MonthWindow {
    int start = 0;
    int end = 999999;
};

struct ExtractRow {
    std::string source_month;
    std::string time_control_id;
    std::string mover_elo_band;
    std::string clock_pressure_bucket;
    std::string prev_opp_think_bucket;
    std::string position_key;
    std::string move_uci;
    int opening_ply_index = 0;
    double think_time_seconds = 0.0;
    double mover_clock_before_seconds = 0.0;
};

struct ContextKey {
    std::string time_control_id;
    std::string mover_elo_band;
    std::string clock_pressure_bucket;
    std::string prev_opp_think_bucket;
    std::string opening_ply_band;

    auto tie() const {
        return std::tie(time_control_id, mover_elo_band, clock_pressure_bucket, prev_opp_think_bucket, opening_ply_band);
    }
    bool operator<(const ContextKey& other) const { return tie() < other.tie(); }
};

struct ContextStats {
    ContextKey key;
    int support_count = 0;
    std::string min_month = "9999-99";
    std::string max_month = "0000-00";

    std::map<std::string, int> position_counts;
    std::map<std::string, int> position_move_counts;
    std::vector<double> think_times;
    std::vector<double> clock_before;
};

struct MovePressureProfile {
    std::string profile_id;
    int support_count = 0;
    std::string source_month_start;
    std::string source_month_end;
    std::string time_control_family;
    std::string elo_family;
    double pressure_sensitivity = 0.0;
    double decisiveness = 0.0;
    double move_diversity = 0.0;
    double fit_mae = 0.0;
    int merged_contexts = 0;
};

struct ThinkTimeProfile {
    std::string profile_id;
    int support_count = 0;
    std::string source_month_start;
    std::string source_month_end;
    double base_time_scale = 0.0;
    double spread = 0.0;
    double short_mass = 0.0;
    double deep_think_tail_mass = 0.0;
    double timeout_tail_mass = 0.0;
    double fit_mae = 0.0;
    int merged_contexts = 0;
};

struct ContextMapping {
    ContextKey key;
    std::string move_pressure_profile_id;
    std::string think_time_profile_id;
    int support_count = 0;
};

static void exec_sql(sqlite3* db, const std::string& sql) {
    char* errmsg = nullptr;
    if (sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &errmsg) != SQLITE_OK) {
        std::string msg = errmsg ? errmsg : "sqlite exec failure";
        sqlite3_free(errmsg);
        throw std::runtime_error(msg);
    }
}

static int parse_yyyy_mm(const std::string& s) {
    if (s.size() != 7 || s[4] != '-') return -1;
    for (std::size_t i = 0; i < s.size(); ++i) {
        if (i == 4) continue;
        if (!std::isdigit(static_cast<unsigned char>(s[i]))) return -1;
    }
    const int y = std::stoi(s.substr(0, 4));
    const int m = std::stoi(s.substr(5, 2));
    if (m < 1 || m > 12) return -1;
    return y * 100 + m;
}

static MonthWindow parse_month_window(const std::optional<std::string>& in) {
    MonthWindow out;
    if (!in.has_value() || in->empty()) return out;
    const auto pos = in->find(':');
    if (pos == std::string::npos) throw std::runtime_error("--month-window expects YYYY-MM:YYYY-MM");
    const auto a = parse_yyyy_mm(in->substr(0, pos));
    const auto b = parse_yyyy_mm(in->substr(pos + 1));
    if (a < 0 || b < 0 || a > b) throw std::runtime_error("Invalid --month-window range");
    out.start = a;
    out.end = b;
    return out;
}

static std::string opening_ply_band(int ply) {
    if (ply <= 10) return "01-10";
    if (ply <= 20) return "11-20";
    if (ply <= 30) return "21-30";
    return "31+";
}

static std::string join_set(const std::set<std::string>& values) {
    std::ostringstream oss;
    bool first = true;
    for (const auto& v : values) {
        if (!first) oss << ',';
        first = false;
        oss << v;
    }
    return oss.str();
}

static double percentile(std::vector<double> values, double p) {
    if (values.empty()) return 0.0;
    std::sort(values.begin(), values.end());
    const double idx = p * static_cast<double>(values.size() - 1);
    const auto lo = static_cast<std::size_t>(std::floor(idx));
    const auto hi = static_cast<std::size_t>(std::ceil(idx));
    if (lo == hi) return values[lo];
    const double frac = idx - static_cast<double>(lo);
    return values[lo] * (1.0 - frac) + values[hi] * frac;
}

static int parse_int_or_zero(const char* s) {
    return s ? std::stoi(s) : 0;
}

static double parse_double_or_zero(const char* s) {
    return s ? std::stod(s) : 0.0;
}

static MovePressureProfile fit_move_pressure(const ContextStats& c) {
    MovePressureProfile p;
    p.support_count = c.support_count;
    p.source_month_start = c.min_month;
    p.source_month_end = c.max_month;
    p.time_control_family = c.key.time_control_id;
    p.elo_family = c.key.mover_elo_band;

    double avg_share = 0.0;
    double avg_best_share = 0.0;
    double entropy = 0.0;
    int positions = 0;

    std::map<std::string, std::map<std::string, int>> by_pos_move;
    for (const auto& [pmk, cnt] : c.position_move_counts) {
        const auto split = pmk.find('|');
        if (split == std::string::npos) continue;
        by_pos_move[pmk.substr(0, split)][pmk.substr(split + 1)] = cnt;
    }

    for (const auto& [pos, total] : c.position_counts) {
        const auto it = by_pos_move.find(pos);
        if (it == by_pos_move.end() || total <= 0) continue;
        ++positions;
        int best = 0;
        double local_entropy = 0.0;
        for (const auto& [mv, cnt] : it->second) {
            (void)mv;
            best = std::max(best, cnt);
            const double prob = static_cast<double>(cnt) / static_cast<double>(total);
            avg_share += prob;
            if (prob > 0.0) local_entropy += -prob * std::log(prob);
        }
        avg_best_share += static_cast<double>(best) / static_cast<double>(total);
        entropy += local_entropy;
    }
    if (positions == 0) positions = 1;

    p.decisiveness = avg_best_share / static_cast<double>(positions);
    p.move_diversity = entropy / static_cast<double>(positions);
    p.pressure_sensitivity = (c.key.clock_pressure_bucket == "critical" ? 1.0 : c.key.clock_pressure_bucket == "low" ? 0.66 : c.key.clock_pressure_bucket == "medium" ? 0.33 : 0.1) * (1.0 - p.decisiveness);
    p.fit_mae = std::abs((avg_share / static_cast<double>(positions)) - p.decisiveness);
    p.merged_contexts = 1;
    return p;
}

static ThinkTimeProfile fit_think_time(const ContextStats& c) {
    ThinkTimeProfile p;
    p.support_count = c.support_count;
    p.source_month_start = c.min_month;
    p.source_month_end = c.max_month;

    std::vector<double> values = c.think_times;
    const double p50 = percentile(values, 0.50);
    const double p90 = percentile(values, 0.90);
    const double mean = values.empty() ? 0.0 : std::accumulate(values.begin(), values.end(), 0.0) / static_cast<double>(values.size());
    double variance = 0.0;
    for (double v : values) {
        const double d = v - mean;
        variance += d * d;
    }
    variance = values.empty() ? 0.0 : variance / static_cast<double>(values.size());

    int short_count = 0;
    int deep_count = 0;
    int timeout_count = 0;
    for (std::size_t i = 0; i < values.size(); ++i) {
        if (values[i] < 2.0) ++short_count;
        if (values[i] >= 30.0) ++deep_count;
        if (i < c.clock_before.size() && c.clock_before[i] > 0.0 && values[i] / c.clock_before[i] >= 0.90) ++timeout_count;
    }
    const double denom = values.empty() ? 1.0 : static_cast<double>(values.size());

    p.base_time_scale = p50;
    p.spread = std::sqrt(variance);
    p.short_mass = static_cast<double>(short_count) / denom;
    p.deep_think_tail_mass = static_cast<double>(deep_count) / denom;
    p.timeout_tail_mass = static_cast<double>(timeout_count) / denom;
    p.fit_mae = std::abs(mean - p50) + std::abs(p90 - (p50 + p.spread));
    p.merged_contexts = 1;
    return p;
}

static double distance(const MovePressureProfile& a, const MovePressureProfile& b) {
    return std::abs(a.pressure_sensitivity - b.pressure_sensitivity) + std::abs(a.decisiveness - b.decisiveness) + std::abs(a.move_diversity - b.move_diversity);
}

static double distance(const ThinkTimeProfile& a, const ThinkTimeProfile& b) {
    return std::abs(a.base_time_scale - b.base_time_scale) / 60.0 + std::abs(a.spread - b.spread) / 60.0 + std::abs(a.short_mass - b.short_mass) + std::abs(a.deep_think_tail_mass - b.deep_think_tail_mass) + std::abs(a.timeout_tail_mass - b.timeout_tail_mass);
}

static void apply_weighted_merge(MovePressureProfile& dst, const MovePressureProfile& src) {
    const double sw = static_cast<double>(src.support_count);
    const double dw = static_cast<double>(dst.support_count);
    const double total = std::max(1.0, sw + dw);
    auto blend = [&](double a, double b) { return (a * dw + b * sw) / total; };
    dst.pressure_sensitivity = blend(dst.pressure_sensitivity, src.pressure_sensitivity);
    dst.decisiveness = blend(dst.decisiveness, src.decisiveness);
    dst.move_diversity = blend(dst.move_diversity, src.move_diversity);
    dst.fit_mae = blend(dst.fit_mae, src.fit_mae);
    dst.support_count += src.support_count;
    dst.source_month_start = std::min(dst.source_month_start, src.source_month_start);
    dst.source_month_end = std::max(dst.source_month_end, src.source_month_end);
    ++dst.merged_contexts;
}

static void apply_weighted_merge(ThinkTimeProfile& dst, const ThinkTimeProfile& src) {
    const double sw = static_cast<double>(src.support_count);
    const double dw = static_cast<double>(dst.support_count);
    const double total = std::max(1.0, sw + dw);
    auto blend = [&](double a, double b) { return (a * dw + b * sw) / total; };
    dst.base_time_scale = blend(dst.base_time_scale, src.base_time_scale);
    dst.spread = blend(dst.spread, src.spread);
    dst.short_mass = blend(dst.short_mass, src.short_mass);
    dst.deep_think_tail_mass = blend(dst.deep_think_tail_mass, src.deep_think_tail_mass);
    dst.timeout_tail_mass = blend(dst.timeout_tail_mass, src.timeout_tail_mass);
    dst.fit_mae = blend(dst.fit_mae, src.fit_mae);
    dst.support_count += src.support_count;
    dst.source_month_start = std::min(dst.source_month_start, src.source_month_start);
    dst.source_month_end = std::max(dst.source_month_end, src.source_month_end);
    ++dst.merged_contexts;
}

static void init_profile_schema(sqlite3* db, bool emit_diagnostics, bool emit_invalid_report) {
    exec_sql(db,
        "CREATE TABLE IF NOT EXISTS profile_manifest ("
        "schema_version TEXT NOT NULL, builder_version TEXT NOT NULL, build_timestamp_utc TEXT NOT NULL,"
        "extract_source_identities TEXT NOT NULL, source_month_window_summary TEXT NOT NULL,"
        "time_control_normalization_policy_version TEXT NOT NULL, rating_policy_version TEXT NOT NULL,"
        "fitting_method_version TEXT NOT NULL, merge_metric_version TEXT NOT NULL, merge_threshold_value REAL NOT NULL,"
        "feature_toggles TEXT NOT NULL);"
        "CREATE TABLE IF NOT EXISTS profile_build_inputs (input_path TEXT PRIMARY KEY, input_row_count INTEGER NOT NULL);"
        "CREATE TABLE IF NOT EXISTS move_pressure_profiles ("
        "profile_id TEXT PRIMARY KEY, profile_version TEXT NOT NULL, support_count INTEGER NOT NULL,"
        "source_month_range TEXT NOT NULL, time_control_family TEXT NOT NULL, elo_family TEXT NOT NULL,"
        "pressure_sensitivity REAL NOT NULL, decisiveness REAL NOT NULL, move_diversity REAL NOT NULL,"
        "fit_mae REAL NOT NULL, merge_provenance TEXT NOT NULL);"
        "CREATE TABLE IF NOT EXISTS think_time_profiles ("
        "profile_id TEXT PRIMARY KEY, profile_version TEXT NOT NULL, support_count INTEGER NOT NULL,"
        "source_month_range TEXT NOT NULL, base_time_scale REAL NOT NULL, spread REAL NOT NULL,"
        "short_mass REAL NOT NULL, deep_think_tail_mass REAL NOT NULL, timeout_tail_mass REAL NOT NULL,"
        "fit_mae REAL NOT NULL, merge_provenance TEXT NOT NULL);"
        "CREATE TABLE IF NOT EXISTS context_profile_map ("
        "time_control_id TEXT NOT NULL, mover_elo_band TEXT NOT NULL, clock_pressure_bucket TEXT NOT NULL,"
        "prev_opp_think_bucket TEXT NOT NULL, opening_ply_band TEXT NOT NULL, support_count INTEGER NOT NULL,"
        "move_pressure_profile_id TEXT NOT NULL, think_time_profile_id TEXT NOT NULL);"
        "CREATE TABLE IF NOT EXISTS profile_merge_history ("
        "family TEXT NOT NULL, source_context_key TEXT NOT NULL, target_profile_id TEXT NOT NULL, distance REAL NOT NULL,"
        "merge_threshold REAL NOT NULL);"
    );
    if (emit_diagnostics) {
        exec_sql(db, "CREATE TABLE IF NOT EXISTS fit_diagnostics (context_key TEXT PRIMARY KEY, support_count INTEGER NOT NULL, move_fit_mae REAL NOT NULL, think_fit_mae REAL NOT NULL);");
    }
    if (emit_invalid_report) {
        exec_sql(db, "CREATE TABLE IF NOT EXISTS invalid_rows (id INTEGER PRIMARY KEY AUTOINCREMENT, input_path TEXT NOT NULL, reason TEXT NOT NULL, detail TEXT NOT NULL);");
    }
}

BehavioralProfileBuildOptions parse_or_throw(int argc, char** argv) {
    BehavioralProfileBuildOptions opts;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto need = [&](const std::string& flag) {
            if (i + 1 >= argc) throw std::runtime_error("Missing value for " + flag);
            return std::string(argv[++i]);
        };
        if (arg == "--help") { print_behavioral_profile_builder_usage(argc > 0 ? argv[0] : "build-behavioral-profiles"); std::exit(0); }
        if (arg == "--input-extract") { opts.input_extract_paths.emplace_back(need(arg)); continue; }
        if (arg == "--output") { opts.output_path = need(arg); continue; }
        if (arg == "--time-controls") { opts.time_controls.push_back(need(arg)); continue; }
        if (arg == "--elo-bands") { opts.elo_bands.push_back(need(arg)); continue; }
        if (arg == "--month-window") { opts.month_window = need(arg); continue; }
        if (arg == "--max-examples") { opts.max_examples = std::stoi(need(arg)); continue; }
        if (arg == "--overwrite") { opts.overwrite = true; continue; }
        if (arg == "--log-every") { opts.log_every = std::stoi(need(arg)); continue; }
        if (arg == "--seed-context-limit") { opts.seed_context_limit = std::stoi(need(arg)); continue; }
        if (arg == "--min-support") { opts.min_support = std::stoi(need(arg)); continue; }
        if (arg == "--merge-threshold") { opts.merge_threshold = std::stod(need(arg)); continue; }
        if (arg == "--strict") { opts.strict = true; continue; }
        if (arg == "--emit-fit-diagnostics") { opts.emit_fit_diagnostics = true; continue; }
        if (arg == "--emit-invalid-report") { opts.emit_invalid_report = true; continue; }
        throw std::runtime_error("Unknown argument: " + arg);
    }
    if (opts.input_extract_paths.empty()) throw std::runtime_error("--input-extract is required at least once");
    if (opts.output_path.empty()) throw std::runtime_error("--output is required");
    for (const auto& band : opts.elo_bands) {
        if (!parse_elo_range(band).has_value()) throw std::runtime_error("Invalid --elo-bands range '" + band + "'. Expected lo-hi inclusive interval.");
    }
    return opts;
}

}  // namespace

BehavioralProfileBuildOptions parse_behavioral_profile_build_cli(int argc, char** argv) {
    return parse_or_throw(argc, argv);
}

void print_behavioral_profile_builder_usage(const std::string& program_name) {
    std::cout
        << "Usage: " << program_name << " --input-extract <sqlite> [--input-extract ...] --output <sqlite> [options]\n"
        << "Options:\n"
        << "  --input-extract <path>                Behavioral Training Extract SQLite input (repeatable).\n"
        << "  --output <path>                       Behavioral Profile Set SQLite output.\n"
        << "  --time-controls <initial+increment>   Optional include filter; repeatable.\n"
        << "  --elo-bands <lo-hi>                   Optional include filter; repeatable inclusive numeric rating ranges.\n"
        << "  --month-window <YYYY-MM:YYYY-MM>      Optional source month filter.\n"
        << "  --max-examples <n>                    Optional global cap for debugging.\n"
        << "  --overwrite                           Recreate output if it exists.\n"
        << "  --log-every <n>                       Progress cadence.\n"
        << "  --seed-context-limit <n>              Optional deterministic context cap.\n"
        << "  --min-support <n>                     Minimum examples required per context (default: 10).\n"
        << "  --merge-threshold <value>             L1 distance threshold for deterministic merging.\n"
        << "  --strict                              Fail if no contexts survive filters.\n"
        << "  --emit-fit-diagnostics                Emit fit_diagnostics table.\n"
        << "  --emit-invalid-report                 Emit invalid_rows for skipped records.\n";
}

BehavioralProfileBuildCounters build_behavioral_profiles(const BehavioralProfileBuildOptions& options) {
    BehavioralProfileBuildCounters counters;
    auto month_window = parse_month_window(options.month_window);

    if (std::filesystem::exists(options.output_path)) {
        if (!options.overwrite) throw std::runtime_error("Output exists; pass --overwrite.");
        std::filesystem::remove(options.output_path);
    }

    std::set<std::string> allowed_tc(options.time_controls.begin(), options.time_controls.end());
    std::vector<EloRange> allowed_elo_ranges;
    for (const auto& band : options.elo_bands) allowed_elo_ranges.push_back(*parse_elo_range(band));

    std::map<ContextKey, ContextStats> contexts;
    std::map<std::string, int> rows_per_input;
    std::set<std::string> months_seen;

    std::vector<std::filesystem::path> inputs = options.input_extract_paths;
    std::sort(inputs.begin(), inputs.end());

    for (const auto& path : inputs) {
        ++counters.extract_files_loaded;
        sqlite3* in_db = nullptr;
        if (sqlite3_open(path.string().c_str(), &in_db) != SQLITE_OK) throw std::runtime_error("Failed to open input extract: " + path.string());
        std::unique_ptr<sqlite3, decltype(&sqlite3_close)> db_guard(in_db, sqlite3_close);

        const std::string sql =
            "SELECT source_month,time_control_id,mover_elo,mover_elo_band,clock_pressure_bucket,prev_opp_think_bucket,"
            "position_key,move_uci,opening_ply_index,think_time_seconds,mover_clock_before_seconds "
            "FROM move_events ORDER BY source_file,game_id,game_ply_index";
        struct CallbackState {
            const BehavioralProfileBuildOptions* options = nullptr;
            const std::vector<EloRange>* allowed_elo_ranges = nullptr;
            const std::set<std::string>* allowed_tc = nullptr;
            MonthWindow month_window;
            std::map<ContextKey, ContextStats>* contexts = nullptr;
            std::map<std::string, int>* rows_per_input = nullptr;
            std::set<std::string>* months_seen = nullptr;
            BehavioralProfileBuildCounters* counters = nullptr;
            std::string input_path;
        } state{&options, &allowed_elo_ranges, &allowed_tc, month_window, &contexts, &rows_per_input, &months_seen, &counters, path.generic_string()};

        auto callback = [](void* user, int argc, char** argv, char**) -> int {
            if (argc != 11) return 0;
            auto* st = static_cast<CallbackState*>(user);
            ++st->counters->raw_move_events_seen;
            ExtractRow row;
            row.source_month = argv[0] ? argv[0] : "";
            row.time_control_id = argv[1] ? argv[1] : "";
            const int mover_elo = parse_int_or_zero(argv[2]);
            row.mover_elo_band = argv[3] ? argv[3] : "";
            row.clock_pressure_bucket = argv[4] ? argv[4] : "";
            row.prev_opp_think_bucket = argv[5] ? argv[5] : "none";
            row.position_key = argv[6] ? argv[6] : "";
            row.move_uci = argv[7] ? argv[7] : "";
            row.opening_ply_index = parse_int_or_zero(argv[8]);
            row.think_time_seconds = parse_double_or_zero(argv[9]);
            row.mover_clock_before_seconds = parse_double_or_zero(argv[10]);

            if (!st->allowed_tc->empty() && !st->allowed_tc->count(row.time_control_id)) return 0;
            if (!st->allowed_elo_ranges->empty() && !in_elo_ranges(mover_elo, *st->allowed_elo_ranges)) return 0;
            const int ym = parse_yyyy_mm(row.source_month);
            if (ym >= 0 && (ym < st->month_window.start || ym > st->month_window.end)) return 0;

            ContextKey key{row.time_control_id, row.mover_elo_band, row.clock_pressure_bucket, row.prev_opp_think_bucket, opening_ply_band(row.opening_ply_index)};
            auto& ctx = (*st->contexts)[key];
            ctx.key = key;
            ++ctx.support_count;
            ctx.min_month = std::min(ctx.min_month, row.source_month);
            ctx.max_month = std::max(ctx.max_month, row.source_month);
            ++ctx.position_counts[row.position_key];
            ++ctx.position_move_counts[row.position_key + "|" + row.move_uci];
            ctx.think_times.push_back(row.think_time_seconds);
            ctx.clock_before.push_back(row.mover_clock_before_seconds);
            ++(*st->rows_per_input)[st->input_path];
            st->months_seen->insert(row.source_month);
            ++st->counters->training_examples_accepted;
            if (st->options->log_every > 0 && st->counters->training_examples_accepted % st->options->log_every == 0) {
                std::cout << "accepted examples: " << st->counters->training_examples_accepted << "\n";
            }
            if (st->options->max_examples > 0 && st->counters->training_examples_accepted >= st->options->max_examples) return 1;
            return 0;
        };
        char* errmsg = nullptr;
        const int rc = sqlite3_exec(in_db, sql.c_str(), callback, &state, &errmsg);
        if (rc != SQLITE_OK && !(options.max_examples > 0 && counters.training_examples_accepted >= options.max_examples)) {
            std::string err = errmsg ? errmsg : "sqlite3_exec input read failed";
            sqlite3_free(errmsg);
            throw std::runtime_error(err);
        }
        if (errmsg) sqlite3_free(errmsg);
        if (options.max_examples > 0 && counters.training_examples_accepted >= options.max_examples) break;
    }

    std::vector<ContextStats> filtered;
    filtered.reserve(contexts.size());
    for (const auto& [key, stats] : contexts) {
        (void)key;
        if (stats.support_count >= options.min_support) filtered.push_back(stats);
    }
    std::sort(filtered.begin(), filtered.end(), [](const ContextStats& a, const ContextStats& b) { return a.key < b.key; });

    if (options.seed_context_limit > 0 && static_cast<int>(filtered.size()) > options.seed_context_limit) {
        filtered.resize(static_cast<std::size_t>(options.seed_context_limit));
    }
    counters.contexts_fitted = static_cast<int>(filtered.size());
    counters.candidate_profiles_created = counters.contexts_fitted * 2;

    if (filtered.empty() && options.strict) {
        throw std::runtime_error("No contexts survived filtering/min-support constraints.");
    }

    std::vector<MovePressureProfile> move_profiles;
    std::vector<ThinkTimeProfile> think_profiles;
    std::vector<ContextMapping> mappings;
    struct MergeRec { std::string family; std::string context_key; std::string profile_id; double dist; };
    std::vector<MergeRec> merge_history;

    int next_move_id = 1;
    int next_think_id = 1;

    for (const auto& ctx : filtered) {
        const auto move_cand = fit_move_pressure(ctx);
        const auto think_cand = fit_think_time(ctx);

        std::string move_id;
        double move_best_dist = 1e9;
        int move_best_index = -1;
        for (std::size_t i = 0; i < move_profiles.size(); ++i) {
            const double d = distance(move_profiles[i], move_cand);
            if (d < move_best_dist) {
                move_best_dist = d;
                move_best_index = static_cast<int>(i);
            }
        }
        if (move_best_index >= 0 && move_best_dist <= options.merge_threshold) {
            move_id = move_profiles[move_best_index].profile_id;
            apply_weighted_merge(move_profiles[move_best_index], move_cand);
            ++counters.profiles_merged;
            merge_history.push_back({"move_pressure", ctx.key.time_control_id + "|" + ctx.key.mover_elo_band + "|" + ctx.key.clock_pressure_bucket + "|" + ctx.key.prev_opp_think_bucket + "|" + ctx.key.opening_ply_band, move_id, move_best_dist});
        } else {
            MovePressureProfile created = move_cand;
            created.profile_id = "mp_" + std::to_string(next_move_id++);
            move_id = created.profile_id;
            move_profiles.push_back(created);
        }

        std::string think_id;
        double think_best_dist = 1e9;
        int think_best_index = -1;
        for (std::size_t i = 0; i < think_profiles.size(); ++i) {
            const double d = distance(think_profiles[i], think_cand);
            if (d < think_best_dist) {
                think_best_dist = d;
                think_best_index = static_cast<int>(i);
            }
        }
        if (think_best_index >= 0 && think_best_dist <= options.merge_threshold) {
            think_id = think_profiles[think_best_index].profile_id;
            apply_weighted_merge(think_profiles[think_best_index], think_cand);
            ++counters.profiles_merged;
            merge_history.push_back({"think_time", ctx.key.time_control_id + "|" + ctx.key.mover_elo_band + "|" + ctx.key.clock_pressure_bucket + "|" + ctx.key.prev_opp_think_bucket + "|" + ctx.key.opening_ply_band, think_id, think_best_dist});
        } else {
            ThinkTimeProfile created = think_cand;
            created.profile_id = "tt_" + std::to_string(next_think_id++);
            think_id = created.profile_id;
            think_profiles.push_back(created);
        }

        mappings.push_back({ctx.key, move_id, think_id, ctx.support_count});
    }

    counters.final_move_pressure_profiles_emitted = static_cast<int>(move_profiles.size());
    counters.final_think_time_profiles_emitted = static_cast<int>(think_profiles.size());

    sqlite3* out_db = nullptr;
    if (sqlite3_open(options.output_path.string().c_str(), &out_db) != SQLITE_OK) throw std::runtime_error("Failed to open output sqlite");
    std::unique_ptr<sqlite3, decltype(&sqlite3_close)> out_guard(out_db, sqlite3_close);
    init_profile_schema(out_db, options.emit_fit_diagnostics, options.emit_invalid_report);
    exec_sql(out_db, "BEGIN IMMEDIATE TRANSACTION;");

    exec_sql(out_db, "DELETE FROM profile_manifest; DELETE FROM profile_build_inputs; DELETE FROM move_pressure_profiles; DELETE FROM think_time_profiles; DELETE FROM context_profile_map; DELETE FROM profile_merge_history;");

    sqlite3_stmt* stmt = nullptr;
    sqlite3_prepare_v2(out_db, "INSERT INTO profile_manifest(schema_version,builder_version,build_timestamp_utc,extract_source_identities,source_month_window_summary,time_control_normalization_policy_version,rating_policy_version,fitting_method_version,merge_metric_version,merge_threshold_value,feature_toggles) VALUES(?,?,?,?,?,?,?,?,?,?,?);", -1, &stmt, nullptr);
    const std::string source_ids = [&]() {
        std::vector<std::string> tmp;
        for (const auto& p : inputs) tmp.push_back(p.generic_string());
        std::sort(tmp.begin(), tmp.end());
        return join_set(std::set<std::string>(tmp.begin(), tmp.end()));
    }();
    const std::string month_summary = months_seen.empty() ? "none" : (*months_seen.begin() + std::string("..") + *months_seen.rbegin());
    const std::string toggles = std::string("emit_fit_diagnostics=") + (options.emit_fit_diagnostics ? "1" : "0") + "|emit_invalid_report=" + (options.emit_invalid_report ? "1" : "0");
    sqlite3_bind_text(stmt, 1, kProfileSchemaVersion, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, kBuilderVersion, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, "deterministic", -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 4, source_ids.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 5, month_summary.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 6, "time_control_v1_exact_initial_plus_increment", -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 7, "rating_band_v1_200", -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 8, kFitMethodVersion, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 9, kMergeMetricVersion, -1, SQLITE_STATIC);
    sqlite3_bind_double(stmt, 10, options.merge_threshold);
    sqlite3_bind_text(stmt, 11, toggles.c_str(), -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) != SQLITE_DONE) throw std::runtime_error("profile_manifest insert failed");
    sqlite3_finalize(stmt);

    sqlite3_prepare_v2(out_db, "INSERT INTO profile_build_inputs(input_path,input_row_count) VALUES(?,?);", -1, &stmt, nullptr);
    for (const auto& [path, count] : rows_per_input) {
        sqlite3_reset(stmt);
        sqlite3_bind_text(stmt, 1, path.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(stmt, 2, count);
        if (sqlite3_step(stmt) != SQLITE_DONE) throw std::runtime_error("profile_build_inputs insert failed");
    }
    sqlite3_finalize(stmt);

    sqlite3_prepare_v2(out_db, "INSERT INTO move_pressure_profiles(profile_id,profile_version,support_count,source_month_range,time_control_family,elo_family,pressure_sensitivity,decisiveness,move_diversity,fit_mae,merge_provenance) VALUES(?,?,?,?,?,?,?,?,?,?,?);", -1, &stmt, nullptr);
    for (const auto& p : move_profiles) {
        sqlite3_reset(stmt);
        sqlite3_bind_text(stmt, 1, p.profile_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, "1", -1, SQLITE_STATIC);
        sqlite3_bind_int(stmt, 3, p.support_count);
        const std::string month_range = p.source_month_start + ".." + p.source_month_end;
        sqlite3_bind_text(stmt, 4, month_range.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 5, p.time_control_family.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 6, p.elo_family.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_double(stmt, 7, p.pressure_sensitivity);
        sqlite3_bind_double(stmt, 8, p.decisiveness);
        sqlite3_bind_double(stmt, 9, p.move_diversity);
        sqlite3_bind_double(stmt, 10, p.fit_mae);
        const std::string provenance = "merged_contexts=" + std::to_string(p.merged_contexts);
        sqlite3_bind_text(stmt, 11, provenance.c_str(), -1, SQLITE_TRANSIENT);
        if (sqlite3_step(stmt) != SQLITE_DONE) throw std::runtime_error("move_pressure_profiles insert failed");
    }
    sqlite3_finalize(stmt);

    sqlite3_prepare_v2(out_db, "INSERT INTO think_time_profiles(profile_id,profile_version,support_count,source_month_range,base_time_scale,spread,short_mass,deep_think_tail_mass,timeout_tail_mass,fit_mae,merge_provenance) VALUES(?,?,?,?,?,?,?,?,?,?,?);", -1, &stmt, nullptr);
    for (const auto& p : think_profiles) {
        sqlite3_reset(stmt);
        sqlite3_bind_text(stmt, 1, p.profile_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, "1", -1, SQLITE_STATIC);
        sqlite3_bind_int(stmt, 3, p.support_count);
        const std::string month_range = p.source_month_start + ".." + p.source_month_end;
        sqlite3_bind_text(stmt, 4, month_range.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_double(stmt, 5, p.base_time_scale);
        sqlite3_bind_double(stmt, 6, p.spread);
        sqlite3_bind_double(stmt, 7, p.short_mass);
        sqlite3_bind_double(stmt, 8, p.deep_think_tail_mass);
        sqlite3_bind_double(stmt, 9, p.timeout_tail_mass);
        sqlite3_bind_double(stmt, 10, p.fit_mae);
        const std::string provenance = "merged_contexts=" + std::to_string(p.merged_contexts);
        sqlite3_bind_text(stmt, 11, provenance.c_str(), -1, SQLITE_TRANSIENT);
        if (sqlite3_step(stmt) != SQLITE_DONE) throw std::runtime_error("think_time_profiles insert failed");
    }
    sqlite3_finalize(stmt);

    sqlite3_prepare_v2(out_db, "INSERT INTO context_profile_map(time_control_id,mover_elo_band,clock_pressure_bucket,prev_opp_think_bucket,opening_ply_band,support_count,move_pressure_profile_id,think_time_profile_id) VALUES(?,?,?,?,?,?,?,?);", -1, &stmt, nullptr);
    for (const auto& m : mappings) {
        sqlite3_reset(stmt);
        sqlite3_bind_text(stmt, 1, m.key.time_control_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, m.key.mover_elo_band.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 3, m.key.clock_pressure_bucket.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 4, m.key.prev_opp_think_bucket.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 5, m.key.opening_ply_band.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(stmt, 6, m.support_count);
        sqlite3_bind_text(stmt, 7, m.move_pressure_profile_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 8, m.think_time_profile_id.c_str(), -1, SQLITE_TRANSIENT);
        if (sqlite3_step(stmt) != SQLITE_DONE) throw std::runtime_error("context_profile_map insert failed");
    }
    sqlite3_finalize(stmt);

    sqlite3_prepare_v2(out_db, "INSERT INTO profile_merge_history(family,source_context_key,target_profile_id,distance,merge_threshold) VALUES(?,?,?,?,?);", -1, &stmt, nullptr);
    for (const auto& m : merge_history) {
        sqlite3_reset(stmt);
        sqlite3_bind_text(stmt, 1, m.family.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, m.context_key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 3, m.profile_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_double(stmt, 4, m.dist);
        sqlite3_bind_double(stmt, 5, options.merge_threshold);
        if (sqlite3_step(stmt) != SQLITE_DONE) throw std::runtime_error("profile_merge_history insert failed");
    }
    sqlite3_finalize(stmt);

    if (options.emit_fit_diagnostics) {
        sqlite3_prepare_v2(out_db, "INSERT INTO fit_diagnostics(context_key,support_count,move_fit_mae,think_fit_mae) VALUES(?,?,?,?);", -1, &stmt, nullptr);
        for (const auto& ctx : filtered) {
            const auto mv = fit_move_pressure(ctx);
            const auto tt = fit_think_time(ctx);
            const std::string key = ctx.key.time_control_id + "|" + ctx.key.mover_elo_band + "|" + ctx.key.clock_pressure_bucket + "|" + ctx.key.prev_opp_think_bucket + "|" + ctx.key.opening_ply_band;
            sqlite3_reset(stmt);
            sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(stmt, 2, ctx.support_count);
            sqlite3_bind_double(stmt, 3, mv.fit_mae);
            sqlite3_bind_double(stmt, 4, tt.fit_mae);
            if (sqlite3_step(stmt) != SQLITE_DONE) throw std::runtime_error("fit_diagnostics insert failed");
        }
        sqlite3_finalize(stmt);
    }

    exec_sql(out_db, "COMMIT;");
    return counters;
}

}  // namespace otcb
