#include "otcb/seeded_practical_risk_probe.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "otcb/chess_board.hpp"
#include "otcb/chess_types.hpp"
#include "otcb/position_key.hpp"
#include "otcb/progress.hpp"
#include "otcb/rating_filter.hpp"
#include "otcb/san_replay.hpp"

namespace otcb {
namespace {

std::string trim(const std::string& in) {
    std::size_t s = 0;
    while (s < in.size() && std::isspace(static_cast<unsigned char>(in[s]))) ++s;
    std::size_t e = in.size();
    while (e > s && std::isspace(static_cast<unsigned char>(in[e - 1]))) --e;
    return in.substr(s, e - s);
}

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

bool is_move_number(const std::string& token) {
    if (token.empty()) return false;
    std::size_t i = 0;
    while (i < token.size() && std::isdigit(static_cast<unsigned char>(token[i]))) ++i;
    if (i == 0) return false;
    while (i < token.size() && token[i] == '.') ++i;
    return i == token.size();
}

bool is_result_token(const std::string& token) {
    return token == "1-0" || token == "0-1" || token == "1/2-1/2" || token == "*";
}

std::vector<std::string> tokenize_movetext(const std::string& movetext) {
    std::vector<std::string> out;
    std::string cur;
    bool in_comment = false;
    int variation_depth = 0;

    auto flush = [&]() {
        if (cur.empty() || variation_depth > 0) {
            cur.clear();
            return;
        }
        if (is_move_number(cur) || cur.front() == '$' || is_result_token(cur)) {
            cur.clear();
            return;
        }
        while (!cur.empty() && (cur.back() == '!' || cur.back() == '?')) cur.pop_back();
        if (!cur.empty()) out.push_back(cur);
        cur.clear();
    };

    for (char ch : movetext) {
        if (in_comment) {
            if (ch == '}') in_comment = false;
            continue;
        }
        if (ch == '{') {
            flush();
            in_comment = true;
            continue;
        }
        if (ch == '(') {
            flush();
            ++variation_depth;
            continue;
        }
        if (ch == ')') {
            flush();
            if (variation_depth > 0) --variation_depth;
            continue;
        }
        if (std::isspace(static_cast<unsigned char>(ch))) {
            flush();
            continue;
        }
        if (variation_depth == 0) cur.push_back(ch);
    }
    flush();
    return out;
}

struct NodeStats {
    int visits = 0;
    double score_sum = 0.0;
    double score_sq_sum = 0.0;
    std::map<std::string, int> child_counts;
};

struct EntryStats {
    int support = 0;
    NodeStats root_node;
    std::map<std::string, int> first_reply_counts;
    std::map<std::string, NodeStats> nodes;
};

struct ProbeStats {
    SeededPracticalRiskProbeDefinition def;
    int games_reaching_probe_position = 0;
    std::map<std::string, int> all_observed_entry_moves;
    std::map<std::string, EntryStats> entries;
    int result_count = 0;
    double result_sum = 0.0;
};

void update_node(NodeStats& stats, double score) {
    ++stats.visits;
    stats.score_sum += score;
    stats.score_sq_sum += score * score;
}

std::pair<double, double> mean_sigma(const NodeStats& stats) {
    if (stats.visits <= 0) return {0.0, 0.0};
    const double mu = stats.score_sum / static_cast<double>(stats.visits);
    const double ex2 = stats.score_sq_sum / static_cast<double>(stats.visits);
    const double var = std::max(0.0, ex2 - mu * mu);
    return {mu, std::sqrt(var)};
}

struct EvalResult {
    double mu = 0.0;
    double sigma = 0.0;
    std::vector<std::string> chosen_line;
};

Color parse_side(const std::string& side_text) {
    if (side_text == "white") return Color::White;
    if (side_text == "black") return Color::Black;
    throw std::runtime_error("invalid side_to_optimize '" + side_text + "' expected white|black");
}

Color side_to_move_from_position_key(const std::string& key) {
    const auto first_space = key.find(' ');
    const auto second_space = key.find(' ', first_space == std::string::npos ? 0 : first_space + 1);
    if (first_space == std::string::npos || second_space == std::string::npos || second_space <= first_space + 1) {
        throw std::runtime_error("invalid position_key for side-to-move extraction: " + key);
    }
    const char stm = key[first_space + 1];
    if (stm == 'w') return Color::White;
    if (stm == 'b') return Color::Black;
    throw std::runtime_error("invalid side-to-move in position_key: " + key);
}

EvalResult evaluate_node(const EntryStats& entry,
                         const std::string& path,
                         int depth,
                         int horizon,
                         Color node_side,
                         Color optimize_side,
                         int self_move_min_support,
                         int reply_min_support,
                         double prior_weight,
                         double global_mean) {
    auto node_it = entry.nodes.find(path);
    if (node_it == entry.nodes.end()) {
        return {global_mean, 0.0, {}};
    }
    const NodeStats& node = node_it->second;
    const auto [emp_mu, emp_sigma] = mean_sigma(node);
    if (depth >= horizon || node.child_counts.empty()) {
        return {emp_mu, emp_sigma, {}};
    }

    struct ChildEval { std::string move; int count; EvalResult eval; double shrunk_mu; };
    std::vector<ChildEval> children;
    for (const auto& [move, count] : node.child_counts) {
        const std::string child_path = path.empty() ? move : path + " " + move;
        EvalResult child_eval = evaluate_node(entry, child_path, depth + 1, horizon, node_side == Color::White ? Color::Black : Color::White,
                                              optimize_side, self_move_min_support, reply_min_support, prior_weight, global_mean);
        const double shrunk_mu = (child_eval.mu * count + prior_weight * global_mean) / (static_cast<double>(count) + prior_weight);
        children.push_back({move, count, child_eval, shrunk_mu});
    }

    std::sort(children.begin(), children.end(), [](const ChildEval& a, const ChildEval& b) {
        if (a.move != b.move) return a.move < b.move;
        return a.count < b.count;
    });

    if (node_side == optimize_side) {
        std::vector<const ChildEval*> viable;
        for (const auto& child : children) {
            if (child.count >= self_move_min_support) viable.push_back(&child);
        }
        if (viable.empty()) {
            for (const auto& child : children) viable.push_back(&child);
        }
        const ChildEval* best = viable.front();
        for (const ChildEval* candidate : viable) {
            if (candidate->shrunk_mu > best->shrunk_mu || (candidate->shrunk_mu == best->shrunk_mu && candidate->move < best->move)) {
                best = candidate;
            }
        }
        EvalResult result = best->eval;
        result.chosen_line.insert(result.chosen_line.begin(), best->move);
        return result;
    }

    std::vector<const ChildEval*> replies;
    for (const auto& child : children) {
        if (child.count >= reply_min_support) replies.push_back(&child);
    }
    if (replies.empty()) {
        for (const auto& child : children) replies.push_back(&child);
    }
    double total = 0.0;
    for (const ChildEval* child : replies) total += static_cast<double>(child->count);
    double mu = 0.0;
    double second = 0.0;
    for (const ChildEval* child : replies) {
        const double p = static_cast<double>(child->count) / total;
        mu += p * child->eval.mu;
        second += p * (child->eval.sigma * child->eval.sigma + child->eval.mu * child->eval.mu);
    }
    const double var = std::max(0.0, second - mu * mu);
    return {mu, std::sqrt(var), {}};
}

std::string to_lower_copy(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return value;
}

std::optional<int> parse_int(const std::string& value) {
    if (value.empty()) return std::nullopt;
    try {
        size_t p = 0;
        int out = std::stoi(value, &p);
        if (p != value.size()) return std::nullopt;
        return out;
    } catch (...) {
        return std::nullopt;
    }
}

struct JsonValue {
    enum class Kind { Null, Bool, Number, String, Array, Object };
    Kind kind = Kind::Null;
    bool bool_value = false;
    double number_value = 0.0;
    std::string string_value;
    std::vector<JsonValue> array_value;
    std::map<std::string, JsonValue> object_value;
};

class JsonParser {
public:
    explicit JsonParser(std::string text) : text_(std::move(text)) {}

    JsonValue parse() {
        skip_ws();
        JsonValue v = parse_value();
        skip_ws();
        if (pos_ != text_.size()) throw std::runtime_error("unexpected trailing JSON content");
        return v;
    }

private:
    JsonValue parse_value() {
        if (peek('{')) return parse_object();
        if (peek('[')) return parse_array();
        if (peek('"')) return parse_string();
        if (starts("true")) {
            pos_ += 4;
            JsonValue v;
            v.kind = JsonValue::Kind::Bool;
            v.bool_value = true;
            return v;
        }
        if (starts("false")) {
            pos_ += 5;
            JsonValue v;
            v.kind = JsonValue::Kind::Bool;
            v.bool_value = false;
            return v;
        }
        if (starts("null")) {
            pos_ += 4;
            JsonValue v;
            v.kind = JsonValue::Kind::Null;
            return v;
        }
        return parse_number();
    }

    JsonValue parse_object() {
        expect('{');
        JsonValue obj;
        obj.kind = JsonValue::Kind::Object;
        skip_ws();
        if (peek('}')) {
            ++pos_;
            return obj;
        }
        while (true) {
            JsonValue key = parse_string();
            skip_ws();
            expect(':');
            skip_ws();
            obj.object_value.emplace(key.string_value, parse_value());
            skip_ws();
            if (peek('}')) {
                ++pos_;
                break;
            }
            expect(',');
            skip_ws();
        }
        return obj;
    }

    JsonValue parse_array() {
        expect('[');
        JsonValue arr;
        arr.kind = JsonValue::Kind::Array;
        skip_ws();
        if (peek(']')) {
            ++pos_;
            return arr;
        }
        while (true) {
            arr.array_value.push_back(parse_value());
            skip_ws();
            if (peek(']')) {
                ++pos_;
                break;
            }
            expect(',');
            skip_ws();
        }
        return arr;
    }

    JsonValue parse_string() {
        expect('"');
        JsonValue str;
        str.kind = JsonValue::Kind::String;
        while (pos_ < text_.size()) {
            char ch = text_[pos_++];
            if (ch == '"') break;
            if (ch == '\\') {
                if (pos_ >= text_.size()) throw std::runtime_error("bad json escape");
                const char esc = text_[pos_++];
                switch (esc) {
                    case '"': str.string_value.push_back('"'); break;
                    case '\\': str.string_value.push_back('\\'); break;
                    case '/': str.string_value.push_back('/'); break;
                    case 'b': str.string_value.push_back('\b'); break;
                    case 'f': str.string_value.push_back('\f'); break;
                    case 'n': str.string_value.push_back('\n'); break;
                    case 'r': str.string_value.push_back('\r'); break;
                    case 't': str.string_value.push_back('\t'); break;
                    default: throw std::runtime_error("unsupported json escape");
                }
            } else {
                str.string_value.push_back(ch);
            }
        }
        return str;
    }

    JsonValue parse_number() {
        std::size_t start = pos_;
        if (peek('-')) ++pos_;
        while (pos_ < text_.size() && std::isdigit(static_cast<unsigned char>(text_[pos_]))) ++pos_;
        if (peek('.')) {
            ++pos_;
            while (pos_ < text_.size() && std::isdigit(static_cast<unsigned char>(text_[pos_]))) ++pos_;
        }
        if (peek('e') || peek('E')) {
            ++pos_;
            if (peek('+') || peek('-')) ++pos_;
            while (pos_ < text_.size() && std::isdigit(static_cast<unsigned char>(text_[pos_]))) ++pos_;
        }
        if (start == pos_) throw std::runtime_error("expected json value");
        JsonValue number;
        number.kind = JsonValue::Kind::Number;
        number.number_value = std::stod(text_.substr(start, pos_ - start));
        return number;
    }

    bool peek(char ch) const { return pos_ < text_.size() && text_[pos_] == ch; }
    bool starts(const char* s) const { return text_.compare(pos_, std::strlen(s), s) == 0; }
    void expect(char ch) { if (!peek(ch)) throw std::runtime_error(std::string("expected '") + ch + "'"); ++pos_; }
    void skip_ws() { while (pos_ < text_.size() && std::isspace(static_cast<unsigned char>(text_[pos_]))) ++pos_; }

    std::string text_;
    std::size_t pos_ = 0;
};

const JsonValue& expect_member(const JsonValue& obj, const std::string& key) {
    const auto it = obj.object_value.find(key);
    if (it == obj.object_value.end()) throw std::runtime_error("probe-spec missing required key: " + key);
    return it->second;
}

std::string require_string(const JsonValue& v, const std::string& field) {
    if (v.kind != JsonValue::Kind::String) throw std::runtime_error(field + " must be string");
    return v.string_value;
}

int require_int(const JsonValue& v, const std::string& field) {
    if (v.kind != JsonValue::Kind::Number) throw std::runtime_error(field + " must be number");
    return static_cast<int>(std::llround(v.number_value));
}

double require_double(const JsonValue& v, const std::string& field) {
    if (v.kind != JsonValue::Kind::Number) throw std::runtime_error(field + " must be number");
    return v.number_value;
}

std::vector<std::string> require_string_array(const JsonValue& v, const std::string& field) {
    if (v.kind != JsonValue::Kind::Array) throw std::runtime_error(field + " must be array");
    std::vector<std::string> values;
    for (const auto& item : v.array_value) values.push_back(require_string(item, field + "[]"));
    return values;
}

std::vector<SeededPracticalRiskProbeDefinition> load_probe_spec(const std::filesystem::path& path) {
    std::ifstream in(path);
    if (!in) throw std::runtime_error("failed to open probe-spec: " + path.string());
    std::ostringstream buf;
    buf << in.rdbuf();
    JsonParser parser(buf.str());
    JsonValue root = parser.parse();
    if (root.kind != JsonValue::Kind::Object) throw std::runtime_error("probe-spec root must be object");
    const std::string probes_key = "probes";
    const JsonValue& probes = expect_member(root, probes_key);
    if (probes.kind != JsonValue::Kind::Array) throw std::runtime_error("probe-spec probes must be array");

    std::vector<SeededPracticalRiskProbeDefinition> defs;
    for (const auto& item : probes.array_value) {
        if (item.kind != JsonValue::Kind::Object) throw std::runtime_error("probe entry must be object");
        SeededPracticalRiskProbeDefinition def;
        def.probe_id = require_string(expect_member(item, "probe_id"), "probe_id");
        def.position_key = require_string(expect_member(item, "position_key"), "position_key");
        def.side_to_optimize = to_lower_copy(require_string(expect_member(item, "side_to_optimize"), "side_to_optimize"));
        def.candidate_moves = require_string_array(expect_member(item, "candidate_moves"), "candidate_moves");
        def.baseline_moves = require_string_array(expect_member(item, "baseline_moves"), "baseline_moves");
        def.horizon_plies = require_int(expect_member(item, "horizon_plies"), "horizon_plies");
        const auto search_it = item.object_value.find("search_max_ply");
        def.search_max_ply = search_it == item.object_value.end() ? 30 : require_int(search_it->second, "search_max_ply");
        def.entry_min_support = require_int(expect_member(item, "entry_min_support"), "entry_min_support");
        def.self_move_min_support = require_int(expect_member(item, "self_move_min_support"), "self_move_min_support");
        def.reply_min_support = require_int(expect_member(item, "reply_min_support"), "reply_min_support");
        def.prior_weight = require_double(expect_member(item, "prior_weight"), "prior_weight");
        def.delta_max = require_double(expect_member(item, "delta_max"), "delta_max");
        defs.push_back(std::move(def));
    }
    if (defs.empty()) throw std::runtime_error("probe-spec must contain at least one probe");
    return defs;
}

struct ParsedGame {
    std::map<std::string, std::string> tags;
    std::string movetext;
};

template <typename Callback>
void stream_games(const std::filesystem::path& input, ProgressReporter& progress, Callback&& callback) {
    std::ifstream in(input, std::ios::binary);
    if (!in) throw std::runtime_error("failed to open input pgn: " + input.string());
    ParsedGame current;
    bool in_moves = false;
    std::string line;
    std::uint64_t bytes = 0;
    while (std::getline(in, line)) {
        bytes += static_cast<std::uint64_t>(line.size() + 1);
        progress.update([&](ProgressSnapshot& s) { s.source_bytes_scanned = bytes; });
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (!line.empty() && line.front() == '[') {
            in_moves = false;
            const auto sp = line.find(' ');
            if (sp != std::string::npos && line.back() == ']' && sp + 2 < line.size() && line[sp + 1] == '"' && line[line.size() - 2] == '"') {
                current.tags[line.substr(1, sp - 1)] = line.substr(sp + 2, line.size() - sp - 4);
            }
            continue;
        }
        if (trim(line).empty()) {
            if (!current.movetext.empty() && !current.tags.empty()) {
                callback(current);
                current = ParsedGame{};
            }
            in_moves = true;
            continue;
        }
        if (in_moves || !current.tags.empty()) {
            if (!current.movetext.empty()) current.movetext.push_back(' ');
            current.movetext += trim(line);
            in_moves = true;
        }
    }
    if (!current.movetext.empty() && !current.tags.empty()) {
        callback(current);
    }
}

double score_for_side(const std::string& result, Color optimize_side) {
    if (result == "1/2-1/2") return 0.5;
    if (result == "1-0") return optimize_side == Color::White ? 1.0 : 0.0;
    if (result == "0-1") return optimize_side == Color::Black ? 1.0 : 0.0;
    return 0.5;
}

}  // namespace

void print_seeded_practical_risk_probe_usage(const std::string& program_name) {
    std::cout
        << "Usage: " << program_name << " --input-pgn <path> --output-dir <dir> --artifact-id <id> --probe-spec <json>\\n"
        << "       --min-rating <n> --max-rating <n> --rating-policy <both_in_band|average_in_band|white_in_band|black_in_band>\\n"
        << "       --time-controls <tc1,tc2> --time-control-id <id> --initial-time-seconds <sec> --increment-seconds <sec> --time-format-label <label>\\n";
}

SeededPracticalRiskProbeOptions parse_seeded_practical_risk_probe_cli(int argc, char** argv) {
    SeededPracticalRiskProbeOptions out;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto require_value = [&](const char* flag) -> std::string {
            if (i + 1 >= argc) throw std::runtime_error(std::string("missing value for ") + flag);
            return argv[++i];
        };
        if (arg == "--input-pgn") out.input_pgn = require_value("--input-pgn");
        else if (arg == "--output-dir") out.output_dir = require_value("--output-dir");
        else if (arg == "--artifact-id") out.artifact_id = require_value("--artifact-id");
        else if (arg == "--min-rating") out.min_rating = std::stoi(require_value("--min-rating"));
        else if (arg == "--max-rating") out.max_rating = std::stoi(require_value("--max-rating"));
        else if (arg == "--rating-policy") {
            const auto parsed = parse_rating_policy(require_value("--rating-policy"));
            if (!parsed.has_value()) throw std::runtime_error("invalid --rating-policy");
            out.rating_policy = *parsed;
        } else if (arg == "--time-controls") {
            std::stringstream ss(require_value("--time-controls"));
            std::string part;
            while (std::getline(ss, part, ',')) {
                part = trim(part);
                if (!part.empty()) out.time_controls.push_back(part);
            }
        } else if (arg == "--time-control-id") out.time_control_id = require_value("--time-control-id");
        else if (arg == "--initial-time-seconds") out.initial_time_seconds = std::stoi(require_value("--initial-time-seconds"));
        else if (arg == "--increment-seconds") out.increment_seconds = std::stoi(require_value("--increment-seconds"));
        else if (arg == "--time-format-label") out.time_format_label = require_value("--time-format-label");
        else if (arg == "--probe-spec") out.probe_spec = require_value("--probe-spec");
        else if (arg == "--emit-progress-log") out.emit_progress_log = true;
        else if (arg == "--emit-status-json") out.emit_status_json = true;
        else if (arg == "--quiet-progress") out.quiet_progress = true;
        else if (arg == "--heartbeat-seconds") out.heartbeat_seconds = std::stoi(require_value("--heartbeat-seconds"));
        else if (arg == "--help" || arg == "-h") {
            print_seeded_practical_risk_probe_usage(argv[0]);
            std::exit(0);
        } else {
            throw std::runtime_error("unknown argument: " + arg);
        }
    }
    if (out.input_pgn.empty() || out.output_dir.empty() || out.artifact_id.empty() || out.probe_spec.empty()) {
        throw std::runtime_error("missing required arguments --input-pgn --output-dir --artifact-id --probe-spec");
    }
    if (out.min_rating > out.max_rating) throw std::runtime_error("--min-rating must be <= --max-rating");
    if (out.time_controls.empty()) throw std::runtime_error("--time-controls is required");
    if (out.time_control_id.empty()) throw std::runtime_error("--time-control-id is required");
    return out;
}

int run_seeded_practical_risk_probe(const SeededPracticalRiskProbeOptions& options) {
    const std::filesystem::path bundle_dir = options.output_dir / options.artifact_id;
    std::filesystem::create_directories(bundle_dir);

    ProgressReporter progress(ProgressReporterOptions{options.quiet_progress, options.emit_progress_log, options.emit_status_json, options.heartbeat_seconds, bundle_dir});
    progress.start();

    progress.stage_started(ProgressStage::Preflight, "loading probe spec");
    const auto probe_defs = load_probe_spec(options.probe_spec);
    progress.stage_completed("probe spec loaded");

    std::map<std::string, std::vector<std::size_t>> probes_by_position;
    std::vector<ProbeStats> probe_stats;
    int max_search_ply = 0;
    for (std::size_t i = 0; i < probe_defs.size(); ++i) {
        probes_by_position[probe_defs[i].position_key].push_back(i);
        ProbeStats stats;
        stats.def = probe_defs[i];
        probe_stats.push_back(std::move(stats));
        max_search_ply = std::max(max_search_ply, probe_defs[i].search_max_ply);
    }

    const auto source_size = std::filesystem::exists(options.input_pgn) ? std::optional<std::uint64_t>(std::filesystem::file_size(options.input_pgn)) : std::nullopt;
    progress.stage_started(ProgressStage::SeedProbeScan, "seed probe scan", source_size);
    std::set<std::string> tc_allowed(options.time_controls.begin(), options.time_controls.end());
    int games_seen = 0;
    int games_scope = 0;
    int games_probe_hits = 0;
    stream_games(options.input_pgn, progress, [&](const ParsedGame& game) {
        ++games_seen;
        progress.update([&](ProgressSnapshot& s) { s.games_scanned = games_seen; });

        const auto welo_it = game.tags.find("WhiteElo");
        const auto belo_it = game.tags.find("BlackElo");
        const auto tc_it = game.tags.find("TimeControl");
        const auto result_it = game.tags.find("Result");
        if (welo_it == game.tags.end() || belo_it == game.tags.end() || tc_it == game.tags.end() || result_it == game.tags.end()  ) return;
        const auto welo = parse_int(welo_it->second);
        const auto belo = parse_int(belo_it->second);
        if (!welo.has_value() || !belo.has_value()  ) return;
        if (!rating_policy_match(*welo, *belo, options.rating_policy, {EloRange{options.min_rating, options.max_rating}})  ) return;
        if (tc_allowed.find(tc_it->second) == tc_allowed.end()  ) return;

        ++games_scope;
        progress.update([&](ProgressSnapshot& s) { s.games_accepted = games_scope; });

        const auto san_moves = tokenize_movetext(game.movetext);
        ChessBoard board;
            bool any_hit_game = false;
            for (std::size_t ply = 0; ply < san_moves.size(); ++ply) {
            if (static_cast<int>(ply) > max_search_ply) break;
            const std::string position_key = make_position_key(board, PositionKeyFormat::FenNormalized);
            const auto probe_it = probes_by_position.find(position_key);
            if (probe_it != probes_by_position.end()) {
                for (const std::size_t probe_index : probe_it->second) {
                    ProbeStats& stats = probe_stats[probe_index];
                    if (static_cast<int>(ply) > stats.def.search_max_ply) continue;
                    if (ply >= san_moves.size()) continue;
                    const std::string entry_san = san_moves[ply];
                    const auto resolved_entry = resolve_san_move(board, entry_san);
                    if (!resolved_entry.success || !resolved_entry.move.has_value()) continue;
                    const std::string entry_uci = move_to_uci(*resolved_entry.move);
                    const Color optimize_side = parse_side(stats.def.side_to_optimize);
                    const double score = score_for_side(result_it->second, optimize_side);
                    any_hit_game = true;
                    ++stats.games_reaching_probe_position;
                    ++stats.result_count;
                    stats.result_sum += score;
                    ++stats.all_observed_entry_moves[entry_uci];
                    EntryStats& entry_stats = stats.entries[entry_uci];
                    ++entry_stats.support;

                    board.apply_move(*resolved_entry.move);
                    if (ply + 1 < san_moves.size()) {
                        const auto first_reply = resolve_san_move(board, san_moves[ply + 1]);
                        if (first_reply.success && first_reply.move.has_value()) {
                            ++entry_stats.first_reply_counts[move_to_uci(*first_reply.move)];
                        }
                    }

                    update_node(entry_stats.root_node, score);
                    update_node(entry_stats.nodes[""], score);
                    std::string path;
                    ChessBoard temp_board = board;
                    for (int d = 0; d < stats.def.horizon_plies; ++d) {
                        const std::size_t move_index = ply + 1 + static_cast<std::size_t>(d);
                        if (move_index >= san_moves.size()) break;
                        const auto resolved = resolve_san_move(temp_board, san_moves[move_index]);
                        if (!resolved.success || !resolved.move.has_value()) break;
                        const std::string uci = move_to_uci(*resolved.move);
                        entry_stats.nodes[path].child_counts[uci]++;
                        path = path.empty() ? uci : path + " " + uci;
                        temp_board.apply_move(*resolved.move);
                        update_node(entry_stats.nodes[path], score);
                    }
                }
            }

            const auto resolved = resolve_san_move(board, san_moves[ply]);
            if (!resolved.success || !resolved.move.has_value()) break;
            board.apply_move(*resolved.move);
        }
        if (any_hit_game) ++games_probe_hits;
        progress.update([&](ProgressSnapshot& s) {
            s.games_rejected = games_seen - games_scope;
            s.replay_attempts = games_seen;
            s.replay_successes = games_scope;
            s.replay_failures = games_seen - games_scope;
            s.extracted_plies += static_cast<int>(san_moves.size());
            s.raw_observations = games_probe_hits;
            s.probe_games_in_scope = games_scope;
            s.probe_games_reaching_any = games_probe_hits;
        });
    });
    progress.stage_completed("seed probe scan complete");

    progress.stage_started(ProgressStage::SeedProbeEvaluate, "seed probe evaluate");

    std::ostringstream report;
    report << "{\n  \"artifact_role\": \"seeded_practical_risk_probe\",\n  \"probes\": [\n";
    for (std::size_t i = 0; i < probe_stats.size(); ++i) {
        const ProbeStats& probe = probe_stats[i];
        progress.update([&](ProgressSnapshot& s) {
            s.probe_current_id = probe.def.probe_id;
            s.probe_entries_evaluated = static_cast<int>(i);
        });
        progress.note_event("evaluating " + probe.def.probe_id);
        const Color optimize_side = parse_side(probe.def.side_to_optimize);
        const Color entry_side = side_to_move_from_position_key(probe.def.position_key);
        const Color post_entry_side = entry_side == Color::White ? Color::Black : Color::White;
        const double global_mean = probe.result_count > 0 ? probe.result_sum / static_cast<double>(probe.result_count) : 0.5;

        report << "    {\n";
        report << "      \"probe_id\": \"" << json_escape(probe.def.probe_id) << "\",\n";
        report << "      \"position_key\": \"" << json_escape(probe.def.position_key) << "\",\n";
        report << "      \"side_to_optimize\": \"" << json_escape(probe.def.side_to_optimize) << "\",\n";
        report << "      \"games_reaching_probe_position\": " << probe.games_reaching_probe_position << ",\n";

        auto write_entry_list = [&](const std::vector<std::string>& entries, const char* key_name) {
            report << "      \"" << key_name << "\": [\n";
            for (std::size_t idx = 0; idx < entries.size(); ++idx) {
                const std::string& move = entries[idx];
                auto stat_it = probe.entries.find(move);
                const EntryStats empty;
                const EntryStats& es = stat_it == probe.entries.end() ? empty : stat_it->second;
                const auto [emp_mu, emp_sigma] = mean_sigma(es.root_node);
                EvalResult eval = evaluate_node(es, "", 0, probe.def.horizon_plies, post_entry_side, optimize_side,
                                                probe.def.self_move_min_support, probe.def.reply_min_support,
                                                probe.def.prior_weight, global_mean);
                report << "        {\n";
                report << "          \"entry_move\": \"" << move << "\",\n";
                report << "          \"support_count\": " << es.support << ",\n";
                report << "          \"empirical_root_mean\": " << std::fixed << std::setprecision(6) << emp_mu << ",\n";
                report << "          \"empirical_root_sigma\": " << std::fixed << std::setprecision(6) << emp_sigma << ",\n";
                report << "          \"recursive_eval_mean\": " << std::fixed << std::setprecision(6) << eval.mu << ",\n";
                report << "          \"recursive_eval_sigma\": " << std::fixed << std::setprecision(6) << eval.sigma << ",\n";
                report << "          \"reply_breadth\": " << es.first_reply_counts.size() << ",\n";
                report << "          \"top_first_replies\": [";
                int k = 0;
                for (auto it = es.first_reply_counts.begin(); it != es.first_reply_counts.end() && k < 5; ++it, ++k) {
                    if (k) report << ", ";
                    report << "{\"move\":\"" << it->first << "\",\"count\":" << it->second << "}";
                }
                report << "],\n";
                report << "          \"chosen_self_continuation\": [";
                for (std::size_t j = 0; j < eval.chosen_line.size(); ++j) {
                    if (j) report << ", ";
                    report << "\"" << eval.chosen_line[j] << "\"";
                }
                report << "]\n";
                report << "        }" << (idx + 1 == entries.size() ? "\n" : ",\n");
            }
            report << "      ]";
        };

        report << "      \"all_observed_entry_moves\": [";
        int c = 0;
        for (const auto& [move, count] : probe.all_observed_entry_moves) {
            if (c++) report << ", ";
            report << "{\"move\":\"" << move << "\",\"count\":" << count << "}";
        }
        report << "],\n";
        write_entry_list(probe.def.candidate_moves, "candidate_entries");
        report << ",\n";
        write_entry_list(probe.def.baseline_moves, "baseline_entries");

        std::optional<std::string> best_candidate;
        EvalResult best_candidate_eval;
        int best_candidate_support = 0;
        for (const auto& move : probe.def.candidate_moves) {
            auto it = probe.entries.find(move);
            if (it == probe.entries.end()) continue;
            EvalResult eval = evaluate_node(it->second, "", 0, probe.def.horizon_plies, post_entry_side, optimize_side,
                                            probe.def.self_move_min_support, probe.def.reply_min_support,
                                            probe.def.prior_weight, global_mean);
            if (!best_candidate.has_value() || eval.mu > best_candidate_eval.mu) {
                best_candidate = move;
                best_candidate_eval = eval;
                best_candidate_support = it->second.support;
            }
        }
        std::optional<std::string> best_baseline;
        EvalResult best_baseline_eval;
        int best_baseline_support = 0;
        for (const auto& move : probe.def.baseline_moves) {
            auto it = probe.entries.find(move);
            if (it == probe.entries.end()) continue;
            EvalResult eval = evaluate_node(it->second, "", 0, probe.def.horizon_plies, post_entry_side, optimize_side,
                                            probe.def.self_move_min_support, probe.def.reply_min_support,
                                            probe.def.prior_weight, global_mean);
            if (!best_baseline.has_value() || eval.mu > best_baseline_eval.mu) {
                best_baseline = move;
                best_baseline_eval = eval;
                best_baseline_support = it->second.support;
            }
        }

        if (best_candidate.has_value() && best_baseline.has_value() && best_candidate_support >= probe.def.entry_min_support && best_baseline_support >= probe.def.entry_min_support) {
            const double cand_ceiling = best_candidate_eval.mu + best_candidate_eval.sigma;
            const double base_ceiling = best_baseline_eval.mu + best_baseline_eval.sigma;
            const bool mean_ok = best_candidate_eval.mu >= best_baseline_eval.mu - probe.def.delta_max;
            const bool support_ok = best_candidate_support >= probe.def.entry_min_support;
            const bool accepted = mean_ok && support_ok && cand_ceiling >= base_ceiling;
            report << ",\n      \"comparison\": {\n";
            report << "        \"candidate_move\": \"" << *best_candidate << "\",\n";
            report << "        \"baseline_move\": \"" << *best_baseline << "\",\n";
            report << "        \"candidate_mu\": " << best_candidate_eval.mu << ",\n";
            report << "        \"candidate_sigma\": " << best_candidate_eval.sigma << ",\n";
            report << "        \"baseline_mu\": " << best_baseline_eval.mu << ",\n";
            report << "        \"baseline_sigma\": " << best_baseline_eval.sigma << ",\n";
            report << "        \"candidate_ceiling\": " << cand_ceiling << ",\n";
            report << "        \"baseline_ceiling\": " << base_ceiling << ",\n";
            report << "        \"mean_penalty_check\": " << (mean_ok ? "true" : "false") << ",\n";
            report << "        \"entry_support_check\": " << (support_ok ? "true" : "false") << ",\n";
            report << "        \"provisional_accept\": " << (accepted ? "true" : "false") << "\n";
            report << "      }\n";
        } else {
            report << "\n";
        }
        report << "    }" << (i + 1 == probe_stats.size() ? "\n" : ",\n");
    }
    report << "  ]\n}\n";
    progress.stage_completed("seed probe evaluate complete");

    progress.stage_started(ProgressStage::WriteArtifacts, "writing artifacts");
    {
        std::ofstream probe_json(bundle_dir / "probe_report.json");
        probe_json << report.str();

        std::ofstream summary(bundle_dir / "build_summary.txt");
        summary << "seeded practical-risk probe build complete\n";
        summary << "input pgn: " << options.input_pgn.string() << "\n";
        summary << "probe count: " << probe_defs.size() << "\n";
        summary << "stockfish used: false\n";
        summary << "external book dependency used: false\n";

        std::ofstream manifest(bundle_dir / "manifest.json");
        manifest << "{\n";
        manifest << "  \"artifact_id\": \"" << json_escape(options.artifact_id) << "\",\n";
        manifest << "  \"artifact_role\": \"seeded_practical_risk_probe\",\n";
        manifest << "  \"input_pgn_path\": \"" << json_escape(options.input_pgn.string()) << "\",\n";
        manifest << "  \"rating_band\": \"" << options.min_rating << "-" << options.max_rating << "\",\n";
        manifest << "  \"rating_policy\": \"" << to_string(options.rating_policy) << "\",\n";
        manifest << "  \"time_control_id\": \"" << json_escape(options.time_control_id) << "\",\n";
        manifest << "  \"initial_time_seconds\": " << options.initial_time_seconds << ",\n";
        manifest << "  \"increment_seconds\": " << options.increment_seconds << ",\n";
        manifest << "  \"time_format_label\": \"" << json_escape(options.time_format_label) << "\",\n";
        manifest << "  \"probe_count\": " << probe_defs.size() << ",\n";
        manifest << "  \"whole_game_result_utility\": true,\n";
        manifest << "  \"stockfish_used\": false,\n";
        manifest << "  \"external_book_dependency_used\": false,\n";
        manifest << "  \"output_files\": {\n";
        manifest << "    \"manifest\": \"manifest.json\",\n";
        manifest << "    \"build_summary\": \"build_summary.txt\",\n";
        manifest << "    \"probe_report\": \"probe_report.json\"\n";
        manifest << "  }\n";
        manifest << "}\n";
    }
    progress.stage_completed("artifacts written");
    progress.stage_started(ProgressStage::Finalize, "finalize");
    progress.update([](ProgressSnapshot& s) { s.risky_estimated_remaining_work = 0; s.stage_active = false; });
    progress.stage_completed("done");
    progress.finish();
    return 0;
}

}  // namespace otcb
