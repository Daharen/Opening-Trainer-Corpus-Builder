#include "otcb/practical_risk_screen.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#ifdef _WIN32
#define NOMINMAX
#include <windows.h>
#else
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include <sqlite3.h>

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
    for (const char ch : movetext) {
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

struct ParsedGame {
    std::map<std::string, std::string> tags;
    std::string movetext;
};

template <typename Callback>
void stream_games(const std::filesystem::path& pgn_path, ProgressReporter& progress, Callback&& callback) {
    std::ifstream in(pgn_path);
    if (!in) throw std::runtime_error("unable to open input pgn: " + pgn_path.string());

    ParsedGame current;
    bool in_moves = false;
    std::string line;
    while (std::getline(in, line)) {
        progress.update([&](ProgressSnapshot& s) { s.source_bytes_scanned += static_cast<std::uint64_t>(line.size() + 1); });

        const std::string t = trim(line);
        if (t.empty()) {
            if (in_moves && !current.movetext.empty() && !current.tags.empty()) {
                callback(current);
                current = ParsedGame{};
                in_moves = false;
            }
            continue;
        }

        if (!in_moves && t.front() == '[') {
            const auto q1 = t.find('"');
            const auto q2 = t.rfind('"');
            if (q1 != std::string::npos && q2 != std::string::npos && q2 > q1) {
                std::string name = trim(t.substr(1, q1 - 1));
                std::string value = t.substr(q1 + 1, q2 - q1 - 1);
                if (!name.empty()) current.tags[name] = value;
            }
            continue;
        }

        if (in_moves || !current.tags.empty()) {
            if (!current.movetext.empty()) current.movetext.push_back(' ');
            current.movetext += t;
            in_moves = true;
        }
    }
    if (!current.movetext.empty() && !current.tags.empty()) callback(current);
}

double score_for_side(const std::string& result, Color side) {
    if (result == "1/2-1/2") return 0.5;
    if (result == "1-0") return side == Color::White ? 1.0 : 0.0;
    if (result == "0-1") return side == Color::Black ? 1.0 : 0.0;
    return 0.5;
}

struct MoveStats {
    int support_count = 0;
    double score_sum = 0.0;
    double score_sq_sum = 0.0;
};

struct RootStats {
    int root_support = 0;
    std::map<std::string, MoveStats> moves;
};

std::pair<double, double> mean_sigma(const MoveStats& stats) {
    if (stats.support_count <= 0) return {0.0, 0.0};
    const double mu = stats.score_sum / static_cast<double>(stats.support_count);
    const double ex2 = stats.score_sq_sum / static_cast<double>(stats.support_count);
    const double variance = std::max(0.0, ex2 - mu * mu);
    return {mu, std::sqrt(variance)};
}

std::string policy_hash(const PracticalRiskScreenOptions& o) {
    std::ostringstream out;
    out << o.engine_accept_policy << '|' << o.engine_max_loss_cp << '|' << o.engine_reference_mode;
    return out.str();
}

class EngineEvalCache {
public:
    explicit EngineEvalCache(const std::filesystem::path& sqlite_path) : sqlite_path_(sqlite_path) {
        if (sqlite3_open(sqlite_path.string().c_str(), &db_) != SQLITE_OK) {
            throw std::runtime_error("unable to open engine eval cache sqlite");
        }
        const char* ddl =
            "CREATE TABLE IF NOT EXISTS eval_cache ("
            "cache_key TEXT PRIMARY KEY,"
            "position_key TEXT NOT NULL,"
            "move_uci TEXT NOT NULL,"
            "engine_id TEXT NOT NULL,"
            "movetime_ms INTEGER NOT NULL,"
            "policy_hash TEXT NOT NULL,"
            "score_cp INTEGER NOT NULL"
            ");";
        char* err = nullptr;
        if (sqlite3_exec(db_, ddl, nullptr, nullptr, &err) != SQLITE_OK) {
            std::string msg = err ? err : "sqlite error";
            sqlite3_free(err);
            throw std::runtime_error("sqlite create table failed: " + msg);
        }
    }

    ~EngineEvalCache() {
        if (db_) sqlite3_close(db_);
    }

    std::optional<double> get(const std::string& key) {
        auto it = memory_.find(key);
        if (it != memory_.end()) return it->second;
        const char* sql = "SELECT score_cp FROM eval_cache WHERE cache_key=?1";
        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) != SQLITE_OK) return std::nullopt;
        sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_TRANSIENT);
        std::optional<double> out;
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            out = static_cast<double>(sqlite3_column_int(stmt, 0));
            memory_.emplace(key, *out);
        }
        sqlite3_finalize(stmt);
        return out;
    }

    void put(const std::string& key,
             const std::string& position_key,
             const std::string& move_uci,
             const std::string& engine_id,
             int movetime_ms,
             const std::string& ph,
             double score_cp) {
        memory_[key] = score_cp;
        const char* sql =
            "INSERT OR REPLACE INTO eval_cache(cache_key, position_key, move_uci, engine_id, movetime_ms, policy_hash, score_cp)"
            "VALUES(?1,?2,?3,?4,?5,?6,?7)";
        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) != SQLITE_OK) return;
        sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, position_key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 3, move_uci.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 4, engine_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(stmt, 5, movetime_ms);
        sqlite3_bind_text(stmt, 6, ph.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(stmt, 7, static_cast<int>(std::lround(score_cp)));
        (void)sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }

private:
    std::filesystem::path sqlite_path_;
    sqlite3* db_ = nullptr;
    std::unordered_map<std::string, double> memory_;
};

class UciEngine {
public:
    UciEngine(const std::filesystem::path& path, int hash_mb, int threads) {
#ifdef _WIN32
        SECURITY_ATTRIBUTES sa{};
        sa.nLength = sizeof(sa);
        sa.bInheritHandle = TRUE;
        sa.lpSecurityDescriptor = nullptr;

        HANDLE child_stdout_read = nullptr;
        HANDLE child_stdout_write = nullptr;
        HANDLE child_stdin_read = nullptr;
        HANDLE child_stdin_write = nullptr;
        if (!CreatePipe(&child_stdout_read, &child_stdout_write, &sa, 0)) {
            throw std::runtime_error("failed to create engine stdout pipe");
        }
        if (!SetHandleInformation(child_stdout_read, HANDLE_FLAG_INHERIT, 0)) {
            CloseHandle(child_stdout_read);
            CloseHandle(child_stdout_write);
            throw std::runtime_error("failed to configure engine stdout pipe inheritance");
        }
        if (!CreatePipe(&child_stdin_read, &child_stdin_write, &sa, 0)) {
            CloseHandle(child_stdout_read);
            CloseHandle(child_stdout_write);
            throw std::runtime_error("failed to create engine stdin pipe");
        }
        if (!SetHandleInformation(child_stdin_write, HANDLE_FLAG_INHERIT, 0)) {
            CloseHandle(child_stdout_read);
            CloseHandle(child_stdout_write);
            CloseHandle(child_stdin_read);
            CloseHandle(child_stdin_write);
            throw std::runtime_error("failed to configure engine stdin pipe inheritance");
        }

        std::wstring command = build_windows_command(path);

        STARTUPINFOW startup{};
        startup.cb = sizeof(startup);
        startup.hStdError = child_stdout_write;
        startup.hStdOutput = child_stdout_write;
        startup.hStdInput = child_stdin_read;
        startup.dwFlags |= STARTF_USESTDHANDLES;

        PROCESS_INFORMATION process{};
        std::vector<wchar_t> cmdline(command.begin(), command.end());
        cmdline.push_back(L'\0');
        if (!CreateProcessW(
                nullptr,
                cmdline.data(),
                nullptr,
                nullptr,
                TRUE,
                0,
                nullptr,
                nullptr,
                &startup,
                &process)) {
            CloseHandle(child_stdout_read);
            CloseHandle(child_stdout_write);
            CloseHandle(child_stdin_read);
            CloseHandle(child_stdin_write);
            throw std::runtime_error("failed to launch engine process");
        }
        CloseHandle(process.hThread);
        process_handle_ = process.hProcess;
        write_handle_ = child_stdin_write;
        read_handle_ = child_stdout_read;
        CloseHandle(child_stdout_write);
        CloseHandle(child_stdin_read);
#else
        int in_pipe[2]{-1, -1};
        int out_pipe[2]{-1, -1};
        if (pipe(in_pipe) != 0 || pipe(out_pipe) != 0) {
            throw std::runtime_error("failed to create pipes for engine");
        }
        child_pid_ = fork();
        if (child_pid_ < 0) {
            throw std::runtime_error("failed to fork engine process");
        }
        if (child_pid_ == 0) {
            dup2(in_pipe[0], STDIN_FILENO);
            dup2(out_pipe[1], STDOUT_FILENO);
            dup2(out_pipe[1], STDERR_FILENO);
            close(in_pipe[0]);
            close(in_pipe[1]);
            close(out_pipe[0]);
            close(out_pipe[1]);
            execl(path.string().c_str(), path.string().c_str(), static_cast<char*>(nullptr));
            _exit(127);
        }
        close(in_pipe[0]);
        close(out_pipe[1]);
        write_fp_ = fdopen(in_pipe[1], "w");
        read_fp_ = fdopen(out_pipe[0], "r");
        if (!write_fp_ || !read_fp_) {
            throw std::runtime_error("failed to open engine stdio streams");
        }
#endif
        send("uci");
        read_until("uciok");
        send("setoption name Hash value " + std::to_string(hash_mb));
        send("setoption name Threads value " + std::to_string(threads));
        send("isready");
        read_until("readyok");
    }

    ~UciEngine() {
#ifdef _WIN32
        if (write_handle_ != nullptr) {
            try {
                send("quit");
            } catch (...) {
            }
            CloseHandle(write_handle_);
            write_handle_ = nullptr;
        }
        if (read_handle_ != nullptr) {
            CloseHandle(read_handle_);
            read_handle_ = nullptr;
        }
        if (process_handle_ != nullptr) {
            WaitForSingleObject(process_handle_, INFINITE);
            CloseHandle(process_handle_);
            process_handle_ = nullptr;
        }
#else
        if (write_fp_) {
            try {
                send("quit");
            } catch (...) {
            }
            fclose(write_fp_);
            write_fp_ = nullptr;
        }
        if (read_fp_) {
            fclose(read_fp_);
            read_fp_ = nullptr;
        }
        if (child_pid_ > 0) {
            int status = 0;
            waitpid(child_pid_, &status, 0);
            child_pid_ = -1;
        }
#endif
    }

    std::string engine_id() const { return engine_id_; }

    double eval_cp(const std::string& fen, const std::optional<std::string>& move_uci, int movetime_ms) {
        std::ostringstream pos;
        pos << "position fen " << fen;
        if (move_uci.has_value()) pos << " moves " << *move_uci;
        send(pos.str());
        send("go movetime " + std::to_string(movetime_ms));
        return read_score_until_bestmove();
    }

private:
    void send(const std::string& cmd) {
#ifdef _WIN32
        const std::string line = cmd + "\n";
        DWORD written = 0;
        if (!WriteFile(write_handle_, line.data(), static_cast<DWORD>(line.size()), &written, nullptr) ||
            written != line.size()) {
            throw std::runtime_error("failed writing command to engine process");
        }
#else
        std::fprintf(write_fp_, "%s\n", cmd.c_str());
        std::fflush(write_fp_);
#endif
    }

    void read_until(const std::string& token) {
        while (auto line = read_line()) {
            if (line->rfind("id name ", 0) == 0) engine_id_ = line->substr(8);
            if (*line == token) return;
        }
        throw std::runtime_error("engine stream ended before token: " + token);
    }

    static double parse_score(const std::string& line) {
        // info depth ... score cp X ... OR score mate Y
        const auto cp = line.find(" score cp ");
        if (cp != std::string::npos) {
            const std::string rest = line.substr(cp + 10);
            std::istringstream in(rest);
            int v = 0;
            in >> v;
            return static_cast<double>(v);
        }
        const auto mate = line.find(" score mate ");
        if (mate != std::string::npos) {
            const std::string rest = line.substr(mate + 12);
            std::istringstream in(rest);
            int m = 0;
            in >> m;
            return m > 0 ? 100000.0 : -100000.0;
        }
        return 0.0;
    }

    double read_score_until_bestmove() {
        double last_score = 0.0;
        while (auto line = read_line()) {
            if (line->rfind("info ", 0) == 0 && line->find(" score ") != std::string::npos) {
                last_score = parse_score(*line);
            }
            if (line->rfind("bestmove ", 0) == 0) return last_score;
        }
        throw std::runtime_error("engine stream ended before bestmove");
    }

    std::optional<std::string> read_line() {
#ifdef _WIN32
        std::string line;
        while (true) {
            char ch = '\0';
            DWORD nread = 0;
            BOOL ok = ReadFile(read_handle_, &ch, 1, &nread, nullptr);
            if (!ok || nread == 0) {
                if (line.empty()) return std::nullopt;
                return trim(line);
            }
            if (ch == '\n') return trim(line);
            if (ch != '\r') line.push_back(ch);
        }
#else
        std::array<char, 4096> buf{};
        if (!std::fgets(buf.data(), static_cast<int>(buf.size()), read_fp_)) return std::nullopt;
        return trim(std::string(buf.data()));
#endif
    }

#ifdef _WIN32
    static std::wstring to_wstring(const std::string& in) {
        if (in.empty()) return L"";
        const int sz = MultiByteToWideChar(CP_UTF8, 0, in.c_str(), -1, nullptr, 0);
        if (sz <= 0) throw std::runtime_error("failed UTF-8 to UTF-16 conversion");
        std::wstring out(static_cast<std::size_t>(sz - 1), L'\0');
        MultiByteToWideChar(CP_UTF8, 0, in.c_str(), -1, out.data(), sz);
        return out;
    }

    static std::wstring quote_windows_arg(const std::wstring& arg) {
        std::wstring quoted = L"\"";
        for (const wchar_t c : arg) {
            if (c == L'"') quoted += L"\\\"";
            else quoted += c;
        }
        quoted += L"\"";
        return quoted;
    }

    static std::wstring build_windows_command(const std::filesystem::path& path) {
        const std::wstring path_w = path.wstring();
        if (path.extension() == ".py") {
            const char* python_env = std::getenv("PYTHON_EXECUTABLE");
            if (!python_env || std::strlen(python_env) == 0) python_env = std::getenv("PYTHON");
            const std::wstring python = to_wstring((python_env && std::strlen(python_env) > 0) ? python_env : "python");
            return quote_windows_arg(python) + L" " + quote_windows_arg(path_w);
        }
        return quote_windows_arg(path_w);
    }
#endif

#ifdef _WIN32
    HANDLE write_handle_ = nullptr;
    HANDLE read_handle_ = nullptr;
    HANDLE process_handle_ = nullptr;
#else
    FILE* write_fp_ = nullptr;
    FILE* read_fp_ = nullptr;
    pid_t child_pid_ = -1;
#endif
    std::string engine_id_ = "unknown";
};

struct CandidateReport {
    std::string candidate_move;
    int candidate_support = 0;
    double candidate_mu = 0.0;
    double candidate_sigma = 0.0;
    double candidate_ceiling = 0.0;
    std::string candidate_engine_reason;
    bool candidate_is_engine_fail = false;
    double baseline_ceiling = 0.0;
    bool candidate_ceiling_beats_baseline = false;
    bool final_pass = false;
    std::string final_reason_code;
};

struct RootReport {
    std::string position_key;
    int root_support = 0;
    std::string most_common_raw_baseline_move;
    int most_common_raw_baseline_support = 0;
    double most_common_raw_baseline_ceiling = 0.0;
    std::optional<std::string> accepted_baseline_move;
    int accepted_baseline_support = 0;
    std::string accepted_baseline_engine_reason;
    double accepted_baseline_mu = 0.0;
    double accepted_baseline_sigma = 0.0;
    double accepted_baseline_ceiling = 0.0;
    std::vector<std::string> survivor_candidates;
    std::vector<CandidateReport> candidates;
    std::string root_reason_code;
};

std::string cache_key(const std::string& position_key,
                      const std::string& move_uci,
                      const std::string& engine_id,
                      int movetime_ms,
                      const std::string& policy_hash) {
    return position_key + "|" + move_uci + "|" + engine_id + "|" + std::to_string(movetime_ms) + "|" + policy_hash;
}

}  // namespace

void print_practical_risk_screen_usage(const std::string& program_name) {
    std::cout
        << "Usage: " << program_name << " --input-pgn <path> --output-dir <dir> --artifact-id <id>\\n"
        << "       --min-rating <n> --max-rating <n> --rating-policy <both_in_band|average_in_band|white_in_band|black_in_band>\\n"
        << "       --retained-ply <n> --time-controls <tc1,tc2> --time-control-id <id> --initial-time-seconds <sec>\\n"
        << "       --increment-seconds <sec> --time-format-label <label> --engine-path <path> [screen options]\\n";
}

PracticalRiskScreenOptions parse_practical_risk_screen_cli(int argc, char** argv) {
    PracticalRiskScreenOptions out;
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
        } else if (arg == "--retained-ply") out.retained_ply = std::stoi(require_value("--retained-ply"));
        else if (arg == "--time-controls") {
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
        else if (arg == "--candidate-min-support") out.candidate_min_support = std::stoi(require_value("--candidate-min-support"));
        else if (arg == "--baseline-min-support") out.baseline_min_support = std::stoi(require_value("--baseline-min-support"));
        else if (arg == "--root-min-support") out.root_min_support = std::stoi(require_value("--root-min-support"));
        else if (arg == "--engine-path") out.engine_path = require_value("--engine-path");
        else if (arg == "--engine-movetime-ms") out.engine_movetime_ms = std::stoi(require_value("--engine-movetime-ms"));
        else if (arg == "--engine-hash-mb") out.engine_hash_mb = std::stoi(require_value("--engine-hash-mb"));
        else if (arg == "--engine-threads") out.engine_threads = std::stoi(require_value("--engine-threads"));
        else if (arg == "--baseline-prefix-limit") out.baseline_prefix_limit = std::stoi(require_value("--baseline-prefix-limit"));
        else if (arg == "--candidate-prefix-limit") out.candidate_prefix_limit = std::stoi(require_value("--candidate-prefix-limit"));
        else if (arg == "--emit-progress-log") out.emit_progress_log = true;
        else if (arg == "--emit-status-json") out.emit_status_json = true;
        else if (arg == "--heartbeat-seconds") out.heartbeat_seconds = std::stoi(require_value("--heartbeat-seconds"));
        else if (arg == "--quiet-progress") out.quiet_progress = true;
        else if (arg == "--engine-accept-policy") out.engine_accept_policy = require_value("--engine-accept-policy");
        else if (arg == "--engine-max-loss-cp") out.engine_max_loss_cp = std::stoi(require_value("--engine-max-loss-cp"));
        else if (arg == "--engine-reference-mode") out.engine_reference_mode = require_value("--engine-reference-mode");
        else if (arg == "--help" || arg == "-h") {
            print_practical_risk_screen_usage(argv[0]);
            std::exit(0);
        } else {
            throw std::runtime_error("unknown argument: " + arg);
        }
    }
    if (out.input_pgn.empty() || out.output_dir.empty() || out.artifact_id.empty() || out.engine_path.empty()) {
        throw std::runtime_error("missing required arguments --input-pgn --output-dir --artifact-id --engine-path");
    }
    if (out.retained_ply <= 0) throw std::runtime_error("--retained-ply must be > 0");
    if (out.time_controls.empty()) throw std::runtime_error("--time-controls required");
    if (out.time_control_id.empty()) throw std::runtime_error("--time-control-id required");
    if (out.min_rating > out.max_rating) throw std::runtime_error("--min-rating must be <= --max-rating");
    if (out.engine_accept_policy != "max_loss_cp") throw std::runtime_error("only --engine-accept-policy=max_loss_cp supported");
    if (out.engine_reference_mode != "root_best") throw std::runtime_error("only --engine-reference-mode=root_best supported");
    return out;
}

int run_practical_risk_screen(const PracticalRiskScreenOptions& options) {
    const auto bundle_dir = options.output_dir / options.artifact_id;
    std::filesystem::create_directories(bundle_dir);
    std::filesystem::create_directories(bundle_dir / "audit");

    ProgressReporter progress(ProgressReporterOptions{options.quiet_progress, options.emit_progress_log, options.emit_status_json, options.heartbeat_seconds, bundle_dir});
    progress.start();

    progress.stage_started(ProgressStage::Preflight, "preflight practical-risk screen");
    progress.stage_completed("preflight complete");

    const auto source_size = std::filesystem::exists(options.input_pgn) ? std::optional<std::uint64_t>(std::filesystem::file_size(options.input_pgn)) : std::nullopt;
    progress.stage_started(ProgressStage::AggregateEmpiricalScreen, "aggregate-empirical-screen", source_size);

    std::map<std::string, RootStats> roots;
    std::set<std::string> tc_allowed(options.time_controls.begin(), options.time_controls.end());
    int games_seen = 0;
    int games_scope = 0;
    std::uint64_t root_move_rows = 0;

    stream_games(options.input_pgn, progress, [&](const ParsedGame& game) {
        ++games_seen;
        const auto welo_it = game.tags.find("WhiteElo");
        const auto belo_it = game.tags.find("BlackElo");
        const auto tc_it = game.tags.find("TimeControl");
        const auto result_it = game.tags.find("Result");
        if (welo_it == game.tags.end() || belo_it == game.tags.end() || tc_it == game.tags.end() || result_it == game.tags.end()) return;
        const auto welo = parse_int(welo_it->second);
        const auto belo = parse_int(belo_it->second);
        if (!welo.has_value() || !belo.has_value()) return;
        if (!rating_policy_match(*welo, *belo, options.rating_policy, {EloRange{options.min_rating, options.max_rating}})) return;
        if (tc_allowed.find(tc_it->second) == tc_allowed.end()) return;

        ++games_scope;
        ChessBoard board;
        const auto san_moves = tokenize_movetext(game.movetext);
        for (std::size_t ply = 0; ply < san_moves.size() && static_cast<int>(ply) < options.retained_ply; ++ply) {
            const auto resolved = resolve_san_move(board, san_moves[ply]);
            if (!resolved.success || !resolved.move.has_value()) break;
            const auto mover = board.side_to_move();
            const std::string key = make_position_key(board, PositionKeyFormat::FenNormalized);
            const std::string move_uci = move_to_uci(*resolved.move);
            const double utility = score_for_side(result_it->second, mover);
            RootStats& root = roots[key];
            ++root.root_support;
            MoveStats& m = root.moves[move_uci];
            ++m.support_count;
            m.score_sum += utility;
            m.score_sq_sum += utility * utility;
            ++root_move_rows;
            board.apply_move(*resolved.move);
        }

        progress.update([&](ProgressSnapshot& s) {
            s.games_scanned = games_seen;
            s.games_accepted = games_scope;
            s.games_rejected = games_seen - games_scope;
            s.aggregated_positions = static_cast<int>(roots.size());
            s.aggregate_move_entries = static_cast<int>(root_move_rows);
            const double elapsed = std::max(1.0, std::chrono::duration<double>(std::chrono::steady_clock::now() - s.stage_started_at).count());
            s.throughput_per_second = static_cast<double>(games_seen) / elapsed;
        });
    });
    progress.stage_completed("aggregate-empirical-screen");

    progress.stage_started(ProgressStage::EngineBaselineScreen, "engine-baseline-screen");
    UciEngine engine(options.engine_path, options.engine_hash_mb, options.engine_threads);
    EngineEvalCache cache(bundle_dir / "engine_eval_cache.sqlite");
    const std::string ph = policy_hash(options);

    int roots_discarded_no_baseline = 0;
    int candidates_passed = 0;
    int candidates_rejected = 0;
    int baseline_evals = 0;
    int candidate_evals = 0;
    int cache_hits = 0;
    int cache_misses = 0;

    auto cached_eval_root_cp = [&](const std::string& position_key, const ChessBoard& board, const std::string& move_uci) {
        const std::string key = cache_key(position_key, move_uci, engine.engine_id(), options.engine_movetime_ms, ph);
        if (const auto hit = cache.get(key); hit.has_value()) {
            ++cache_hits;
            return *hit;
        }
        ++cache_misses;
        const double cp_side_to_move = engine.eval_cp(board.to_fen(), move_uci.empty() ? std::nullopt : std::optional<std::string>(move_uci), options.engine_movetime_ms);
        const double cp_root = move_uci.empty() ? cp_side_to_move : -cp_side_to_move;
        cache.put(key, position_key, move_uci, engine.engine_id(), options.engine_movetime_ms, ph, cp_root);
        return cp_root;
    };

    std::vector<RootReport> reports;
    int survivor_roots = 0;
    for (const auto& [position_key, root] : roots) {
        RootReport rr;
        rr.position_key = position_key;
        rr.root_support = root.root_support;

        std::vector<std::pair<std::string, MoveStats>> ranked(root.moves.begin(), root.moves.end());
        std::sort(ranked.begin(), ranked.end(), [](const auto& a, const auto& b) {
            if (a.second.support_count != b.second.support_count) return a.second.support_count > b.second.support_count;
            return a.first < b.first;
        });
        if (ranked.empty()) continue;

        const auto& raw_baseline = ranked.front();
        rr.most_common_raw_baseline_move = raw_baseline.first;
        rr.most_common_raw_baseline_support = raw_baseline.second.support_count;
        const auto [rb_mu, rb_sigma] = mean_sigma(raw_baseline.second);
        rr.most_common_raw_baseline_ceiling = rb_mu + rb_sigma;

        if (root.root_support < options.root_min_support) {
            rr.root_reason_code = "discard_root_low_support";
            reports.push_back(rr);
            continue;
        }
        if (raw_baseline.second.support_count < options.baseline_min_support) {
            rr.root_reason_code = "discard_no_candidate_support";
            reports.push_back(rr);
            continue;
        }

        std::vector<std::tuple<std::string, MoveStats, double>> cand_survivors;
        for (std::size_t i = 1; i < ranked.size(); ++i) {
            const auto& mv = ranked[i];
            if (mv.second.support_count < options.candidate_min_support) continue;
            const auto [cmu, csig] = mean_sigma(mv.second);
            const double cceil = cmu + csig;
            if (cceil > rr.most_common_raw_baseline_ceiling) {
                cand_survivors.push_back({mv.first, mv.second, cceil});
                rr.survivor_candidates.push_back(mv.first);
            }
        }
        if (cand_survivors.empty()) {
            rr.root_reason_code = "discard_no_candidate_empirical_survivors";
            reports.push_back(rr);
            continue;
        }

        ++survivor_roots;
        ChessBoard board;
        // reconstruct board from key is non-trivial here; deterministic path only for starting position roots in fixture.
        // For generality, skip roots not initial position in engine stage.
        if (position_key != make_position_key(ChessBoard{}, PositionKeyFormat::FenNormalized)) {
            rr.root_reason_code = "discard_no_engine_accepted_baseline";
            reports.push_back(rr);
            ++roots_discarded_no_baseline;
            continue;
        }

        const double best_root_cp = cached_eval_root_cp(position_key, board, "");
        int tried = 0;
        for (const auto& [move, stats] : ranked) {
            if (stats.support_count < options.baseline_min_support) continue;
            if (tried >= options.baseline_prefix_limit) break;
            ++tried;
            ++baseline_evals;
            const double mv_cp = cached_eval_root_cp(position_key, board, move);
            const double loss = best_root_cp - mv_cp;
            const bool accepted = loss <= static_cast<double>(options.engine_max_loss_cp);
            if (accepted) {
                rr.accepted_baseline_move = move;
                rr.accepted_baseline_support = stats.support_count;
                rr.accepted_baseline_engine_reason = "accepted_max_loss_cp";
                const auto [mu, sigma] = mean_sigma(stats);
                rr.accepted_baseline_mu = mu;
                rr.accepted_baseline_sigma = sigma;
                rr.accepted_baseline_ceiling = mu + sigma;
                break;
            }
        }

        if (!rr.accepted_baseline_move.has_value()) {
            rr.root_reason_code = "discard_no_engine_accepted_baseline";
            reports.push_back(rr);
            ++roots_discarded_no_baseline;
            continue;
        }

        std::sort(cand_survivors.begin(), cand_survivors.end(), [](const auto& a, const auto& b) {
            const auto& as = std::get<1>(a);
            const auto& bs = std::get<1>(b);
            if (as.support_count != bs.support_count) return as.support_count > bs.support_count;
            return std::get<0>(a) < std::get<0>(b);
        });

        int cand_done = 0;
        for (const auto& cand : cand_survivors) {
            if (cand_done >= options.candidate_prefix_limit) break;
            ++cand_done;
            ++candidate_evals;
            const std::string move = std::get<0>(cand);
            const MoveStats stats = std::get<1>(cand);
            CandidateReport cr;
            cr.candidate_move = move;
            cr.candidate_support = stats.support_count;
            const auto [mu, sigma] = mean_sigma(stats);
            cr.candidate_mu = mu;
            cr.candidate_sigma = sigma;
            cr.candidate_ceiling = mu + sigma;
            cr.baseline_ceiling = rr.accepted_baseline_ceiling;
            cr.candidate_ceiling_beats_baseline = cr.candidate_ceiling > rr.accepted_baseline_ceiling;

            const double mv_cp = cached_eval_root_cp(position_key, board, move);
            const double loss = best_root_cp - mv_cp;
            cr.candidate_is_engine_fail = loss > static_cast<double>(options.engine_max_loss_cp);
            cr.candidate_engine_reason = cr.candidate_is_engine_fail ? "fail_max_loss_cp" : "not_fail_max_loss_cp";

            if (!cr.candidate_is_engine_fail) {
                cr.final_pass = false;
                cr.final_reason_code = "discard_candidate_engine_not_fail";
                ++candidates_rejected;
            } else if (!cr.candidate_ceiling_beats_baseline) {
                cr.final_pass = false;
                cr.final_reason_code = "discard_candidate_ceiling_not_above_baseline";
                ++candidates_rejected;
            } else {
                cr.final_pass = true;
                cr.final_reason_code = "pass_candidate_ceiling_above_engine_accepted_baseline";
                ++candidates_passed;
            }
            rr.candidates.push_back(cr);
        }

        rr.root_reason_code = rr.candidates.empty() ? "discard_no_candidate_empirical_survivors" : "root_evaluated";

        progress.update([&](ProgressSnapshot& s) {
            s.risky_positions_considered = static_cast<int>(reports.size());
            s.risky_candidates_evaluated = candidate_evals;
            s.risky_admitted_rows = candidates_passed;
            s.risky_rejected_rows = candidates_rejected;
            s.risky_unresolved_rows = roots_discarded_no_baseline;
            s.risky_candidate_fails_considered = baseline_evals;
            s.risky_pooling_events = cache_hits;
        });

        reports.push_back(rr);
    }
    progress.stage_completed("engine-baseline-screen complete");

    progress.stage_started(ProgressStage::WriteArtifacts, "write-artifacts");
    {
        std::ofstream manifest(bundle_dir / "manifest.json");
        manifest << "{\n";
        manifest << "  \"artifact_id\": \"" << json_escape(options.artifact_id) << "\",\n";
        manifest << "  \"artifact_role\": \"practical_risk_screening\",\n";
        manifest << "  \"stockfish_used\": true,\n";
        manifest << "  \"external_book_dependency_used\": false,\n";
        manifest << "  \"evaluation_policy\": {\n";
        manifest << "    \"engine_accept_policy\": \"" << json_escape(options.engine_accept_policy) << "\",\n";
        manifest << "    \"engine_reference_mode\": \"" << json_escape(options.engine_reference_mode) << "\",\n";
        manifest << "    \"engine_max_loss_cp\": " << options.engine_max_loss_cp << "\n";
        manifest << "  },\n";
        manifest << "  \"candidate_min_support\": " << options.candidate_min_support << ",\n";
        manifest << "  \"baseline_min_support\": " << options.baseline_min_support << ",\n";
        manifest << "  \"root_min_support\": " << options.root_min_support << ",\n";
        manifest << "  \"baseline_prefix_limit\": " << options.baseline_prefix_limit << ",\n";
        manifest << "  \"candidate_prefix_limit\": " << options.candidate_prefix_limit << ",\n";
        manifest << "  \"raw_empirical_ceiling_rule_used\": true,\n";
        manifest << "  \"mean_penalty_veto_used\": false\n";
        manifest << "}\n";

        std::ofstream summary(bundle_dir / "build_summary.txt");
        summary << "practical-risk screening build complete\n";
        summary << "roots_observed=" << roots.size() << "\n";
        summary << "roots_survived_empirical=" << survivor_roots << "\n";
        summary << "baseline_engine_evaluations=" << baseline_evals << "\n";
        summary << "candidate_engine_evaluations=" << candidate_evals << "\n";
        summary << "cache_hits=" << cache_hits << " cache_misses=" << cache_misses << "\n";

        std::ofstream report(bundle_dir / "root_screen_report.jsonl");
        for (const RootReport& rr : reports) {
            report << "{\"position_key\":\"" << json_escape(rr.position_key) << "\","
                   << "\"root_support\":" << rr.root_support << ","
                   << "\"most_common_raw_baseline_move\":\"" << json_escape(rr.most_common_raw_baseline_move) << "\","
                   << "\"most_common_raw_baseline_support\":" << rr.most_common_raw_baseline_support << ","
                   << "\"most_common_raw_baseline_ceiling\":" << rr.most_common_raw_baseline_ceiling << ","
                   << "\"surviving_candidates_after_empirical_filter\":[";
            for (std::size_t i = 0; i < rr.survivor_candidates.size(); ++i) {
                if (i) report << ',';
                report << "\"" << json_escape(rr.survivor_candidates[i]) << "\"";
            }
            report << "],";
            if (rr.accepted_baseline_move.has_value()) {
                report << "\"accepted_baseline_move\":\"" << json_escape(*rr.accepted_baseline_move) << "\","
                       << "\"accepted_baseline_support\":" << rr.accepted_baseline_support << ","
                       << "\"accepted_baseline_engine_reason\":\"" << json_escape(rr.accepted_baseline_engine_reason) << "\","
                       << "\"accepted_baseline_mu_empirical\":" << rr.accepted_baseline_mu << ","
                       << "\"accepted_baseline_sigma_empirical\":" << rr.accepted_baseline_sigma << ","
                       << "\"accepted_baseline_ceiling_empirical\":" << rr.accepted_baseline_ceiling << ",";
            } else {
                report << "\"accepted_baseline_move\":null,\"accepted_baseline_support\":0,\"accepted_baseline_engine_reason\":\"\",";
                report << "\"accepted_baseline_mu_empirical\":0.0,\"accepted_baseline_sigma_empirical\":0.0,\"accepted_baseline_ceiling_empirical\":0.0,";
            }
            report << "\"candidate_engine_reports\":[";
            for (std::size_t i = 0; i < rr.candidates.size(); ++i) {
                const auto& c = rr.candidates[i];
                if (i) report << ',';
                report << "{\"candidate_move\":\"" << json_escape(c.candidate_move) << "\","
                       << "\"candidate_support\":" << c.candidate_support << ","
                       << "\"candidate_mu_empirical\":" << c.candidate_mu << ","
                       << "\"candidate_sigma_empirical\":" << c.candidate_sigma << ","
                       << "\"candidate_ceiling_empirical\":" << c.candidate_ceiling << ","
                       << "\"candidate_engine_reason\":\"" << json_escape(c.candidate_engine_reason) << "\","
                       << "\"candidate_is_engine_fail\":" << (c.candidate_is_engine_fail ? "true" : "false") << ","
                       << "\"baseline_ceiling_empirical\":" << c.baseline_ceiling << ","
                       << "\"candidate_ceiling_beats_baseline\":" << (c.candidate_ceiling_beats_baseline ? "true" : "false") << ","
                       << "\"final_pass\":" << (c.final_pass ? "true" : "false") << ","
                       << "\"final_reason_code\":\"" << json_escape(c.final_reason_code) << "\"}";
            }
            report << "],\"root_reason_code\":\"" << json_escape(rr.root_reason_code) << "\"}" << '\n';
        }

        std::ofstream screen_summary(bundle_dir / "screen_summary.json");
        screen_summary << "{\n";
        screen_summary << "  \"roots_observed\": " << roots.size() << ",\n";
        screen_summary << "  \"roots_screened\": " << reports.size() << ",\n";
        screen_summary << "  \"roots_survived_empirical\": " << survivor_roots << ",\n";
        screen_summary << "  \"roots_discarded_no_accepted_baseline\": " << roots_discarded_no_baseline << ",\n";
        screen_summary << "  \"baseline_engine_evaluations\": " << baseline_evals << ",\n";
        screen_summary << "  \"candidate_engine_evaluations\": " << candidate_evals << ",\n";
        screen_summary << "  \"cache_hits\": " << cache_hits << ",\n";
        screen_summary << "  \"cache_misses\": " << cache_misses << ",\n";
        screen_summary << "  \"candidates_passed\": " << candidates_passed << ",\n";
        screen_summary << "  \"candidates_rejected\": " << candidates_rejected << "\n";
        screen_summary << "}\n";
    }
    progress.stage_completed("write-artifacts complete");

    progress.stage_started(ProgressStage::Finalize, "finalize");
    progress.update([](ProgressSnapshot& s) {
        s.risky_estimated_remaining_work = 0;
        s.stage_active = false;
    });
    progress.stage_completed("done");
    progress.finish();
    return 0;
}

}  // namespace otcb
