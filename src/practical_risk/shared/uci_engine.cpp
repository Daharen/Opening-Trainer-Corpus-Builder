#include "otcb/practical_risk/uci_engine.hpp"

#include <array>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <sstream>
#include <stdexcept>
#include <vector>

#ifndef _WIN32
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace otcb {
namespace {

std::string trim(const std::string& in) {
    std::size_t s = 0;
    while (s < in.size() && std::isspace(static_cast<unsigned char>(in[s]))) ++s;
    std::size_t e = in.size();
    while (e > s && std::isspace(static_cast<unsigned char>(in[e - 1]))) --e;
    return in.substr(s, e - s);
}

}  // namespace

UciEngine::UciEngine(const std::filesystem::path& path, int hash_mb, int threads) {
#ifdef _WIN32
    SECURITY_ATTRIBUTES sa{};
    sa.nLength = sizeof(sa);
    sa.bInheritHandle = TRUE;

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
    if (!CreateProcessW(nullptr, cmdline.data(), nullptr, nullptr, TRUE, 0, nullptr, nullptr, &startup, &process)) {
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

UciEngine::~UciEngine() {
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

std::string UciEngine::engine_id() const { return engine_id_; }

double UciEngine::eval_cp(const std::string& fen, const std::optional<std::string>& move_uci, int movetime_ms) {
    std::ostringstream pos;
    pos << "position fen " << fen;
    if (move_uci.has_value()) pos << " moves " << *move_uci;
    send(pos.str());
    send("go movetime " + std::to_string(movetime_ms));
    return read_score_until_bestmove();
}

void UciEngine::send(const std::string& cmd) {
#ifdef _WIN32
    const std::string line = cmd + "\n";
    DWORD written = 0;
    if (!WriteFile(write_handle_, line.data(), static_cast<DWORD>(line.size()), &written, nullptr) || written != line.size()) {
        throw std::runtime_error("failed writing command to engine process");
    }
#else
    std::fprintf(write_fp_, "%s\n", cmd.c_str());
    std::fflush(write_fp_);
#endif
}

void UciEngine::read_until(const std::string& token) {
    while (auto line = read_line()) {
        if (line->rfind("id name ", 0) == 0) engine_id_ = line->substr(8);
        if (*line == token) return;
    }
    throw std::runtime_error("engine stream ended before token: " + token);
}

double UciEngine::parse_score(const std::string& line) {
    const auto cp = line.find(" score cp ");
    if (cp != std::string::npos) {
        std::istringstream in(line.substr(cp + 10));
        int v = 0;
        in >> v;
        return static_cast<double>(v);
    }
    const auto mate = line.find(" score mate ");
    if (mate != std::string::npos) {
        std::istringstream in(line.substr(mate + 12));
        int m = 0;
        in >> m;
        return m > 0 ? 100000.0 : -100000.0;
    }
    return 0.0;
}

double UciEngine::read_score_until_bestmove() {
    double last_score = 0.0;
    while (auto line = read_line()) {
        if (line->rfind("info ", 0) == 0 && line->find(" score ") != std::string::npos) {
            last_score = parse_score(*line);
        }
        if (line->rfind("bestmove ", 0) == 0) return last_score;
    }
    throw std::runtime_error("engine stream ended before bestmove");
}

std::optional<std::string> UciEngine::read_line() {
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
std::wstring UciEngine::to_wstring(const std::string& in) {
    if (in.empty()) return L"";
    const int sz = MultiByteToWideChar(CP_UTF8, 0, in.c_str(), -1, nullptr, 0);
    if (sz <= 0) throw std::runtime_error("failed UTF-8 to UTF-16 conversion");
    std::wstring out(static_cast<std::size_t>(sz - 1), L'\0');
    MultiByteToWideChar(CP_UTF8, 0, in.c_str(), -1, out.data(), sz);
    return out;
}

std::wstring UciEngine::quote_windows_arg(const std::wstring& arg) {
    std::wstring quoted = L"\"";
    for (const wchar_t c : arg) {
        if (c == L'"') quoted += L"\\\"";
        else quoted += c;
    }
    quoted += L"\"";
    return quoted;
}

std::wstring UciEngine::build_windows_command(const std::filesystem::path& path) {
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

}  // namespace otcb
