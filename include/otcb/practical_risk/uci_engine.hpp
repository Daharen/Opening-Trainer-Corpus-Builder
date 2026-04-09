#pragma once

#include <cstdio>
#include <filesystem>
#include <optional>
#include <string>

#ifdef _WIN32
#define NOMINMAX
#include <windows.h>
#endif

namespace otcb {

class UciEngine {
public:
    UciEngine(const std::filesystem::path& path, int hash_mb, int threads);
    ~UciEngine();

    UciEngine(const UciEngine&) = delete;
    UciEngine& operator=(const UciEngine&) = delete;

    std::string engine_id() const;
    double eval_cp(const std::string& fen, const std::optional<std::string>& move_uci, int movetime_ms);

private:
    void send(const std::string& cmd);
    void read_until(const std::string& token);
    static double parse_score(const std::string& line);
    double read_score_until_bestmove();
    std::optional<std::string> read_line();

#ifdef _WIN32
    static std::wstring to_wstring(const std::string& in);
    static std::wstring quote_windows_arg(const std::wstring& arg);
    static std::wstring build_windows_command(const std::filesystem::path& path);
#endif

#ifdef _WIN32
    HANDLE write_handle_ = nullptr;
    HANDLE read_handle_ = nullptr;
    HANDLE process_handle_ = nullptr;
#else
    FILE* write_fp_ = nullptr;
    FILE* read_fp_ = nullptr;
    int child_pid_ = -1;
#endif
    std::string engine_id_ = "unknown";
};

}  // namespace otcb
