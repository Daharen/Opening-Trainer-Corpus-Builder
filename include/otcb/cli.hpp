#pragma once

#include <iosfwd>
#include <string>
#include <vector>

#include "otcb/config.hpp"

namespace otcb {

struct CliParseResult {
    BuildConfig config;
    bool ok = false;
    bool should_exit = false;
    int exit_code = 0;
    std::vector<std::string> errors;
};

CliParseResult parse_cli(int argc, char** argv);
void print_usage(std::ostream& stream, const std::string& program_name);

}  // namespace otcb
