#include "otcb/cli.hpp"

#include <iostream>
#include <sstream>
#include <stdexcept>

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

std::string require_value(int argc, char** argv, int& index, const std::string& option) {
    if (index + 1 >= argc) {
        throw std::runtime_error("Missing value for " + option);
    }
    ++index;
    return argv[index];
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
                continue;
            }
            if (arg == "--input-pgn") {
                result.config.input_pgn = require_value(argc, argv, index, arg);
                continue;
            }
            if (arg == "--output-dir") {
                result.config.output_dir = require_value(argc, argv, index, arg);
                continue;
            }
            if (arg == "--min-rating") {
                result.config.min_rating = parse_int_argument(arg, require_value(argc, argv, index, arg));
                continue;
            }
            if (arg == "--max-rating") {
                result.config.max_rating = parse_int_argument(arg, require_value(argc, argv, index, arg));
                continue;
            }
            if (arg == "--rating-policy") {
                const auto parsed = parse_rating_policy(require_value(argc, argv, index, arg));
                if (!parsed.has_value()) {
                    throw std::runtime_error("Invalid value for --rating-policy. See --help for supported values.");
                }
                result.config.rating_policy = parsed;
                continue;
            }
            if (arg == "--retained-ply") {
                result.config.retained_ply = parse_int_argument(arg, require_value(argc, argv, index, arg));
                continue;
            }
            if (arg == "--threads") {
                result.config.threads = parse_int_argument(arg, require_value(argc, argv, index, arg));
                continue;
            }
            if (arg == "--max-games") {
                result.config.max_games = parse_int_argument(arg, require_value(argc, argv, index, arg));
                continue;
            }
            if (arg == "--artifact-id") {
                result.config.artifact_id = require_value(argc, argv, index, arg);
                continue;
            }
            if (arg == "--progress-interval") {
                result.config.progress_interval = parse_int_argument(arg, require_value(argc, argv, index, arg));
                continue;
            }
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
        << "Initial C++ baseline for scaffold dry-run artifact emission. Full PGN corpus building is not yet implemented.\n\n"
        << "Options:\n"
        << "  --input-pgn <path>           Path to the source PGN file. Required for --dry-run.\n"
        << "  --output-dir <path>          Directory where the artifact bundle will be created. Required for --dry-run.\n"
        << "  --min-rating <int>           Inclusive lower rating bound.\n"
        << "  --max-rating <int>           Inclusive upper rating bound.\n"
        << "  --rating-policy <value>      Explicit rating eligibility policy. Required.\n"
        << rating_policy_help() << "\n"
        << "  --retained-ply <int>         Number of opening plies to retain. Must be > 0.\n"
        << "  --threads <int>              Requested worker thread count. Must be >= 1.\n"
        << "  --max-games <int>            Maximum games to consider. Must be >= 0.\n"
        << "  --artifact-id <string>       Optional explicit artifact bundle identifier.\n"
        << "  --progress-interval <int>    Progress reporting interval. Must be >= 1.\n"
        << "  --dry-run                    Validate parameters and emit a scaffold artifact bundle.\n"
        << "  --help                       Print this help message and exit.\n";
}

}  // namespace otcb
