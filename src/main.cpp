#include <exception>
#include <iostream>

#include "otcb/bundle_writer.hpp"
#include "otcb/cli.hpp"

int main(int argc, char** argv) {
    const auto parsed = otcb::parse_cli(argc, argv);

    if (parsed.config.help_requested) {
        otcb::print_usage(std::cout, argc > 0 ? argv[0] : "opening-trainer-corpus-builder");
        return parsed.exit_code;
    }

    if (!parsed.ok) {
        for (const auto& error : parsed.errors) {
            std::cerr << "error: " << error << '\n';
        }
        std::cerr << '\n';
        otcb::print_usage(std::cerr, argc > 0 ? argv[0] : "opening-trainer-corpus-builder");
        return parsed.exit_code;
    }

    if (!parsed.config.dry_run) {
        std::cerr << "error: Only --dry-run scaffold mode is implemented in this baseline.\n\n";
        otcb::print_usage(std::cerr, argc > 0 ? argv[0] : "opening-trainer-corpus-builder");
        return 1;
    }

    try {
        const auto result = otcb::write_dry_run_bundle(parsed.config);
        std::cout << "Scaffold artifact bundle written to: " << result.bundle_root.lexically_normal().generic_string() << '\n';
        std::cout << "Artifact id: " << result.artifact_id << '\n';
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "error: " << ex.what() << '\n';
        return 1;
    }
}
