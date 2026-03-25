#include <iostream>

#include "otcb/timing_conditioned_bundle_builder.hpp"

int main(int argc, char** argv) {
    try {
        const auto options = otcb::parse_timing_conditioned_bundle_cli(argc, argv);
        const auto counters = otcb::build_timing_conditioned_corpus_bundle(options);
        std::cout << "corpus artifacts loaded: " << counters.corpus_artifacts_loaded << '\n';
        std::cout << "profile artifacts loaded: " << counters.profile_artifacts_loaded << '\n';
        std::cout << "positions examined: " << counters.positions_examined << '\n';
        std::cout << "move rows examined: " << counters.move_rows_examined << '\n';
        std::cout << "contexts mapped: " << counters.contexts_mapped << '\n';
        std::cout << "profiles referenced: " << counters.profiles_referenced << '\n';
        std::cout << "compatibility warnings surfaced: " << counters.compatibility_warnings << '\n';
        std::cout << "final bundle emitted path: " << counters.emitted_bundle_path.generic_string() << '\n';
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "error: " << ex.what() << '\n';
        return 1;
    }
}
