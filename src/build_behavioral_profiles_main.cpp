#include <iostream>

#include "otcb/behavioral_profile_builder.hpp"

int main(int argc, char** argv) {
    try {
        const auto options = otcb::parse_behavioral_profile_build_cli(argc, argv);
        const auto counters = otcb::build_behavioral_profiles(options);
        std::cout << "extract files loaded: " << counters.extract_files_loaded << '\n';
        std::cout << "raw move events seen: " << counters.raw_move_events_seen << '\n';
        std::cout << "training examples accepted: " << counters.training_examples_accepted << '\n';
        std::cout << "contexts fitted: " << counters.contexts_fitted << '\n';
        std::cout << "candidate profiles created: " << counters.candidate_profiles_created << '\n';
        std::cout << "profiles merged: " << counters.profiles_merged << '\n';
        std::cout << "final move-pressure profiles emitted: " << counters.final_move_pressure_profiles_emitted << '\n';
        std::cout << "final think-time profiles emitted: " << counters.final_think_time_profiles_emitted << '\n';
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "error: " << ex.what() << '\n';
        return 1;
    }
}
