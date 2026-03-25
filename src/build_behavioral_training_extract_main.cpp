#include <iostream>

#include "otcb/behavioral_extract.hpp"

int main(int argc, char** argv) {
    try {
        const auto options = otcb::parse_behavioral_extract_cli(argc, argv);
        const auto counters = otcb::build_behavioral_training_extract(options);
        std::cout << "files processed: " << counters.files_processed << '\n';
        std::cout << "games seen: " << counters.games_seen << '\n';
        std::cout << "games accepted: " << counters.games_accepted << '\n';
        std::cout << "games rejected: " << counters.games_rejected << '\n';
        std::cout << "move events emitted: " << counters.move_events_emitted << '\n';
        std::cout << "rows skipped due to missing clock data: " << counters.rows_skipped_missing_clock << '\n';
        std::cout << "rows skipped due to invalid time control: " << counters.rows_skipped_invalid_time_control << '\n';
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "error: " << ex.what() << '\n';
        return 1;
    }
}
