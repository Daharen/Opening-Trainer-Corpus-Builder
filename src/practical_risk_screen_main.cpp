#include <exception>
#include <iostream>

#include "otcb/practical_risk_screen.hpp"

int main(int argc, char** argv) {
    try {
        const auto options = otcb::parse_practical_risk_screen_cli(argc, argv);
        return otcb::run_practical_risk_screen(options);
    } catch (const std::exception& ex) {
        std::cerr << "error: " << ex.what() << '\n';
        return 1;
    }
}
