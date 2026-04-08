#include <exception>
#include <iostream>

#include "otcb/practical_risk/prestockfish.hpp"

int main(int argc, char** argv) {
    try {
        const auto options = otcb::parse_practical_risk_prestockfish_cli(argc, argv);
        return otcb::run_practical_risk_prestockfish(options);
    } catch (const std::exception& ex) {
        std::cerr << "error: " << ex.what() << '\n';
        return 1;
    }
}
