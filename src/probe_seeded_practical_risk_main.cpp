#include <exception>
#include <iostream>

#include "otcb/seeded_practical_risk_probe.hpp"

int main(int argc, char** argv) {
    try {
        const auto options = otcb::parse_seeded_practical_risk_probe_cli(argc, argv);
        return otcb::run_seeded_practical_risk_probe(options);
    } catch (const std::exception& ex) {
        std::cerr << "error: " << ex.what() << '\n';
        return 1;
    }
}
