#include "otcb/practical_risk/final.hpp"

#include <iostream>

int main(int argc, char** argv) {
    try {
        const auto options = otcb::parse_practical_risk_final_cli(argc, argv);
        return otcb::run_practical_risk_final(options);
    } catch (const std::exception& ex) {
        std::cerr << "build-practical-risk-final error: " << ex.what() << '\n';
        return 1;
    }
}
