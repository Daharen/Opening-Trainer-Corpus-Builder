#include "otcb/practical_risk/reconciled.hpp"

#include <iostream>

int main(int argc, char** argv) {
    try {
        const auto options = otcb::parse_practical_risk_reconciled_cli(argc, argv);
        return otcb::run_practical_risk_reconciled(options);
    } catch (const std::exception& ex) {
        std::cerr << "build-practical-risk-reconciled error: " << ex.what() << '\n';
        return 1;
    }
}
