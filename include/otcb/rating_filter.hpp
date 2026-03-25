#pragma once

#include <optional>
#include <string>
#include <vector>

#include "otcb/config.hpp"

namespace otcb {

struct EloRange {
    int lo = 0;
    int hi = 0;
};

std::optional<EloRange> parse_elo_range(const std::string& spec);
bool in_elo_ranges(int rating, const std::vector<EloRange>& ranges);
bool rating_policy_match(int white_rating, int black_rating, RatingPolicy policy, const std::vector<EloRange>& ranges);

}  // namespace otcb
