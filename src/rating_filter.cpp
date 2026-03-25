#include "otcb/rating_filter.hpp"

#include <cctype>
#include <exception>

namespace otcb {

namespace {

bool parse_non_negative_int(const std::string& text, int& out) {
    if (text.empty()) {
        return false;
    }
    for (const char ch : text) {
        if (!std::isdigit(static_cast<unsigned char>(ch))) {
            return false;
        }
    }
    try {
        out = std::stoi(text);
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

}  // namespace

std::optional<EloRange> parse_elo_range(const std::string& spec) {
    const std::size_t dash = spec.find('-');
    if (dash == std::string::npos || dash == 0 || dash + 1 >= spec.size()) {
        return std::nullopt;
    }

    int lo = 0;
    int hi = 0;
    if (!parse_non_negative_int(spec.substr(0, dash), lo) || !parse_non_negative_int(spec.substr(dash + 1), hi)) {
        return std::nullopt;
    }
    if (lo > hi) {
        return std::nullopt;
    }
    return EloRange{lo, hi};
}

bool in_elo_ranges(const int rating, const std::vector<EloRange>& ranges) {
    for (const EloRange& range : ranges) {
        if (rating >= range.lo && rating <= range.hi) {
            return true;
        }
    }
    return false;
}

bool rating_policy_match(const int white_rating, const int black_rating, const RatingPolicy policy, const std::vector<EloRange>& ranges) {
    if (ranges.empty()) {
        return true;
    }

    switch (policy) {
        case RatingPolicy::BothInBand:
            return in_elo_ranges(white_rating, ranges) && in_elo_ranges(black_rating, ranges);
        case RatingPolicy::AverageInBand:
            return in_elo_ranges((white_rating + black_rating) / 2, ranges);
        case RatingPolicy::WhiteInBand:
            return in_elo_ranges(white_rating, ranges);
        case RatingPolicy::BlackInBand:
            return in_elo_ranges(black_rating, ranges);
    }

    return false;
}

}  // namespace otcb
