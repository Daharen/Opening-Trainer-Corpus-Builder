#include "otcb/game_envelope.hpp"

namespace otcb {

std::string to_string(const HeaderScanClassification classification) {
    switch (classification) {
        case HeaderScanClassification::Accepted:
            return "accepted";
        case HeaderScanClassification::RejectedMissingWhiteElo:
            return "rejected_missing_white_elo";
        case HeaderScanClassification::RejectedMissingBlackElo:
            return "rejected_missing_black_elo";
        case HeaderScanClassification::RejectedInvalidWhiteElo:
            return "rejected_invalid_white_elo";
        case HeaderScanClassification::RejectedInvalidBlackElo:
            return "rejected_invalid_black_elo";
        case HeaderScanClassification::RejectedPolicyMismatch:
            return "rejected_policy_mismatch";
        case HeaderScanClassification::RejectedIncompleteHeaderBlock:
            return "rejected_incomplete_header_block";
        case HeaderScanClassification::RejectedIncompleteGameEnvelope:
            return "rejected_incomplete_game_envelope";
        case HeaderScanClassification::RejectedNonstandardOrUnsupportedHeaderShape:
            return "rejected_nonstandard_or_unsupported_header_shape";
    }
    return "unknown";
}

}  // namespace otcb
