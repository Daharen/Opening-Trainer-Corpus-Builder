#pragma once

#include <optional>
#include <string>
#include <vector>

namespace otcb {

enum class TokenizationFailureReason {
    None,
    UnterminatedComment,
    UnterminatedVariation,
    UnsupportedEscapeLine,
};

struct SanTokenizationResult {
    bool success = false;
    TokenizationFailureReason failure_reason = TokenizationFailureReason::None;
    std::vector<std::string> san_tokens;
    bool terminated_by_result = false;
};

std::string to_string(TokenizationFailureReason reason);
SanTokenizationResult tokenize_movetext(const std::string& movetext);

}  // namespace otcb
