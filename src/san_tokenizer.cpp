#include "otcb/san_tokenizer.hpp"

#include <cctype>

namespace otcb {
namespace {

bool is_result_token(const std::string& token) {
    return token == "1-0" || token == "0-1" || token == "1/2-1/2" || token == "*";
}

bool is_move_number_token(const std::string& token) {
    if (token.empty()) {
        return false;
    }
    std::size_t index = 0;
    while (index < token.size() && std::isdigit(static_cast<unsigned char>(token[index]))) {
        ++index;
    }
    if (index == 0) {
        return false;
    }
    while (index < token.size() && token[index] == '.') {
        ++index;
    }
    return index == token.size();
}

bool is_nag_token(const std::string& token) {
    return !token.empty() && token.front() == '$';
}

std::string trim_annotation_suffixes(std::string token) {
    while (!token.empty()) {
        const char tail = token.back();
        if (tail == '!' || tail == '?') {
            token.pop_back();
            continue;
        }
        break;
    }
    return token;
}

}  // namespace

std::string to_string(const TokenizationFailureReason reason) {
    switch (reason) {
        case TokenizationFailureReason::None: return "none";
        case TokenizationFailureReason::UnterminatedComment: return "unterminated_comment";
        case TokenizationFailureReason::UnterminatedVariation: return "unterminated_variation";
        case TokenizationFailureReason::UnsupportedEscapeLine: return "unsupported_escape_line";
    }
    return "unknown";
}

SanTokenizationResult tokenize_movetext(const std::string& movetext) {
    SanTokenizationResult result;
    std::string current;
    int variation_depth = 0;
    bool in_comment = false;
    bool at_line_start = true;

    const auto flush_token = [&]() {
        if (current.empty() || variation_depth > 0) {
            current.clear();
            return;
        }
        std::string token = trim_annotation_suffixes(current);
        current.clear();
        if (token.empty() || is_move_number_token(token) || is_nag_token(token)) {
            return;
        }
        if (is_result_token(token)) {
            result.terminated_by_result = true;
            return;
        }
        result.san_tokens.push_back(token);
    };

    for (std::size_t index = 0; index < movetext.size(); ++index) {
        const char ch = movetext[index];
        if (at_line_start && ch == '%') {
            result.failure_reason = TokenizationFailureReason::UnsupportedEscapeLine;
            return result;
        }
        if (in_comment) {
            if (ch == '}') {
                in_comment = false;
            }
            at_line_start = ch == '\n' || ch == '\r';
            continue;
        }
        if (ch == '{') {
            flush_token();
            in_comment = true;
            at_line_start = false;
            continue;
        }
        if (ch == '(') {
            flush_token();
            ++variation_depth;
            at_line_start = false;
            continue;
        }
        if (ch == ')') {
            flush_token();
            if (variation_depth == 0) {
                result.failure_reason = TokenizationFailureReason::UnterminatedVariation;
                return result;
            }
            --variation_depth;
            at_line_start = false;
            continue;
        }
        if (std::isspace(static_cast<unsigned char>(ch))) {
            flush_token();
            at_line_start = ch == '\n' || ch == '\r';
            continue;
        }
        if (variation_depth == 0) {
            current.push_back(ch);
        }
        at_line_start = false;
    }
    flush_token();
    if (in_comment) {
        result.failure_reason = TokenizationFailureReason::UnterminatedComment;
        return result;
    }
    if (variation_depth != 0) {
        result.failure_reason = TokenizationFailureReason::UnterminatedVariation;
        return result;
    }
    result.success = true;
    return result;
}

}  // namespace otcb
