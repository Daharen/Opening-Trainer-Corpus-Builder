#include "otcb/san_replay.hpp"

#include <algorithm>
#include <cctype>

namespace otcb {
namespace {

std::string strip_check_suffixes(std::string token) {
    while (!token.empty() && (token.back() == '+' || token.back() == '#')) {
        token.pop_back();
    }
    return token;
}


std::optional<ParsedSanMove> parse_san(const std::string& san_token, ReplayFailureReason& failure_reason) {
    ParsedSanMove parsed;
    parsed.normalized_token = san_token;
    std::string token = strip_check_suffixes(san_token);
    while (!token.empty() && (token.back() == '!' || token.back() == '?')) {
        token.pop_back();
    }
    if (token == "O-O" || token == "0-0") {
        parsed.is_castling_kingside = true;
        parsed.normalized_token = token;
        return parsed;
    }
    if (token == "O-O-O" || token == "0-0-0") {
        parsed.is_castling_queenside = true;
        parsed.normalized_token = token;
        return parsed;
    }

    const std::size_t equals = token.find('=');
    if (equals != std::string::npos) {
        if (equals + 1 >= token.size()) {
            failure_reason = ReplayFailureReason::PromotionParseFailed;
            return std::nullopt;
        }
        const auto promo = piece_type_from_san_letter(token[equals + 1]);
        if (!promo.has_value()) {
            failure_reason = ReplayFailureReason::PromotionParseFailed;
            return std::nullopt;
        }
        parsed.promotion = *promo;
        token = token.substr(0, equals);
    }
    if (token.size() < 2) {
        failure_reason = ReplayFailureReason::UnsupportedMovetextToken;
        return std::nullopt;
    }
    const std::string square_text = token.substr(token.size() - 2);
    const int target = square_from_string(square_text);
    if (target < 0) {
        failure_reason = ReplayFailureReason::UnsupportedMovetextToken;
        return std::nullopt;
    }
    parsed.target_square = target;
    std::string prefix = token.substr(0, token.size() - 2);
    if (!prefix.empty()) {
        const auto piece = piece_type_from_san_letter(prefix.front());
        if (piece.has_value()) {
            parsed.piece = *piece;
            prefix.erase(prefix.begin());
        }
    }

    const std::size_t capture_pos = prefix.find('x');
    if (capture_pos != std::string::npos) {
        parsed.is_capture = true;
        prefix.erase(capture_pos, 1);
    }

    for (const char ch : prefix) {
        if (ch >= 'a' && ch <= 'h') {
            if (parsed.from_file.has_value()) {
                failure_reason = ReplayFailureReason::UnsupportedMovetextToken;
                return std::nullopt;
            }
            parsed.from_file = ch - 'a';
        } else if (ch >= '1' && ch <= '8') {
            if (parsed.from_rank.has_value()) {
                failure_reason = ReplayFailureReason::UnsupportedMovetextToken;
                return std::nullopt;
            }
            parsed.from_rank = ch - '1';
        } else {
            failure_reason = ReplayFailureReason::UnsupportedMovetextToken;
            return std::nullopt;
        }
    }
    parsed.normalized_token = token;
    return parsed;
}

bool matches_parsed_san(const Move& move, const ChessBoard& board, const ParsedSanMove& parsed) {
    if (!parsed.target_square.has_value()) {
        return false;
    }
    if (move.piece != parsed.piece || move.to != *parsed.target_square) {
        return false;
    }
    if (parsed.is_capture != move.is_capture && !(parsed.is_capture && move.is_en_passant)) {
        return false;
    }
    if (parsed.promotion != PieceType::None && move.promotion != parsed.promotion) {
        return false;
    }
    if (parsed.promotion == PieceType::None && move.promotion != PieceType::None) {
        return false;
    }
    if (parsed.from_file.has_value() && (move.from % 8) != *parsed.from_file) {
        return false;
    }
    if (parsed.from_rank.has_value() && (move.from / 8) != *parsed.from_rank) {
        return false;
    }
    const Piece moving_piece = board.piece_at(move.from);
    if (moving_piece.type != parsed.piece) {
        return false;
    }
    return true;
}

}  // namespace

std::string to_string(const ReplayFailureReason reason) {
    switch (reason) {
        case ReplayFailureReason::None: return "none";
        case ReplayFailureReason::UnsupportedMovetextToken: return "unsupported_movetext_token";
        case ReplayFailureReason::IllegalSanMove: return "illegal_san_move";
        case ReplayFailureReason::AmbiguousSanResolutionFailed: return "ambiguous_san_resolution_failed";
        case ReplayFailureReason::PromotionParseFailed: return "promotion_parse_failed";
        case ReplayFailureReason::CastlingParseFailed: return "castling_parse_failed";
        case ReplayFailureReason::UnexpectedReplayState: return "unexpected_replay_state";
    }
    return "unknown";
}

SanResolutionResult resolve_san_move(const ChessBoard& board, const std::string& san_token) {
    SanResolutionResult result;
    ReplayFailureReason parse_failure = ReplayFailureReason::None;
    const auto parsed = parse_san(san_token, parse_failure);
    if (!parsed.has_value()) {
        result.failure_reason = parse_failure;
        return result;
    }
    result.parsed = *parsed;

    const auto legal_moves = board.generate_legal_moves();
    if (parsed->is_castling_kingside || parsed->is_castling_queenside) {
        std::vector<Move> candidates;
        for (const Move& move : legal_moves) {
            if ((parsed->is_castling_kingside && move.is_castling_kingside) || (parsed->is_castling_queenside && move.is_castling_queenside)) {
                candidates.push_back(move);
            }
        }
        if (candidates.empty()) {
            result.failure_reason = ReplayFailureReason::CastlingParseFailed;
            return result;
        }
        if (candidates.size() > 1) {
            result.failure_reason = ReplayFailureReason::AmbiguousSanResolutionFailed;
            return result;
        }
        result.success = true;
        result.move = candidates.front();
        return result;
    }

    std::vector<Move> candidates;
    for (const Move& move : legal_moves) {
        if (matches_parsed_san(move, board, *parsed)) {
            candidates.push_back(move);
        }
    }
    if (candidates.empty()) {
        result.failure_reason = ReplayFailureReason::IllegalSanMove;
        return result;
    }
    if (candidates.size() > 1) {
        result.failure_reason = ReplayFailureReason::AmbiguousSanResolutionFailed;
        return result;
    }
    result.success = true;
    result.move = candidates.front();
    return result;
}

}  // namespace otcb
