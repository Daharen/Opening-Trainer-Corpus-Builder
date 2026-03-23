#pragma once

#include <optional>
#include <string>
#include <vector>

#include "otcb/chess_board.hpp"

namespace otcb {

enum class ReplayFailureReason {
    None,
    UnsupportedMovetextToken,
    IllegalSanMove,
    AmbiguousSanResolutionFailed,
    PromotionParseFailed,
    CastlingParseFailed,
    UnexpectedReplayState,
};

struct ParsedSanMove {
    bool is_castling_kingside = false;
    bool is_castling_queenside = false;
    PieceType piece = PieceType::Pawn;
    std::optional<int> from_file;
    std::optional<int> from_rank;
    std::optional<int> target_square;
    bool is_capture = false;
    PieceType promotion = PieceType::None;
    bool check_suffix = false;
    bool mate_suffix = false;
    std::string normalized_token;
};

struct SanResolutionResult {
    bool success = false;
    ReplayFailureReason failure_reason = ReplayFailureReason::None;
    std::optional<Move> move;
    ParsedSanMove parsed;
};

std::string to_string(ReplayFailureReason reason);
SanResolutionResult resolve_san_move(const ChessBoard& board, const std::string& san_token);

}  // namespace otcb
