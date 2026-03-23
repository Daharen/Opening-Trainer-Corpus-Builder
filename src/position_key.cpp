#include "otcb/position_key.hpp"

#include <sstream>

#include "otcb/chess_types.hpp"

namespace otcb {
namespace {

std::string render_board_placement(const ChessBoard& board) {
    std::ostringstream out;
    for (int rank = 7; rank >= 0; --rank) {
        int empty_count = 0;
        for (int file = 0; file < 8; ++file) {
            const Piece piece = board.piece_at(rank * 8 + file);
            if (piece.is_none()) {
                ++empty_count;
                continue;
            }
            if (empty_count > 0) {
                out << empty_count;
                empty_count = 0;
            }
            out << piece_type_to_fen_letter(piece.type, piece.color);
        }
        if (empty_count > 0) {
            out << empty_count;
        }
        if (rank > 0) {
            out << '/';
        }
    }
    return out.str();
}

std::string render_castling(const ChessBoard& board) {
    std::string castling;
    if (board.can_castle_white_kingside()) castling += 'K';
    if (board.can_castle_white_queenside()) castling += 'Q';
    if (board.can_castle_black_kingside()) castling += 'k';
    if (board.can_castle_black_queenside()) castling += 'q';
    return castling.empty() ? "-" : castling;
}

std::string render_legal_en_passant(const ChessBoard& board) {
    if (!board.en_passant_target().has_value()) {
        return "-";
    }
    for (const Move& move : board.generate_legal_moves()) {
        if (move.is_en_passant) {
            return square_to_string(*board.en_passant_target());
        }
    }
    return "-";
}

}  // namespace

std::string make_position_key(const ChessBoard& board, const PositionKeyFormat format) {
    std::ostringstream out;
    out << render_board_placement(board) << ' ';
    out << (board.side_to_move() == Color::White ? 'w' : 'b') << ' ';
    out << render_castling(board) << ' ';
    out << render_legal_en_passant(board);
    if (format == PositionKeyFormat::FenFull) {
        const std::string full_fen = board.to_fen();
        const auto last_space = full_fen.rfind(' ');
        const auto second_last_space = full_fen.rfind(' ', last_space - 1);
        if (second_last_space != std::string::npos) {
            out << full_fen.substr(second_last_space);
        }
    }
    return out.str();
}

}  // namespace otcb
