#pragma once

#include <array>
#include <optional>
#include <string>
#include <vector>

#include "otcb/chess_types.hpp"

namespace otcb {

class ChessBoard {
public:
    ChessBoard();

    [[nodiscard]] Color side_to_move() const;
    [[nodiscard]] Piece piece_at(int square) const;
    [[nodiscard]] bool can_castle_white_kingside() const;
    [[nodiscard]] bool can_castle_white_queenside() const;
    [[nodiscard]] bool can_castle_black_kingside() const;
    [[nodiscard]] bool can_castle_black_queenside() const;
    [[nodiscard]] std::optional<int> en_passant_target() const;

    [[nodiscard]] std::vector<Move> generate_legal_moves() const;
    [[nodiscard]] bool is_in_check(Color color) const;
    [[nodiscard]] bool has_any_legal_moves() const;
    [[nodiscard]] bool is_checkmate() const;
    [[nodiscard]] bool is_stalemate() const;
    [[nodiscard]] std::string to_fen() const;
    [[nodiscard]] ChessBoard after_move(const Move& move) const;
    void apply_move(const Move& move);

private:
    std::array<Piece, 64> board_{};
    Color side_to_move_ = Color::White;
    bool white_can_castle_kingside_ = true;
    bool white_can_castle_queenside_ = true;
    bool black_can_castle_kingside_ = true;
    bool black_can_castle_queenside_ = true;
    std::optional<int> en_passant_target_;
    int halfmove_clock_ = 0;
    int fullmove_number_ = 1;

    [[nodiscard]] bool is_square_attacked(int square, Color by_color) const;
    [[nodiscard]] std::vector<Move> generate_pseudo_legal_moves() const;
    void add_pawn_moves(int from, std::vector<Move>& moves) const;
    void add_knight_moves(int from, std::vector<Move>& moves) const;
    void add_sliding_moves(int from, const std::vector<int>& directions, std::vector<Move>& moves) const;
    void add_king_moves(int from, std::vector<Move>& moves) const;
};

}  // namespace otcb
