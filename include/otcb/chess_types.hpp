#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace otcb {

enum class Color : std::uint8_t {
    White,
    Black,
};

enum class PieceType : std::uint8_t {
    None,
    Pawn,
    Knight,
    Bishop,
    Rook,
    Queen,
    King,
};

struct Piece {
    PieceType type = PieceType::None;
    Color color = Color::White;

    [[nodiscard]] bool is_none() const {
        return type == PieceType::None;
    }
};

struct Move {
    int from = -1;
    int to = -1;
    PieceType piece = PieceType::None;
    PieceType promotion = PieceType::None;
    bool is_capture = false;
    bool is_en_passant = false;
    bool is_castling_kingside = false;
    bool is_castling_queenside = false;
};

[[nodiscard]] Color opposite_color(Color color);
[[nodiscard]] std::string square_to_string(int square);
[[nodiscard]] int square_from_string(const std::string& square);
[[nodiscard]] char piece_type_to_san_letter(PieceType piece);
[[nodiscard]] char piece_type_to_fen_letter(PieceType piece, Color color);
[[nodiscard]] std::optional<PieceType> piece_type_from_san_letter(char letter);
[[nodiscard]] std::string move_to_uci(const Move& move);

}  // namespace otcb
