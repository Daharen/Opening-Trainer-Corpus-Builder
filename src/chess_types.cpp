#include "otcb/chess_types.hpp"

#include <cctype>

namespace otcb {

Color opposite_color(const Color color) {
    return color == Color::White ? Color::Black : Color::White;
}

std::string square_to_string(const int square) {
    if (square < 0 || square >= 64) {
        return "??";
    }
    const char file = static_cast<char>('a' + (square % 8));
    const char rank = static_cast<char>('1' + (square / 8));
    return std::string{file, rank};
}

int square_from_string(const std::string& square) {
    if (square.size() != 2 || square[0] < 'a' || square[0] > 'h' || square[1] < '1' || square[1] > '8') {
        return -1;
    }
    return (square[1] - '1') * 8 + (square[0] - 'a');
}

char piece_type_to_san_letter(const PieceType piece) {
    switch (piece) {
        case PieceType::Knight: return 'N';
        case PieceType::Bishop: return 'B';
        case PieceType::Rook: return 'R';
        case PieceType::Queen: return 'Q';
        case PieceType::King: return 'K';
        case PieceType::Pawn:
        case PieceType::None:
            return '\0';
    }
    return '\0';
}

char piece_type_to_fen_letter(const PieceType piece, const Color color) {
    char letter = ' ';
    switch (piece) {
        case PieceType::Pawn: letter = 'p'; break;
        case PieceType::Knight: letter = 'n'; break;
        case PieceType::Bishop: letter = 'b'; break;
        case PieceType::Rook: letter = 'r'; break;
        case PieceType::Queen: letter = 'q'; break;
        case PieceType::King: letter = 'k'; break;
        case PieceType::None: letter = ' '; break;
    }
    return color == Color::White ? static_cast<char>(std::toupper(static_cast<unsigned char>(letter))) : letter;
}

std::optional<PieceType> piece_type_from_san_letter(const char letter) {
    switch (letter) {
        case 'N': return PieceType::Knight;
        case 'B': return PieceType::Bishop;
        case 'R': return PieceType::Rook;
        case 'Q': return PieceType::Queen;
        case 'K': return PieceType::King;
        default: return std::nullopt;
    }
}

std::string move_to_uci(const Move& move) {
    std::string uci = square_to_string(move.from) + square_to_string(move.to);
    if (move.promotion != PieceType::None) {
        uci.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(piece_type_to_fen_letter(move.promotion, Color::Black)))));
    }
    return uci;
}

}  // namespace otcb
