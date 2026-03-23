#include "otcb/chess_board.hpp"

#include <sstream>

namespace otcb {
namespace {

bool is_on_board(const int square) {
    return square >= 0 && square < 64;
}

int rank_of(const int square) {
    return square / 8;
}

int file_of(const int square) {
    return square % 8;
}

bool same_rank(const int a, const int b) {
    return rank_of(a) == rank_of(b);
}

}  // namespace

ChessBoard::ChessBoard() {
    board_.fill(Piece{});
    const Piece white_pawn{PieceType::Pawn, Color::White};
    const Piece black_pawn{PieceType::Pawn, Color::Black};
    for (int file = 0; file < 8; ++file) {
        board_[8 + file] = white_pawn;
        board_[48 + file] = black_pawn;
    }
    const std::array<PieceType, 8> back_rank = {
        PieceType::Rook, PieceType::Knight, PieceType::Bishop, PieceType::Queen,
        PieceType::King, PieceType::Bishop, PieceType::Knight, PieceType::Rook,
    };
    for (int file = 0; file < 8; ++file) {
        board_[file] = Piece{back_rank[file], Color::White};
        board_[56 + file] = Piece{back_rank[file], Color::Black};
    }
}

Color ChessBoard::side_to_move() const { return side_to_move_; }
Piece ChessBoard::piece_at(const int square) const { return board_[square]; }
bool ChessBoard::can_castle_white_kingside() const { return white_can_castle_kingside_; }
bool ChessBoard::can_castle_white_queenside() const { return white_can_castle_queenside_; }
bool ChessBoard::can_castle_black_kingside() const { return black_can_castle_kingside_; }
bool ChessBoard::can_castle_black_queenside() const { return black_can_castle_queenside_; }
std::optional<int> ChessBoard::en_passant_target() const { return en_passant_target_; }

bool ChessBoard::is_square_attacked(const int square, const Color by_color) const {
    const int pawn_step = by_color == Color::White ? -8 : 8;
    for (const int dx : {-1, 1}) {
        const int from = square + pawn_step + dx;
        if (!is_on_board(from) || std::abs(file_of(from) - file_of(square)) != 1) {
            continue;
        }
        const Piece piece = board_[from];
        if (!piece.is_none() && piece.color == by_color && piece.type == PieceType::Pawn) {
            return true;
        }
    }

    const std::array<int, 8> knight_deltas = {15, 17, 10, 6, -15, -17, -10, -6};
    for (const int delta : knight_deltas) {
        const int from = square + delta;
        if (!is_on_board(from)) {
            continue;
        }
        const int file_diff = std::abs(file_of(from) - file_of(square));
        const int rank_diff = std::abs(rank_of(from) - rank_of(square));
        if (!((file_diff == 1 && rank_diff == 2) || (file_diff == 2 && rank_diff == 1))) {
            continue;
        }
        const Piece piece = board_[from];
        if (!piece.is_none() && piece.color == by_color && piece.type == PieceType::Knight) {
            return true;
        }
    }

    const std::array<int, 4> bishop_dirs = {9, 7, -9, -7};
    for (const int dir : bishop_dirs) {
        int current = square + dir;
        while (is_on_board(current) && std::abs(file_of(current) - file_of(current - dir)) == 1) {
            const Piece piece = board_[current];
            if (!piece.is_none()) {
                if (piece.color == by_color && (piece.type == PieceType::Bishop || piece.type == PieceType::Queen)) {
                    return true;
                }
                break;
            }
            current += dir;
        }
    }

    const std::array<int, 4> rook_dirs = {8, -8, 1, -1};
    for (const int dir : rook_dirs) {
        int current = square + dir;
        while (is_on_board(current) && (dir == 8 || dir == -8 || same_rank(current, current - dir))) {
            const Piece piece = board_[current];
            if (!piece.is_none()) {
                if (piece.color == by_color && (piece.type == PieceType::Rook || piece.type == PieceType::Queen)) {
                    return true;
                }
                break;
            }
            current += dir;
        }
    }

    for (int dr = -1; dr <= 1; ++dr) {
        for (int df = -1; df <= 1; ++df) {
            if (dr == 0 && df == 0) {
                continue;
            }
            const int r = rank_of(square) + dr;
            const int f = file_of(square) + df;
            if (r < 0 || r > 7 || f < 0 || f > 7) {
                continue;
            }
            const int from = r * 8 + f;
            const Piece piece = board_[from];
            if (!piece.is_none() && piece.color == by_color && piece.type == PieceType::King) {
                return true;
            }
        }
    }

    return false;
}

bool ChessBoard::is_in_check(const Color color) const {
    for (int square = 0; square < 64; ++square) {
        const Piece piece = board_[square];
        if (!piece.is_none() && piece.color == color && piece.type == PieceType::King) {
            return is_square_attacked(square, opposite_color(color));
        }
    }
    return false;
}

void ChessBoard::add_pawn_moves(const int from, std::vector<Move>& moves) const {
    const Piece pawn = board_[from];
    const int direction = pawn.color == Color::White ? 8 : -8;
    const int start_rank = pawn.color == Color::White ? 1 : 6;
    const int promotion_rank = pawn.color == Color::White ? 6 : 1;
    const int current_rank = rank_of(from);
    const int one_step = from + direction;
    if (is_on_board(one_step) && board_[one_step].is_none()) {
        if (current_rank == promotion_rank) {
            for (const PieceType promo : {PieceType::Queen, PieceType::Rook, PieceType::Bishop, PieceType::Knight}) {
                moves.push_back(Move{from, one_step, PieceType::Pawn, promo, false, false, false, false});
            }
        } else {
            moves.push_back(Move{from, one_step, PieceType::Pawn, PieceType::None, false, false, false, false});
            const int two_step = from + 2 * direction;
            if (current_rank == start_rank && board_[two_step].is_none()) {
                moves.push_back(Move{from, two_step, PieceType::Pawn, PieceType::None, false, false, false, false});
            }
        }
    }
    for (const int df : {-1, 1}) {
        const int target_file = file_of(from) + df;
        const int target_rank = current_rank + (pawn.color == Color::White ? 1 : -1);
        if (target_file < 0 || target_file > 7 || target_rank < 0 || target_rank > 7) {
            continue;
        }
        const int to = target_rank * 8 + target_file;
        const Piece target = board_[to];
        const bool is_en_passant = en_passant_target_.has_value() && *en_passant_target_ == to && target.is_none();
        if ((!target.is_none() && target.color != pawn.color) || is_en_passant) {
            if (current_rank == promotion_rank) {
                for (const PieceType promo : {PieceType::Queen, PieceType::Rook, PieceType::Bishop, PieceType::Knight}) {
                    moves.push_back(Move{from, to, PieceType::Pawn, promo, true, is_en_passant, false, false});
                }
            } else {
                moves.push_back(Move{from, to, PieceType::Pawn, PieceType::None, true, is_en_passant, false, false});
            }
        }
    }
}

void ChessBoard::add_knight_moves(const int from, std::vector<Move>& moves) const {
    const Piece piece = board_[from];
    const std::array<int, 8> deltas = {15, 17, 10, 6, -15, -17, -10, -6};
    for (const int delta : deltas) {
        const int to = from + delta;
        if (!is_on_board(to)) {
            continue;
        }
        const int file_diff = std::abs(file_of(to) - file_of(from));
        const int rank_diff = std::abs(rank_of(to) - rank_of(from));
        if (!((file_diff == 1 && rank_diff == 2) || (file_diff == 2 && rank_diff == 1))) {
            continue;
        }
        const Piece target = board_[to];
        if (target.is_none() || target.color != piece.color) {
            moves.push_back(Move{from, to, piece.type, PieceType::None, !target.is_none(), false, false, false});
        }
    }
}

void ChessBoard::add_sliding_moves(const int from, const std::vector<int>& directions, std::vector<Move>& moves) const {
    const Piece piece = board_[from];
    for (const int dir : directions) {
        int to = from + dir;
        while (is_on_board(to) && (dir == 8 || dir == -8 || std::abs(file_of(to) - file_of(to - dir)) == 1)) {
            const Piece target = board_[to];
            if (target.is_none()) {
                moves.push_back(Move{from, to, piece.type, PieceType::None, false, false, false, false});
            } else {
                if (target.color != piece.color) {
                    moves.push_back(Move{from, to, piece.type, PieceType::None, true, false, false, false});
                }
                break;
            }
            to += dir;
        }
    }
}

void ChessBoard::add_king_moves(const int from, std::vector<Move>& moves) const {
    const Piece king = board_[from];
    for (int dr = -1; dr <= 1; ++dr) {
        for (int df = -1; df <= 1; ++df) {
            if (dr == 0 && df == 0) {
                continue;
            }
            const int r = rank_of(from) + dr;
            const int f = file_of(from) + df;
            if (r < 0 || r > 7 || f < 0 || f > 7) {
                continue;
            }
            const int to = r * 8 + f;
            const Piece target = board_[to];
            if (target.is_none() || target.color != king.color) {
                moves.push_back(Move{from, to, PieceType::King, PieceType::None, !target.is_none(), false, false, false});
            }
        }
    }

    if (king.color == Color::White) {
        if (white_can_castle_kingside_ && board_[5].is_none() && board_[6].is_none() && !is_in_check(Color::White) && !is_square_attacked(5, Color::Black) && !is_square_attacked(6, Color::Black)) {
            moves.push_back(Move{4, 6, PieceType::King, PieceType::None, false, false, true, false});
        }
        if (white_can_castle_queenside_ && board_[1].is_none() && board_[2].is_none() && board_[3].is_none() && !is_in_check(Color::White) && !is_square_attacked(3, Color::Black) && !is_square_attacked(2, Color::Black)) {
            moves.push_back(Move{4, 2, PieceType::King, PieceType::None, false, false, false, true});
        }
    } else {
        if (black_can_castle_kingside_ && board_[61].is_none() && board_[62].is_none() && !is_in_check(Color::Black) && !is_square_attacked(61, Color::White) && !is_square_attacked(62, Color::White)) {
            moves.push_back(Move{60, 62, PieceType::King, PieceType::None, false, false, true, false});
        }
        if (black_can_castle_queenside_ && board_[57].is_none() && board_[58].is_none() && board_[59].is_none() && !is_in_check(Color::Black) && !is_square_attacked(59, Color::White) && !is_square_attacked(58, Color::White)) {
            moves.push_back(Move{60, 58, PieceType::King, PieceType::None, false, false, false, true});
        }
    }
}

std::vector<Move> ChessBoard::generate_pseudo_legal_moves() const {
    std::vector<Move> moves;
    for (int square = 0; square < 64; ++square) {
        const Piece piece = board_[square];
        if (piece.is_none() || piece.color != side_to_move_) {
            continue;
        }
        switch (piece.type) {
            case PieceType::Pawn: add_pawn_moves(square, moves); break;
            case PieceType::Knight: add_knight_moves(square, moves); break;
            case PieceType::Bishop: add_sliding_moves(square, {9, 7, -9, -7}, moves); break;
            case PieceType::Rook: add_sliding_moves(square, {8, -8, 1, -1}, moves); break;
            case PieceType::Queen: add_sliding_moves(square, {9, 7, -9, -7, 8, -8, 1, -1}, moves); break;
            case PieceType::King: add_king_moves(square, moves); break;
            case PieceType::None: break;
        }
    }
    return moves;
}

std::vector<Move> ChessBoard::generate_legal_moves() const {
    std::vector<Move> legal;
    for (const Move& move : generate_pseudo_legal_moves()) {
        ChessBoard next = after_move(move);
        if (!next.is_in_check(side_to_move_)) {
            legal.push_back(move);
        }
    }
    return legal;
}

ChessBoard ChessBoard::after_move(const Move& move) const {
    ChessBoard next = *this;
    next.apply_move(move);
    return next;
}

void ChessBoard::apply_move(const Move& move) {
    const Piece moving = board_[move.from];
    const Piece captured = board_[move.to];
    board_[move.from] = Piece{};
    if (move.is_en_passant) {
        const int capture_square = side_to_move_ == Color::White ? move.to - 8 : move.to + 8;
        board_[capture_square] = Piece{};
    }
    if (move.is_castling_kingside) {
        board_[move.to] = moving;
        if (moving.color == Color::White) {
            board_[7] = Piece{};
            board_[5] = Piece{PieceType::Rook, Color::White};
        } else {
            board_[63] = Piece{};
            board_[61] = Piece{PieceType::Rook, Color::Black};
        }
    } else if (move.is_castling_queenside) {
        board_[move.to] = moving;
        if (moving.color == Color::White) {
            board_[0] = Piece{};
            board_[3] = Piece{PieceType::Rook, Color::White};
        } else {
            board_[56] = Piece{};
            board_[59] = Piece{PieceType::Rook, Color::Black};
        }
    } else {
        Piece placed = moving;
        if (move.promotion != PieceType::None) {
            placed.type = move.promotion;
        }
        board_[move.to] = placed;
    }

    if (moving.type == PieceType::King) {
        if (moving.color == Color::White) {
            white_can_castle_kingside_ = false;
            white_can_castle_queenside_ = false;
        } else {
            black_can_castle_kingside_ = false;
            black_can_castle_queenside_ = false;
        }
    }
    if (moving.type == PieceType::Rook) {
        if (move.from == 0) white_can_castle_queenside_ = false;
        if (move.from == 7) white_can_castle_kingside_ = false;
        if (move.from == 56) black_can_castle_queenside_ = false;
        if (move.from == 63) black_can_castle_kingside_ = false;
    }
    if (captured.type == PieceType::Rook) {
        if (move.to == 0) white_can_castle_queenside_ = false;
        if (move.to == 7) white_can_castle_kingside_ = false;
        if (move.to == 56) black_can_castle_queenside_ = false;
        if (move.to == 63) black_can_castle_kingside_ = false;
    }

    en_passant_target_.reset();
    if (moving.type == PieceType::Pawn && std::abs(move.to - move.from) == 16) {
        en_passant_target_ = (move.to + move.from) / 2;
    }

    if (moving.type == PieceType::Pawn || move.is_capture || move.is_en_passant) {
        halfmove_clock_ = 0;
    } else {
        ++halfmove_clock_;
    }
    if (side_to_move_ == Color::Black) {
        ++fullmove_number_;
    }
    side_to_move_ = opposite_color(side_to_move_);
}

bool ChessBoard::has_any_legal_moves() const { return !generate_legal_moves().empty(); }
bool ChessBoard::is_checkmate() const { return is_in_check(side_to_move_) && !has_any_legal_moves(); }
bool ChessBoard::is_stalemate() const { return !is_in_check(side_to_move_) && !has_any_legal_moves(); }

std::string ChessBoard::to_fen() const {
    std::ostringstream out;
    for (int rank = 7; rank >= 0; --rank) {
        int empty_count = 0;
        for (int file = 0; file < 8; ++file) {
            const Piece piece = board_[rank * 8 + file];
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
    out << ' ' << (side_to_move_ == Color::White ? 'w' : 'b') << ' ';
    std::string castling;
    if (white_can_castle_kingside_) castling += 'K';
    if (white_can_castle_queenside_) castling += 'Q';
    if (black_can_castle_kingside_) castling += 'k';
    if (black_can_castle_queenside_) castling += 'q';
    out << (castling.empty() ? "-" : castling) << ' ';
    out << (en_passant_target_.has_value() ? square_to_string(*en_passant_target_) : "-") << ' ';
    out << halfmove_clock_ << ' ' << fullmove_number_;
    return out.str();
}

}  // namespace otcb
