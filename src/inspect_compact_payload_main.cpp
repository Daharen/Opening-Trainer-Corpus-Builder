#include <sqlite3.h>

#include <iostream>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <vector>
#include <cctype>
#include <cstdlib>

#include "otcb/chess_board.hpp"
#include "otcb/chess_types.hpp"
#include "otcb/position_key.hpp"

namespace {

std::vector<std::string> split(const std::string& text, char delimiter) {
    std::vector<std::string> out;
    std::string current;
    for (char ch : text) {
        if (ch == delimiter) {
            if (!current.empty()) out.push_back(current);
            current.clear();
        } else {
            current.push_back(ch);
        }
    }
    if (!current.empty()) out.push_back(current);
    return out;
}

otcb::Move move_from_uci(const otcb::ChessBoard& board, const std::string& uci) {
    otcb::Move move{};
    move.from = otcb::square_from_string(uci.substr(0, 2));
    move.to = otcb::square_from_string(uci.substr(2, 2));
    move.promotion = uci.size() == 5 ? *otcb::piece_type_from_san_letter(static_cast<char>(std::toupper(static_cast<unsigned char>(uci[4])))) : otcb::PieceType::None;
    const otcb::Piece moving_piece = board.piece_at(move.from);
    move.piece = moving_piece.type;
    move.is_capture = !board.piece_at(move.to).is_none();
    if (moving_piece.type == otcb::PieceType::Pawn && board.en_passant_target().has_value() && *board.en_passant_target() == move.to && board.piece_at(move.to).is_none() && (move.from % 8 != move.to % 8)) {
        move.is_capture = true;
        move.is_en_passant = true;
    }
    move.is_castling_kingside = moving_piece.type == otcb::PieceType::King && (move.from == 4 || move.from == 60) && (move.to == 6 || move.to == 62);
    move.is_castling_queenside = moving_piece.type == otcb::PieceType::King && (move.from == 4 || move.from == 60) && (move.to == 2 || move.to == 58);
    return move;
}

std::optional<otcb::Move> legal_move_from_uci(const otcb::ChessBoard& board, const std::string& uci) {
    const auto parsed = move_from_uci(board, uci);
    for (const auto& legal : board.generate_legal_moves()) {
        if (legal.from == parsed.from && legal.to == parsed.to && legal.promotion == parsed.promotion) {
            return legal;
        }
    }
    return std::nullopt;
}

std::string san_for_move(const otcb::ChessBoard& board, const otcb::Move& move) {
    if (move.is_castling_kingside) return "O-O";
    if (move.is_castling_queenside) return "O-O-O";

    std::string san;
    if (move.piece != otcb::PieceType::Pawn) {
        san.push_back(otcb::piece_type_to_san_letter(move.piece));
        bool need_file = false;
        bool need_rank = false;
        for (const auto& other : board.generate_legal_moves()) {
            if (other.from == move.from) continue;
            if (other.to == move.to && other.piece == move.piece) {
                if (other.from % 8 != move.from % 8) need_file = true;
                if (other.from / 8 != move.from / 8) need_rank = true;
                if (other.from % 8 == move.from % 8) need_rank = true;
                if (other.from / 8 == move.from / 8) need_file = true;
            }
        }
        if (need_file) san.push_back(static_cast<char>('a' + (move.from % 8)));
        if (need_rank) san.push_back(static_cast<char>('1' + (move.from / 8)));
    } else if (move.is_capture || move.is_en_passant) {
        san.push_back(static_cast<char>('a' + (move.from % 8)));
    }

    if (move.is_capture || move.is_en_passant) san.push_back('x');
    san += otcb::square_to_string(move.to);
    if (move.promotion != otcb::PieceType::None) {
        san.push_back('=');
        san.push_back(otcb::piece_type_to_san_letter(move.promotion));
    }
    const auto next = board.after_move(move);
    if (next.is_checkmate()) san.push_back('#');
    else if (next.is_in_check(next.side_to_move())) san.push_back('+');
    return san;
}

void usage() {
    std::cout << "Usage: inspect-compact-corpus --bundle <artifact_bundle> [--position-key <key> | --moves <uci1,uci2,...>] [--show-san]\n";
}

}  // namespace

int main(int argc, char** argv) {
    std::string bundle;
    std::string position_key;
    std::string moves_csv;
    bool show_san = false;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--bundle" && i + 1 < argc) bundle = argv[++i];
        else if (arg == "--position-key" && i + 1 < argc) position_key = argv[++i];
        else if (arg == "--moves" && i + 1 < argc) moves_csv = argv[++i];
        else if (arg == "--show-san") show_san = true;
        else if (arg == "--help") { usage(); return 0; }
        else { std::cerr << "Unknown arg: " << arg << "\n"; usage(); return 1; }
    }
    if (bundle.empty()) { usage(); return 1; }

    sqlite3* db = nullptr;
    const std::string db_path = bundle + "/data/corpus_compact.sqlite";
    if (sqlite3_open(db_path.c_str(), &db) != SQLITE_OK) {
        std::cerr << "Failed to open compact payload: " << db_path << "\n";
        return 1;
    }

    std::cout << "Manifest summary:\n";
    std::cout << "- compact payload: data/corpus_compact.sqlite\n";

    if (!moves_csv.empty()) {
        otcb::ChessBoard board;
        for (const auto& uci : split(moves_csv, ',')) {
            const auto legal = legal_move_from_uci(board, uci);
            if (!legal.has_value()) {
                std::cerr << "Illegal move in sequence: " << uci << "\n";
                sqlite3_close(db);
                return 1;
            }
            board.apply_move(*legal);
        }
        position_key = otcb::make_position_key(board, otcb::PositionKeyFormat::FenNormalized);
    }

    if (!position_key.empty()) {
        std::cout << "Position: " << position_key << "\n";

        otcb::ChessBoard board;
        if (!moves_csv.empty()) {
            for (const auto& uci : split(moves_csv, ',')) {
                board.apply_move(*legal_move_from_uci(board, uci));
            }
        }

        const std::string sql =
            "SELECT pm.raw_count, m.uci_text FROM positions p "
            "JOIN position_moves pm ON pm.position_id = p.position_id "
            "JOIN moves m ON m.move_id = pm.move_id "
            "WHERE p.position_key_inspect = '" + position_key + "' ORDER BY pm.raw_count DESC, m.uci_text ASC;";

        auto* ctx = new std::pair<otcb::ChessBoard, bool>(board, show_san);
        auto callback = [](void* ptr, int argc, char** argv, char**) -> int {
            (void)argc;
            auto* context = static_cast<std::pair<otcb::ChessBoard, bool>*>(ptr);
            const int raw = std::atoi(argv[0] ? argv[0] : "0");
            const std::string uci = argv[1] ? argv[1] : "";
            std::cout << "  - " << uci << " => " << raw;
            if (context->second) {
                const auto mv = legal_move_from_uci(context->first, uci);
                if (mv.has_value()) {
                    std::cout << " (SAN " << san_for_move(context->first, *mv) << ")";
                }
            }
            std::cout << "\n";
            return 0;
        };
        char* err = nullptr;
        if (sqlite3_exec(db, sql.c_str(), callback, ctx, &err) != SQLITE_OK) {
            std::cerr << "query failed: " << (err ? err : "unknown") << "\n";
            if (err) sqlite3_free(err);
            delete ctx;
            sqlite3_close(db);
            return 1;
        }
        delete ctx;
    }

    sqlite3_close(db);
    return 0;
}
