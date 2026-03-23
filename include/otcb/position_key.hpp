#pragma once

#include <string>

#include "otcb/chess_board.hpp"
#include "otcb/config.hpp"

namespace otcb {

std::string make_position_key(const ChessBoard& board, PositionKeyFormat format);

}  // namespace otcb
