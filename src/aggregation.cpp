#include "otcb/aggregation.hpp"

#include <algorithm>
#include <map>
#include <sstream>

#include "otcb/position_key.hpp"

namespace otcb {
namespace {

std::string json_escape(const std::string& input) {
    std::string escaped;
    escaped.reserve(input.size());
    for (const char ch : input) {
        switch (ch) {
            case '\\': escaped += "\\\\"; break;
            case '"': escaped += "\\\""; break;
            case '\n': escaped += "\\n"; break;
            case '\r': escaped += "\\r"; break;
            case '\t': escaped += "\\t"; break;
            default: escaped += ch; break;
        }
    }
    return escaped;
}

struct MutablePositionAggregate {
    std::string side_to_move;
    int total_observations = 0;
    std::map<std::string, AggregatedMoveCount> moves;
};

}  // namespace

AggregationResult aggregate_counts(const BuildConfig& config, const SourcePreflightInfo& preflight_info, const RangePlan& range_plan) {
    AggregationResult result;
    result.extraction_result = extract_openings(config, preflight_info, range_plan);
    result.summary.source_path = preflight_info.canonical_input_path.generic_string();
    result.summary.source_size_bytes = preflight_info.file_size_bytes;
    result.summary.planner_algorithm = range_plan.planner_algorithm;
    result.summary.target_range_bytes = range_plan.target_range_bytes;
    result.summary.boundary_scan_bytes = range_plan.boundary_scan_bytes;
    result.summary.rating_policy = to_string(*config.rating_policy);
    result.summary.min_rating = config.min_rating;
    result.summary.max_rating = config.max_rating;
    result.summary.retained_ply = config.retained_ply;
    result.summary.total_ranges_executed = result.extraction_result.summary.total_ranges_executed;
    result.summary.total_games_scanned = result.extraction_result.summary.total_games_scanned;
    result.summary.total_games_accepted_upstream = result.extraction_result.summary.total_games_accepted_upstream;
    result.summary.total_replay_attempts = result.extraction_result.summary.total_replay_attempts;
    result.summary.total_replay_successes = result.extraction_result.summary.total_replay_successes;
    result.summary.min_position_count = config.min_position_count;
    result.summary.position_key_format = to_string(*config.position_key_format);
    result.summary.move_key_format = to_string(*config.move_key_format);
    result.summary.notes.push_back("aggregate-counts reuses deterministic range ownership, upstream header eligibility filtering, and accepted-game SAN replay before position->move raw-count aggregation.");
    result.summary.notes.push_back("Payload contains raw counts only; shaping, sparse suppression, rare-move suppression, weighting, and trainer-side consumption remain deferred.");

    std::map<std::string, MutablePositionAggregate> aggregated;
    for (const auto& sequence : result.extraction_result.sequences) {
        for (const auto& event : sequence.ply_events) {
            if (!event.uci.has_value() || !event.fen_before.has_value()) {
                continue;
            }
            ChessBoard board;
            // Reconstruct up to this point via stored FEN semantics already produced by replay.
            // For this lane the replay pipeline always stores fen_before for extracted events.
            // We only need the board for normalized key rendering, so replay the event FEN by reusing stored board text when full format is chosen.
            // Since the board implementation does not parse FEN, rebuild by replaying the sequence prefix.
            // Deterministic cost is bounded by retained_ply and accepted games.
            ChessBoard prefix_board;
            for (const auto& prefix_event : sequence.ply_events) {
                if (prefix_event.ply_index == event.ply_index) {
                    break;
                }
                const auto resolved = prefix_event.uci.value();
                Move move{};
                move.from = square_from_string(resolved.substr(0, 2));
                move.to = square_from_string(resolved.substr(2, 2));
                move.promotion = resolved.size() == 5 ? *piece_type_from_san_letter(static_cast<char>(std::toupper(static_cast<unsigned char>(resolved[4])))) : PieceType::None;
                const Piece moving_piece = prefix_board.piece_at(move.from);
                move.piece = moving_piece.type;
                move.is_capture = !prefix_board.piece_at(move.to).is_none();
                if (moving_piece.type == PieceType::Pawn && prefix_board.en_passant_target().has_value() && *prefix_board.en_passant_target() == move.to && prefix_board.piece_at(move.to).is_none() && (move.from % 8 != move.to % 8)) {
                    move.is_capture = true;
                    move.is_en_passant = true;
                }
                move.is_castling_kingside = moving_piece.type == PieceType::King && move.from == 4 && move.to == 6;
                move.is_castling_queenside = moving_piece.type == PieceType::King && move.from == 4 && move.to == 2;
                if (moving_piece.color == Color::Black && moving_piece.type == PieceType::King) {
                    move.is_castling_kingside = move.from == 60 && move.to == 62;
                    move.is_castling_queenside = move.from == 60 && move.to == 58;
                }
                prefix_board.apply_move(move);
            }
            const std::string position_key = make_position_key(prefix_board, *config.position_key_format);
            auto& position = aggregated[position_key];
            if (position.side_to_move.empty()) {
                position.side_to_move = event.side_to_move_before;
            }
            ++position.total_observations;
            auto& move_count = position.moves[*event.uci];
            move_count.move_key = *event.uci;
            ++move_count.raw_count;
            if (move_count.example_san.empty()) {
                move_count.example_san = event.san;
            }
            ++result.summary.total_extracted_ply_events_consumed;
        }
    }

    for (const auto& [position_key, position] : aggregated) {
        if (position.total_observations < config.min_position_count) {
            ++result.summary.positions_filtered_by_min_count;
            result.summary.observations_filtered_by_min_count += position.total_observations;
            continue;
        }
        AggregatedPositionRecord record;
        record.position_key = position_key;
        record.side_to_move = position.side_to_move;
        record.total_observations = position.total_observations;
        for (const auto& [move_key, move] : position.moves) {
            (void)move_key;
            record.candidate_moves.push_back(move);
        }
        std::sort(record.candidate_moves.begin(), record.candidate_moves.end(), [](const auto& lhs, const auto& rhs) {
            if (lhs.raw_count != rhs.raw_count) {
                return lhs.raw_count > rhs.raw_count;
            }
            return lhs.move_key < rhs.move_key;
        });
        record.candidate_move_count = static_cast<int>(record.candidate_moves.size());
        result.summary.total_aggregate_move_entries_emitted += record.candidate_move_count;
        result.summary.total_raw_observations_emitted += record.total_observations;
        result.positions.push_back(record);
    }

    result.summary.total_unique_positions_emitted = static_cast<int>(result.positions.size());
    if (config.emit_aggregate_preview) {
        const int limit = config.aggregate_preview_limit;
        for (const auto& record : result.positions) {
            if (limit >= 0 && static_cast<int>(result.preview_rows.size()) >= limit) {
                break;
            }
            result.preview_rows.push_back(record);
        }
    }
    result.summary.preview_row_count_emitted = static_cast<int>(result.preview_rows.size());
    return result;
}

std::string render_aggregated_position_move_counts_jsonl(const std::vector<AggregatedPositionRecord>& positions, const BuildConfig& config) {
    std::ostringstream out;
    for (const auto& record : positions) {
        out << '{'
            << "\"position_key\":\"" << json_escape(record.position_key) << "\"," 
            << "\"position_key_format\":\"" << json_escape(to_string(*config.position_key_format)) << "\"," 
            << "\"side_to_move\":\"" << json_escape(record.side_to_move) << "\"," 
            << "\"candidate_move_count\":" << record.candidate_move_count << ','
            << "\"total_observations\":" << record.total_observations << ','
            << "\"candidate_moves\":[";
        for (std::size_t i = 0; i < record.candidate_moves.size(); ++i) {
            const auto& move = record.candidate_moves[i];
            out << '{'
                << "\"move_key\":\"" << json_escape(move.move_key) << "\"," 
                << "\"move_key_format\":\"" << json_escape(to_string(*config.move_key_format)) << "\"," 
                << "\"raw_count\":" << move.raw_count;
            if (!move.example_san.empty()) {
                out << ",\"example_san\":\"" << json_escape(move.example_san) << "\"";
            }
            out << '}';
            if (i + 1 < record.candidate_moves.size()) {
                out << ',';
            }
        }
        out << "]}\n";
    }
    return out.str();
}

std::string render_aggregate_preview_jsonl(const std::vector<AggregatedPositionRecord>& preview_rows, const BuildConfig& config) {
    std::ostringstream out;
    for (const auto& record : preview_rows) {
        out << '{'
            << "\"position_key\":\"" << json_escape(record.position_key) << "\"," 
            << "\"position_key_format\":\"" << json_escape(to_string(*config.position_key_format)) << "\"," 
            << "\"side_to_move\":\"" << json_escape(record.side_to_move) << "\"," 
            << "\"total_observations\":" << record.total_observations << ','
            << "\"top_candidate_moves\":[";
        for (std::size_t i = 0; i < record.candidate_moves.size(); ++i) {
            if (i == 3) break;
            const auto& move = record.candidate_moves[i];
            out << '{' << "\"move_key\":\"" << json_escape(move.move_key) << "\",\"raw_count\":" << move.raw_count << '}';
            if (i + 1 < record.candidate_moves.size() && i < 2) out << ',';
        }
        out << "]}\n";
    }
    return out.str();
}

std::string render_aggregation_summary_json(const AggregationSummary& summary) {
    std::ostringstream out;
    out << "{\n"
        << "  \"source_path\": \"" << json_escape(summary.source_path) << "\",\n"
        << "  \"source_size_bytes\": " << summary.source_size_bytes << ",\n"
        << "  \"rating_policy\": \"" << json_escape(summary.rating_policy) << "\",\n"
        << "  \"rating_bounds\": {\"min\": " << summary.min_rating << ", \"max\": " << summary.max_rating << "},\n"
        << "  \"retained_ply\": " << summary.retained_ply << ",\n"
        << "  \"total_ranges_executed\": " << summary.total_ranges_executed << ",\n"
        << "  \"total_games_scanned\": " << summary.total_games_scanned << ",\n"
        << "  \"total_games_accepted_upstream\": " << summary.total_games_accepted_upstream << ",\n"
        << "  \"total_replay_attempts\": " << summary.total_replay_attempts << ",\n"
        << "  \"total_replay_successes\": " << summary.total_replay_successes << ",\n"
        << "  \"total_extracted_ply_events_consumed\": " << summary.total_extracted_ply_events_consumed << ",\n"
        << "  \"total_unique_positions_emitted\": " << summary.total_unique_positions_emitted << ",\n"
        << "  \"total_aggregate_move_entries_emitted\": " << summary.total_aggregate_move_entries_emitted << ",\n"
        << "  \"total_raw_observations_emitted\": " << summary.total_raw_observations_emitted << ",\n"
        << "  \"min_position_count\": " << summary.min_position_count << ",\n"
        << "  \"notes\": [\n";
    for (std::size_t i = 0; i < summary.notes.size(); ++i) {
        out << "    \"" << json_escape(summary.notes[i]) << "\"" << (i + 1 < summary.notes.size() ? "," : "") << "\n";
    }
    out << "  ]\n}\n";
    return out.str();
}

std::string render_aggregation_summary_text(const BuildConfig& config, const AggregationSummary& summary) {
    std::ostringstream out;
    out << "selected mode: aggregate-counts\n";
    out << "source path: " << summary.source_path << "\n";
    out << "source size bytes: " << summary.source_size_bytes << "\n";
    out << "rating policy: " << summary.rating_policy << "\n";
    out << "rating bounds: [" << summary.min_rating << ", " << summary.max_rating << "]\n";
    out << "retained ply: " << summary.retained_ply << "\n";
    out << "position key format: " << summary.position_key_format << "\n";
    out << "move key format: " << summary.move_key_format << "\n";
    out << "total accepted games: " << summary.total_games_accepted_upstream << "\n";
    out << "total replay successes: " << summary.total_replay_successes << "\n";
    out << "total extracted ply events consumed: " << summary.total_extracted_ply_events_consumed << "\n";
    out << "total unique positions emitted: " << summary.total_unique_positions_emitted << "\n";
    out << "total raw observations emitted: " << summary.total_raw_observations_emitted << "\n";
    out << "min-position-count filtering impact: positions_filtered=" << summary.positions_filtered_by_min_count << ", observations_filtered=" << summary.observations_filtered_by_min_count << "\n";
    out << "aggregate preview rows emitted: " << summary.preview_row_count_emitted << "\n";
    out << "note: shaping, suppression, weighting, and trainer-side consumption remain deferred.\n";
    return out.str();
}

}  // namespace otcb
