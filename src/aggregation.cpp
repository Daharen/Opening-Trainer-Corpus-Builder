#include "otcb/aggregation.hpp"

#include <algorithm>
#include <map>
#include <sstream>

#include "otcb/position_key.hpp"
#include "otcb/progress.hpp"

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

struct PredecessorCandidateAggregate {
    std::string parent_position_key;
    std::string incoming_move_uci;
    int depth_from_root = 0;
    int edge_support_count = 0;
};

struct PositionContext {
    int depth_from_root = 0;
    std::string side_to_move;
};

}  // namespace

AggregationResult aggregate_counts(const BuildConfig& config, const SourcePreflightInfo& preflight_info, const RangePlan& range_plan, ProgressReporter* progress) {
    AggregationResult result;
    result.extraction_result = extract_openings(config, preflight_info, range_plan, progress);
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
    const auto& rejection_counts = result.extraction_result.scan_result.summary.global_rejection_counts;
    auto rejection_value = [&](const std::string& key) {
        const auto it = rejection_counts.find(key);
        return it == rejection_counts.end() ? 0 : it->second;
    };
    result.summary.games_rejected_by_rating_filter =
        rejection_value("rejected_missing_white_elo") +
        rejection_value("rejected_missing_black_elo") +
        rejection_value("rejected_invalid_white_elo") +
        rejection_value("rejected_invalid_black_elo") +
        rejection_value("rejected_policy_mismatch");
    result.summary.games_rejected_by_time_control_filter = rejection_value("rejected_time_control_mismatch");
    result.summary.games_rejected_invalid_time_control = rejection_value("rejected_invalid_time_control");
    result.summary.total_replay_attempts = result.extraction_result.summary.total_replay_attempts;
    result.summary.total_replay_successes = result.extraction_result.summary.total_replay_successes;
    result.summary.min_position_count = config.min_position_count;
    result.summary.position_key_format = to_string(*config.position_key_format);
    result.summary.move_key_format = to_string(*config.move_key_format);
    result.summary.payload_format = to_string(config.payload_format);
    result.summary.payload_path = config.payload_format == PayloadFormat::Sqlite
        ? "data/corpus.sqlite"
        : (config.payload_format == PayloadFormat::ExactSqliteV2Compact ? "data/corpus_compact.sqlite" : "data/aggregated_position_move_counts.jsonl");
    result.summary.notes.push_back("aggregate-counts reuses deterministic range ownership, upstream header eligibility filtering, and accepted-game SAN replay before position->move raw-count aggregation.");
    result.summary.notes.push_back("Payload contains raw counts only; shaping, sparse suppression, rare-move suppression, weighting, and trainer-side consumption remain deferred.");

    if (progress) {
        progress->stage_started(ProgressStage::AggregateCounts, "aggregating extracted ply events into position counts", preflight_info.file_size_bytes);
        progress->update([&](ProgressSnapshot& snapshot) {
            snapshot.replay_attempts = result.extraction_result.summary.total_replay_attempts;
            snapshot.replay_successes = result.extraction_result.summary.total_replay_successes;
            snapshot.replay_failures = result.extraction_result.summary.total_replay_failures;
            snapshot.extracted_plies = result.extraction_result.summary.total_extracted_plies;
            snapshot.games_scanned = result.extraction_result.summary.total_games_scanned;
            snapshot.games_accepted = result.extraction_result.summary.total_games_accepted_upstream;
            snapshot.aggregated_positions = 0;
            snapshot.raw_observations = 0;
            snapshot.aggregate_move_entries = 0;
        });
    }

    std::map<std::string, MutablePositionAggregate> aggregated;
    std::map<std::string, std::vector<PredecessorCandidateAggregate>> predecessor_candidates_by_child;
    std::map<std::string, PositionContext> position_context_by_key;
    const std::string selection_policy_version = "canonical_predecessor_policy_v1";
    for (const auto& sequence : result.extraction_result.sequences) {
        ChessBoard board;
        auto root_it = position_context_by_key.find(make_position_key(board, *config.position_key_format));
        if (root_it == position_context_by_key.end()) {
            position_context_by_key.emplace(
                make_position_key(board, *config.position_key_format),
                PositionContext{.depth_from_root = 0, .side_to_move = "white"});
        }
        for (const auto& event : sequence.ply_events) {
            if (!event.uci.has_value() || !event.fen_before.has_value()) {
                continue;
            }
            const std::string parent_position_key = make_position_key(board, *config.position_key_format);
            auto& position = aggregated[parent_position_key];
            if (position.side_to_move.empty()) {
                position.side_to_move = event.side_to_move_before;
            }
            ++position.total_observations;
            auto& move_count = position.moves[*event.uci];
            move_count.move_key = *event.uci;
            ++move_count.raw_count;
            if (config.payload_format != PayloadFormat::ExactSqliteV2Compact && move_count.example_san.empty()) {
                move_count.example_san = event.san;
            }

            Move move{};
            move.from = square_from_string(event.uci->substr(0, 2));
            move.to = square_from_string(event.uci->substr(2, 2));
            move.promotion = event.uci->size() == 5 ? *piece_type_from_san_letter(static_cast<char>(std::toupper(static_cast<unsigned char>((*event.uci)[4])))) : PieceType::None;
            const Piece moving_piece = board.piece_at(move.from);
            move.piece = moving_piece.type;
            move.is_capture = !board.piece_at(move.to).is_none();
            if (moving_piece.type == PieceType::Pawn && board.en_passant_target().has_value() && *board.en_passant_target() == move.to && board.piece_at(move.to).is_none() && (move.from % 8 != move.to % 8)) {
                move.is_capture = true;
                move.is_en_passant = true;
            }
            move.is_castling_kingside = moving_piece.type == PieceType::King && move.from == 4 && move.to == 6;
            move.is_castling_queenside = moving_piece.type == PieceType::King && move.from == 4 && move.to == 2;
            if (moving_piece.color == Color::Black && moving_piece.type == PieceType::King) {
                move.is_castling_kingside = move.from == 60 && move.to == 62;
                move.is_castling_queenside = move.from == 60 && move.to == 58;
            }
            board.apply_move(move);

            const std::string child_position_key = make_position_key(board, *config.position_key_format);
            const int child_depth = event.ply_index;
            auto& child_context = position_context_by_key[child_position_key];
            if (child_context.side_to_move.empty() || child_depth < child_context.depth_from_root) {
                child_context.depth_from_root = child_depth;
                child_context.side_to_move = board.side_to_move() == Color::White ? "white" : "black";
            }
            auto& candidates = predecessor_candidates_by_child[child_position_key];
            auto candidate_it = std::find_if(candidates.begin(), candidates.end(), [&](const PredecessorCandidateAggregate& entry) {
                return entry.parent_position_key == parent_position_key && entry.incoming_move_uci == *event.uci && entry.depth_from_root == child_depth;
            });
            if (candidate_it == candidates.end()) {
                candidates.push_back(PredecessorCandidateAggregate{
                    .parent_position_key = parent_position_key,
                    .incoming_move_uci = *event.uci,
                    .depth_from_root = child_depth,
                    .edge_support_count = 1,
                });
            } else {
                ++candidate_it->edge_support_count;
            }
            ++result.summary.total_extracted_ply_events_consumed;
            if (progress) {
                progress->update([&](ProgressSnapshot& snapshot) {
                    snapshot.raw_observations = result.summary.total_extracted_ply_events_consumed;
                    snapshot.aggregated_positions = static_cast<int>(aggregated.size());
                    const auto seconds = std::max(1.0, std::chrono::duration<double>(std::chrono::steady_clock::now() - snapshot.stage_started_at).count());
                    snapshot.throughput_per_second = snapshot.raw_observations / seconds;
                    snapshot.last_event_message = "aggregation still active";
                });
            }
        }
    }
    result.summary.unique_child_positions_with_predecessor_candidates = static_cast<int>(predecessor_candidates_by_child.size());
    if (config.emit_canonical_predecessors) {
        if (progress) {
            progress->note_event("canonical predecessor reduction started");
        }
        const std::string root_position_key = make_position_key(ChessBoard{}, *config.position_key_format);
        if (position_context_by_key.find(root_position_key) == position_context_by_key.end()) {
            position_context_by_key.emplace(root_position_key, PositionContext{.depth_from_root = 0, .side_to_move = "white"});
        }
        result.canonical_predecessors.push_back(CanonicalPredecessorRecord{
            .position_key = root_position_key,
            .side_to_move = "white",
            .depth_from_root = 0,
            .parent_position_key = std::nullopt,
            .incoming_move_uci = std::nullopt,
            .edge_support_count = 0,
            .selection_policy_version = selection_policy_version,
        });
        for (const auto& [child_position_key, candidates] : predecessor_candidates_by_child) {
            if (candidates.empty()) {
                continue;
            }
            // Deterministic canonical predecessor policy:
            // 1) lower depth_from_root, 2) higher edge_support_count,
            // 3) lexicographically smaller parent_position_key, 4) lexicographically smaller incoming_move_uci.
            const auto best_it = std::min_element(candidates.begin(), candidates.end(), [](const auto& lhs, const auto& rhs) {
                if (lhs.depth_from_root != rhs.depth_from_root) {
                    return lhs.depth_from_root < rhs.depth_from_root;
                }
                if (lhs.edge_support_count != rhs.edge_support_count) {
                    return lhs.edge_support_count > rhs.edge_support_count;
                }
                if (lhs.parent_position_key != rhs.parent_position_key) {
                    return lhs.parent_position_key < rhs.parent_position_key;
                }
                return lhs.incoming_move_uci < rhs.incoming_move_uci;
            });
            const auto context_it = position_context_by_key.find(child_position_key);
            const std::string child_side_to_move = context_it == position_context_by_key.end() ? "unknown" : context_it->second.side_to_move;
            const int child_depth = context_it == position_context_by_key.end() ? best_it->depth_from_root : context_it->second.depth_from_root;
            result.canonical_predecessors.push_back(CanonicalPredecessorRecord{
                .position_key = child_position_key,
                .side_to_move = child_side_to_move,
                .depth_from_root = child_depth,
                .parent_position_key = best_it->parent_position_key,
                .incoming_move_uci = best_it->incoming_move_uci,
                .edge_support_count = best_it->edge_support_count,
                .selection_policy_version = selection_policy_version,
            });
        }
        result.summary.canonical_predecessor_edges_emitted = static_cast<int>(result.canonical_predecessors.size());
        result.summary.canonical_predecessor_payload_file = "data/canonical_predecessor_edges.sqlite";
        result.summary.canonical_predecessor_payload_format = "sqlite";
        result.summary.canonical_predecessor_payload_contract_version = "1";
        result.summary.canonical_predecessor_selection_policy = selection_policy_version;
        result.summary.canonical_predecessor_emitted = true;
        if (progress) {
            progress->note_event("canonical predecessor reduction completed");
        }
    } else {
        result.summary.canonical_predecessor_selection_policy = selection_policy_version;
        result.summary.canonical_predecessor_emitted = false;
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
    if (progress) {
        progress->update([&](ProgressSnapshot& snapshot) {
            snapshot.aggregated_positions = result.summary.total_unique_positions_emitted;
            snapshot.raw_observations = result.summary.total_raw_observations_emitted;
            snapshot.aggregate_move_entries = result.summary.total_aggregate_move_entries_emitted;
            snapshot.percent_complete = 100.0;
        });
        progress->stage_completed("aggregation completed");
    }
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
        << "  \"games_rejected_by_rating_filter\": " << summary.games_rejected_by_rating_filter << ",\n"
        << "  \"games_rejected_by_time_control_filter\": " << summary.games_rejected_by_time_control_filter << ",\n"
        << "  \"games_rejected_invalid_time_control\": " << summary.games_rejected_invalid_time_control << ",\n"
        << "  \"total_replay_attempts\": " << summary.total_replay_attempts << ",\n"
        << "  \"total_replay_successes\": " << summary.total_replay_successes << ",\n"
        << "  \"total_extracted_ply_events_consumed\": " << summary.total_extracted_ply_events_consumed << ",\n"
        << "  \"total_unique_positions_emitted\": " << summary.total_unique_positions_emitted << ",\n"
        << "  \"unique_child_positions_with_predecessor_candidates\": " << summary.unique_child_positions_with_predecessor_candidates << ",\n"
        << "  \"canonical_predecessor_edges_emitted\": " << summary.canonical_predecessor_edges_emitted << ",\n"
        << "  \"total_aggregate_move_entries_emitted\": " << summary.total_aggregate_move_entries_emitted << ",\n"
        << "  \"total_raw_observations_emitted\": " << summary.total_raw_observations_emitted << ",\n"
        << "  \"min_position_count\": " << summary.min_position_count << ",\n"
        << "  \"payload_format\": \"" << json_escape(summary.payload_format) << "\",\n"
        << "  \"payload_path\": \"" << json_escape(summary.payload_path) << "\",\n"
        << "  \"sqlite_positions_rows\": " << summary.sqlite_positions_rows << ",\n"
        << "  \"sqlite_moves_rows\": " << summary.sqlite_moves_rows << ",\n"
        << "  \"sqlite_position_moves_rows\": " << summary.sqlite_position_moves_rows << ",\n"
        << "  \"canonical_payload_file\": \"" << json_escape(summary.canonical_payload_file) << "\",\n"
        << "  \"compatibility_payload_file\": \"" << json_escape(summary.compatibility_payload_file) << "\",\n"
        << "  \"compatibility_mirror_emitted\": " << (summary.compatibility_mirror_emitted ? "true" : "false") << ",\n"
        << "  \"canonical_payload_size_bytes\": " << summary.canonical_payload_size_bytes << ",\n"
        << "  \"compatibility_payload_size_bytes\": " << summary.compatibility_payload_size_bytes << ",\n"
        << "  \"canonical_predecessor_payload_file\": \"" << json_escape(summary.canonical_predecessor_payload_file) << "\",\n"
        << "  \"canonical_predecessor_payload_format\": \"" << json_escape(summary.canonical_predecessor_payload_format) << "\",\n"
        << "  \"canonical_predecessor_payload_contract_version\": \"" << json_escape(summary.canonical_predecessor_payload_contract_version) << "\",\n"
        << "  \"canonical_predecessor_selection_policy\": \"" << json_escape(summary.canonical_predecessor_selection_policy) << "\",\n"
        << "  \"canonical_predecessor_emitted\": " << (summary.canonical_predecessor_emitted ? "true" : "false") << ",\n"
        << "  \"canonical_predecessor_single_parent_per_position\": " << (summary.canonical_predecessor_single_parent_per_position ? "true" : "false") << ",\n"
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
    out << "payload format: " << summary.payload_format << "\n";
    out << "payload path: " << summary.payload_path << "\n";
    out << "total accepted games: " << summary.total_games_accepted_upstream << "\n";
    out << "games rejected by rating filter: " << summary.games_rejected_by_rating_filter << "\n";
    out << "games rejected by time-control mismatch: " << summary.games_rejected_by_time_control_filter << "\n";
    out << "games rejected by invalid/missing time-control: " << summary.games_rejected_invalid_time_control << "\n";
    out << "total replay successes: " << summary.total_replay_successes << "\n";
    out << "total extracted ply events consumed: " << summary.total_extracted_ply_events_consumed << "\n";
    out << "total unique positions emitted: " << summary.total_unique_positions_emitted << "\n";
    out << "unique child positions with predecessor candidates: " << summary.unique_child_positions_with_predecessor_candidates << "\n";
    out << "final canonical predecessor edges emitted: " << summary.canonical_predecessor_edges_emitted << "\n";
    out << "total raw observations emitted: " << summary.total_raw_observations_emitted << "\n";
    out << "min-position-count filtering impact: positions_filtered=" << summary.positions_filtered_by_min_count << ", observations_filtered=" << summary.observations_filtered_by_min_count << "\n";
    out << "aggregate preview rows emitted: " << summary.preview_row_count_emitted << "\n";
    if (summary.payload_format == "sqlite" || summary.payload_format == "exact_sqlite_v2_compact") {
        out << "sqlite rows: positions=" << summary.sqlite_positions_rows << ", moves=" << summary.sqlite_moves_rows << ", position_moves=" << summary.sqlite_position_moves_rows << "\n";
    }
    out << "note: shaping, suppression, weighting, and trainer-side consumption remain deferred.\n";
    return out.str();
}

}  // namespace otcb
