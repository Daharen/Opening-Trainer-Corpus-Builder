#include "otcb/opening_extraction.hpp"

#include <fstream>
#include <sstream>

#include "otcb/chess_board.hpp"
#include "otcb/chess_types.hpp"
#include "otcb/manifest.hpp"
#include "otcb/san_replay.hpp"
#include "otcb/san_tokenizer.hpp"

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

std::string optional_text(const std::optional<std::string>& value) {
    return value.value_or("");
}

std::string extract_game_movetext(std::ifstream& input, const GameEnvelope& envelope) {
    if (envelope.movetext_end_byte < envelope.movetext_start_byte) {
        return "";
    }
    const std::uint64_t length = envelope.movetext_end_byte - envelope.movetext_start_byte + 1;
    std::string movetext(length, '\0');
    input.clear();
    input.seekg(static_cast<std::streamoff>(envelope.movetext_start_byte), std::ios::beg);
    input.read(movetext.data(), static_cast<std::streamsize>(length));
    movetext.resize(static_cast<std::size_t>(input.gcount()));
    return movetext;
}

ExtractionFailureReason map_failure_reason(const ReplayFailureReason reason) {
    switch (reason) {
        case ReplayFailureReason::None: return ExtractionFailureReason::None;
        case ReplayFailureReason::UnsupportedMovetextToken: return ExtractionFailureReason::UnsupportedMovetextToken;
        case ReplayFailureReason::IllegalSanMove: return ExtractionFailureReason::IllegalSanMove;
        case ReplayFailureReason::AmbiguousSanResolutionFailed: return ExtractionFailureReason::AmbiguousSanResolutionFailed;
        case ReplayFailureReason::PromotionParseFailed: return ExtractionFailureReason::PromotionParseFailed;
        case ReplayFailureReason::CastlingParseFailed: return ExtractionFailureReason::CastlingParseFailed;
        case ReplayFailureReason::UnexpectedReplayState: return ExtractionFailureReason::UnexpectedReplayState;
    }
    return ExtractionFailureReason::UnexpectedReplayState;
}

std::string side_to_string(const Color color) {
    return color == Color::White ? "white" : "black";
}

std::string termination_state(const ChessBoard& board) {
    if (board.is_checkmate()) {
        return "checkmate";
    }
    if (board.is_stalemate()) {
        return "stalemate";
    }
    return "";
}

}  // namespace

std::string to_string(const ExtractionFailureReason reason) {
    switch (reason) {
        case ExtractionFailureReason::None: return "none";
        case ExtractionFailureReason::TokenizationFailed: return "tokenization_failed";
        case ExtractionFailureReason::UnsupportedMovetextToken: return "unsupported_movetext_token";
        case ExtractionFailureReason::IllegalSanMove: return "illegal_san_move";
        case ExtractionFailureReason::AmbiguousSanResolutionFailed: return "ambiguous_san_resolution_failed";
        case ExtractionFailureReason::PromotionParseFailed: return "promotion_parse_failed";
        case ExtractionFailureReason::CastlingParseFailed: return "castling_parse_failed";
        case ExtractionFailureReason::GameTerminatedBeforeRetainedPly: return "game_terminated_before_retained_ply";
        case ExtractionFailureReason::HeaderScanRejectedUpstream: return "header_scan_rejected_upstream";
        case ExtractionFailureReason::UnexpectedReplayState: return "unexpected_replay_state";
    }
    return "unknown";
}

ExtractionResult extract_openings(const BuildConfig& config, const SourcePreflightInfo& preflight_info, const RangePlan& range_plan) {
    ExtractionResult result;
    result.scan_result = scan_headers(config, preflight_info, range_plan);
    result.summary.source_path = preflight_info.canonical_input_path.generic_string();
    result.summary.source_size_bytes = preflight_info.file_size_bytes;
    result.summary.planner_algorithm = range_plan.planner_algorithm;
    result.summary.target_range_bytes = range_plan.target_range_bytes;
    result.summary.boundary_scan_bytes = range_plan.boundary_scan_bytes;
    result.summary.rating_policy = to_string(*config.rating_policy);
    result.summary.min_rating = config.min_rating;
    result.summary.max_rating = config.max_rating;
    result.summary.retained_ply = config.retained_ply;
    result.summary.total_ranges_executed = result.scan_result.summary.total_ranges_executed;
    result.summary.total_games_scanned = result.scan_result.summary.total_games_scanned;
    result.summary.total_games_accepted_upstream = result.scan_result.summary.total_games_accepted;
    result.summary.notes.push_back("extract-openings reuses deterministic range ownership and upstream header eligibility filtering before SAN replay.");
    result.summary.notes.push_back("Cross-game aggregation into final position-to-move counts remains explicitly deferred in this lane.");
    if (config.strict_header_scan) {
        result.summary.notes.push_back("Strict header scanning remained enabled during the upstream accepted-game filter.");
    }

    std::ifstream input(preflight_info.canonical_input_path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("Failed to open PGN for opening extraction: " + preflight_info.canonical_input_path.string());
    }

    for (const GameEnvelope& envelope : result.scan_result.accepted_games) {
        ExtractedOpeningSequence sequence;
        sequence.range_index = envelope.range_index;
        sequence.header_start_byte = envelope.header_start_byte;
        sequence.game_end_byte = envelope.game_end_byte;
        sequence.white_elo = envelope.headers.white_elo;
        sequence.black_elo = envelope.headers.black_elo;
        sequence.result = optional_text(envelope.headers.result);
        sequence.event = optional_text(envelope.headers.event);
        sequence.site = optional_text(envelope.headers.site);
        sequence.time_control = optional_text(envelope.headers.time_control);
        sequence.classification = to_string(envelope.classification);
        sequence.failure_reason = to_string(ExtractionFailureReason::None);
        sequence.plies_requested = config.retained_ply;
        ++result.summary.total_replay_attempts;

        const std::string movetext = extract_game_movetext(input, envelope);
        const auto tokenized = tokenize_movetext(movetext);
        if (!tokenized.success) {
            sequence.failure_reason = to_string(ExtractionFailureReason::TokenizationFailed);
            ++result.summary.total_replay_failures;
            ++result.summary.replay_failure_counts[sequence.failure_reason];
            result.sequences.push_back(sequence);
            continue;
        }

        ChessBoard board;
        bool replay_failed = false;
        for (std::size_t token_index = 0; token_index < tokenized.san_tokens.size() && sequence.plies_extracted < config.retained_ply; ++token_index) {
            const std::string& san = tokenized.san_tokens[token_index];
            const std::string fen_before = board.to_fen();
            const Color stm = board.side_to_move();
            const auto resolved = resolve_san_move(board, san);
            if (!resolved.success || !resolved.move.has_value()) {
                sequence.failure_reason = to_string(map_failure_reason(resolved.failure_reason));
                replay_failed = true;
                break;
            }

            ExtractedPlyEvent event;
            event.ply_index = sequence.plies_extracted + 1;
            event.san = san;
            event.uci = move_to_uci(*resolved.move);
            event.fen_before = fen_before;
            event.side_to_move_before = side_to_string(stm);
            event.move_was_legal = true;

            board.apply_move(*resolved.move);
            const std::string termination = termination_state(board);
            event.fen_after = board.to_fen();
            if (!termination.empty()) {
                event.termination_after_move = termination;
            }
            sequence.ply_events.push_back(event);
            ++sequence.plies_extracted;
            ++result.summary.total_extracted_plies;
            if (!termination.empty()) {
                break;
            }
        }

        if (replay_failed) {
            ++result.summary.total_replay_failures;
            ++result.summary.replay_failure_counts[sequence.failure_reason];
        } else if (sequence.plies_extracted >= config.retained_ply) {
            sequence.replayed_successfully = true;
            ++result.summary.total_replay_successes;
        } else {
            sequence.failure_reason = to_string(ExtractionFailureReason::GameTerminatedBeforeRetainedPly);
            ++result.summary.total_replay_failures;
            ++result.summary.replay_failure_counts[sequence.failure_reason];
        }

        result.sequences.push_back(sequence);
        const bool emit_preview = config.emit_extraction_preview &&
            (config.extraction_preview_limit < 0 || static_cast<int>(result.preview_rows.size()) < config.extraction_preview_limit);
        if (emit_preview) {
            result.preview_rows.push_back(sequence);
        }
    }

    result.summary.preview_row_count_emitted = static_cast<int>(result.preview_rows.size());
    return result;
}

std::string render_extracted_opening_sequences_jsonl(const std::vector<ExtractedOpeningSequence>& sequences, const bool include_fen_snapshots, const bool include_uci_moves) {
    std::ostringstream out;
    for (const auto& sequence : sequences) {
        out << "{"
            << "\"range_index\":" << sequence.range_index << ','
            << "\"header_start_byte\":" << sequence.header_start_byte << ','
            << "\"game_end_byte\":" << sequence.game_end_byte << ','
            << "\"white_elo\":" << sequence.white_elo.value_or(-1) << ','
            << "\"black_elo\":" << sequence.black_elo.value_or(-1) << ','
            << "\"result\":\"" << json_escape(sequence.result) << "\"," 
            << "\"event\":\"" << json_escape(sequence.event) << "\"," 
            << "\"site\":\"" << json_escape(sequence.site) << "\"," 
            << "\"time_control\":\"" << json_escape(sequence.time_control) << "\"," 
            << "\"classification\":\"" << json_escape(sequence.classification) << "\"," 
            << "\"plies_requested\":" << sequence.plies_requested << ','
            << "\"plies_extracted\":" << sequence.plies_extracted << ','
            << "\"replayed_successfully\":" << (sequence.replayed_successfully ? "true" : "false") << ','
            << "\"failure_reason\":\"" << json_escape(sequence.failure_reason) << "\"," 
            << "\"ply_events\":[";
        for (std::size_t i = 0; i < sequence.ply_events.size(); ++i) {
            const auto& event = sequence.ply_events[i];
            out << '{'
                << "\"ply_index\":" << event.ply_index << ','
                << "\"san\":\"" << json_escape(event.san) << "\"," 
                << "\"side_to_move_before\":\"" << json_escape(event.side_to_move_before) << "\"," 
                << "\"move_was_legal\":" << (event.move_was_legal ? "true" : "false");
            if (include_uci_moves && event.uci.has_value()) {
                out << ",\"uci\":\"" << json_escape(*event.uci) << "\"";
            }
            if (include_fen_snapshots && event.fen_before.has_value()) {
                out << ",\"fen_before\":\"" << json_escape(*event.fen_before) << "\"";
            }
            if (include_fen_snapshots && event.fen_after.has_value()) {
                out << ",\"fen_after\":\"" << json_escape(*event.fen_after) << "\"";
            }
            if (event.termination_after_move.has_value()) {
                out << ",\"termination_after_move\":\"" << json_escape(*event.termination_after_move) << "\"";
            }
            out << '}';
            if (i + 1 < sequence.ply_events.size()) {
                out << ',';
            }
        }
        out << "]}\n";
    }
    return out.str();
}

std::string render_extraction_preview_jsonl(const std::vector<ExtractedOpeningSequence>& preview_rows) {
    std::ostringstream out;
    for (const auto& sequence : preview_rows) {
        out << '{'
            << "\"range_index\":" << sequence.range_index << ','
            << "\"header_start_byte\":" << sequence.header_start_byte << ','
            << "\"white_elo\":" << sequence.white_elo.value_or(-1) << ','
            << "\"black_elo\":" << sequence.black_elo.value_or(-1) << ','
            << "\"classification\":\"" << json_escape(sequence.classification) << "\"," 
            << "\"plies_extracted\":" << sequence.plies_extracted << ','
            << "\"replayed_successfully\":" << (sequence.replayed_successfully ? "true" : "false") << ','
            << "\"failure_reason\":\"" << json_escape(sequence.failure_reason) << "\"," 
            << "\"san\":[";
        for (std::size_t i = 0; i < sequence.ply_events.size(); ++i) {
            out << '"' << json_escape(sequence.ply_events[i].san) << '"';
            if (i + 1 < sequence.ply_events.size()) {
                out << ',';
            }
        }
        out << "]}\n";
    }
    return out.str();
}

std::string render_extraction_summary_json(const ExtractionSummary& summary) {
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
        << "  \"total_replay_failures\": " << summary.total_replay_failures << ",\n"
        << "  \"total_extracted_plies\": " << summary.total_extracted_plies << ",\n"
        << "  \"replay_failure_counts\": {\n";
    for (auto it = summary.replay_failure_counts.begin(); it != summary.replay_failure_counts.end(); ++it) {
        out << "    \"" << json_escape(it->first) << "\": " << it->second;
        out << (std::next(it) != summary.replay_failure_counts.end() ? "," : "") << "\n";
    }
    out << "  },\n  \"notes\": [\n";
    for (std::size_t i = 0; i < summary.notes.size(); ++i) {
        out << "    \"" << json_escape(summary.notes[i]) << "\"";
        out << (i + 1 < summary.notes.size() ? "," : "") << "\n";
    }
    out << "  ]\n}\n";
    return out.str();
}

std::string render_extraction_summary_text(const BuildConfig& config, const ExtractionSummary& summary) {
    std::ostringstream out;
    out << "selected mode: extract-openings\n";
    out << "source path: " << summary.source_path << "\n";
    out << "source size bytes: " << summary.source_size_bytes << "\n";
    out << "rating policy: " << summary.rating_policy << "\n";
    out << "rating bounds: [" << summary.min_rating << ", " << summary.max_rating << "]\n";
    out << "retained ply: " << summary.retained_ply << "\n";
    out << "total accepted games: " << summary.total_games_accepted_upstream << "\n";
    out << "replay attempts: " << summary.total_replay_attempts << "\n";
    out << "replay successes: " << summary.total_replay_successes << "\n";
    out << "replay failures: " << summary.total_replay_failures << "\n";
    out << "preview rows emitted: " << summary.preview_row_count_emitted << "\n";
    out << "include FEN snapshots: " << (config.include_fen_snapshots ? "yes" : "no") << "\n";
    out << "include UCI moves: " << (config.include_uci_moves ? "yes" : "no") << "\n";
    out << "replay failure counts by reason:\n";
    if (summary.replay_failure_counts.empty()) {
        out << "- none\n";
    } else {
        for (const auto& [reason, count] : summary.replay_failure_counts) {
            out << "- " << reason << ": " << count << "\n";
        }
    }
    out << "note: cross-game aggregation into final position-to-move counts remains deferred.\n";
    return out.str();
}

}  // namespace otcb
