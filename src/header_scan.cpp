#include "otcb/header_scan.hpp"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace otcb {
namespace {

struct ParsedTagLine {
    std::string name;
    std::string value;
};

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

std::string normalize_line(std::string line) {
    if (!line.empty() && line.back() == '\r') {
        line.pop_back();
    }
    return line;
}

bool starts_with(const std::string& text, const std::string& prefix) {
    return text.rfind(prefix, 0) == 0;
}

bool parse_tag_line(const std::string& line, ParsedTagLine& parsed) {
    if (line.size() < 5 || line.front() != '[' || line.back() != ']') {
        return false;
    }
    const std::size_t space_pos = line.find(' ');
    if (space_pos == std::string::npos || space_pos <= 1 || space_pos + 3 > line.size()) {
        return false;
    }
    if (line[space_pos + 1] != '"' || line[line.size() - 2] != '"') {
        return false;
    }
    parsed.name = line.substr(1, space_pos - 1);
    parsed.value = line.substr(space_pos + 2, line.size() - space_pos - 4);
    return true;
}

std::optional<int> parse_rating_value(const std::optional<std::string>& raw) {
    if (!raw.has_value()) {
        return std::nullopt;
    }
    const std::string& value = *raw;
    if (value.empty()) {
        return std::nullopt;
    }
    for (const char ch : value) {
        if (!std::isdigit(static_cast<unsigned char>(ch))) {
            return std::nullopt;
        }
    }
    try {
        return std::stoi(value);
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

void apply_tag(ParsedGameHeaders& headers, const ParsedTagLine& tag) {
    if (tag.name == "Event") {
        headers.event = tag.value;
    } else if (tag.name == "Site") {
        headers.site = tag.value;
    } else if (tag.name == "Date") {
        headers.date = tag.value;
    } else if (tag.name == "White") {
        headers.white = tag.value;
    } else if (tag.name == "Black") {
        headers.black = tag.value;
    } else if (tag.name == "WhiteElo") {
        headers.white_elo_raw = tag.value;
        headers.white_elo = parse_rating_value(headers.white_elo_raw);
    } else if (tag.name == "BlackElo") {
        headers.black_elo_raw = tag.value;
        headers.black_elo = parse_rating_value(headers.black_elo_raw);
    } else if (tag.name == "Result") {
        headers.result = tag.value;
    } else if (tag.name == "Termination") {
        headers.termination = tag.value;
    } else if (tag.name == "TimeControl") {
        headers.time_control = tag.value;
    } else if (tag.name == "Variant") {
        headers.variant = tag.value;
    } else if (tag.name == "ECO") {
        headers.eco = tag.value;
    } else {
        headers.extra_tags.emplace(tag.name, tag.value);
    }
}

bool in_band(const int value, const int min_rating, const int max_rating) {
    return value >= min_rating && value <= max_rating;
}

HeaderScanClassification classify_headers(const BuildConfig& config, const ParsedGameHeaders& headers) {
    if (!headers.white_elo_raw.has_value()) {
        return HeaderScanClassification::RejectedMissingWhiteElo;
    }
    if (!headers.black_elo_raw.has_value()) {
        return HeaderScanClassification::RejectedMissingBlackElo;
    }
    if (!headers.white_elo.has_value()) {
        return HeaderScanClassification::RejectedInvalidWhiteElo;
    }
    if (!headers.black_elo.has_value()) {
        return HeaderScanClassification::RejectedInvalidBlackElo;
    }

    const int white = *headers.white_elo;
    const int black = *headers.black_elo;
    bool accepted = false;
    switch (*config.rating_policy) {
        case RatingPolicy::BothInBand:
            accepted = in_band(white, config.min_rating, config.max_rating) && in_band(black, config.min_rating, config.max_rating);
            break;
        case RatingPolicy::AverageInBand:
            accepted = in_band((white + black) / 2, config.min_rating, config.max_rating);
            break;
        case RatingPolicy::WhiteInBand:
            accepted = in_band(white, config.min_rating, config.max_rating);
            break;
        case RatingPolicy::BlackInBand:
            accepted = in_band(black, config.min_rating, config.max_rating);
            break;
    }
    return accepted ? HeaderScanClassification::Accepted : HeaderScanClassification::RejectedPolicyMismatch;
}

void increment_reason(std::map<std::string, int>& counts, const HeaderScanClassification classification) {
    const std::string key = to_string(classification);
    if (key != "accepted") {
        ++counts[key];
    }
}

std::uint64_t current_offset(std::ifstream& input) {
    const std::streampos pos = input.tellg();
    return pos == std::streampos(-1) ? 0 : static_cast<std::uint64_t>(pos);
}

}  // namespace

HeaderScanResult scan_headers(const BuildConfig& config, const SourcePreflightInfo& preflight_info, const RangePlan& range_plan) {
    std::ifstream input(preflight_info.canonical_input_path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("Failed to open PGN for header scanning: " + preflight_info.canonical_input_path.string());
    }

    HeaderScanResult result;
    result.summary.source_path = preflight_info.canonical_input_path.generic_string();
    result.summary.source_size_bytes = preflight_info.file_size_bytes;
    result.summary.planner_algorithm = range_plan.planner_algorithm;
    result.summary.target_range_bytes = range_plan.target_range_bytes;
    result.summary.boundary_scan_bytes = range_plan.boundary_scan_bytes;
    result.summary.retained_ply = config.retained_ply;
    result.summary.rating_policy = to_string(*config.rating_policy);
    result.summary.min_rating = config.min_rating;
    result.summary.max_rating = config.max_rating;
    result.summary.scan_algorithm = "streaming_line_aligned_event_header_envelope_scan_v1";
    result.summary.notes.push_back("SAN replay, board reconstruction, and final corpus aggregation remain deferred.");
    if (config.strict_header_scan) {
        result.summary.notes.push_back("Strict header scanning enabled: non-tag content inside header blocks is rejected explicitly.");
    }

    for (std::size_t range_index = 0; range_index < range_plan.ranges.size(); ++range_index) {
        const PlannedRange& planned = range_plan.ranges[range_index];
        const std::uint64_t range_start = planned.adjusted_start_byte;
        const std::uint64_t range_end_exclusive = (range_index + 1 < range_plan.ranges.size())
            ? range_plan.ranges[range_index + 1].adjusted_start_byte
            : preflight_info.file_size_bytes;

        RangeScanSummary range_summary;
        range_summary.range_index = planned.range_index;
        range_summary.range_start_byte = range_start;
        range_summary.range_end_byte = range_end_exclusive == 0 ? 0 : range_end_exclusive - 1;
        range_summary.bytes_scanned = range_end_exclusive > range_start ? (range_end_exclusive - range_start) : 0;

        if (range_start >= preflight_info.file_size_bytes || range_start >= range_end_exclusive) {
            range_summary.notes.push_back("Range start resolved to end-of-file; nothing scanned.");
            result.summary.range_summaries.push_back(range_summary);
            continue;
        }

        input.clear();
        input.seekg(static_cast<std::streamoff>(range_start), std::ios::beg);
        if (!input) {
            throw std::runtime_error("Failed to seek PGN source to range start.");
        }

        while (input) {
            const std::uint64_t candidate_start = current_offset(input);
            if (candidate_start >= range_end_exclusive) {
                break;
            }

            std::string first_line;
            if (!std::getline(input, first_line)) {
                break;
            }
            first_line = normalize_line(first_line);
            if (!starts_with(first_line, "[Event ")) {
                continue;
            }

            GameEnvelope envelope;
            envelope.range_index = planned.range_index;
            envelope.header_start_byte = candidate_start;

            ParsedTagLine parsed_tag;
            if (!parse_tag_line(first_line, parsed_tag)) {
                envelope.classification = HeaderScanClassification::RejectedNonstandardOrUnsupportedHeaderShape;
                envelope.rejection_reason = to_string(envelope.classification);
            } else {
                apply_tag(envelope.headers, parsed_tag);
                bool header_complete = false;
                bool malformed_header = false;

                while (input) {
                    const std::uint64_t line_start = current_offset(input);
                    if (line_start > range_end_exclusive) {
                        envelope.classification = HeaderScanClassification::RejectedIncompleteHeaderBlock;
                        envelope.rejection_reason = to_string(envelope.classification);
                        break;
                    }

                    std::string line;
                    if (!std::getline(input, line)) {
                        envelope.classification = HeaderScanClassification::RejectedIncompleteHeaderBlock;
                        envelope.rejection_reason = to_string(envelope.classification);
                        break;
                    }
                    line = normalize_line(line);

                    if (line.empty()) {
                        envelope.header_end_byte = current_offset(input) == 0 ? line_start : current_offset(input) - 1;
                        envelope.movetext_start_byte = current_offset(input);
                        header_complete = true;
                        break;
                    }

                    if (config.strict_header_scan && !starts_with(line, "[")) {
                        malformed_header = true;
                    }

                    ParsedTagLine tag_line;
                    if (!parse_tag_line(line, tag_line)) {
                        malformed_header = true;
                        if (config.strict_header_scan) {
                            envelope.classification = HeaderScanClassification::RejectedNonstandardOrUnsupportedHeaderShape;
                            envelope.rejection_reason = to_string(envelope.classification);
                            break;
                        }
                        continue;
                    }
                    apply_tag(envelope.headers, tag_line);
                }

                if (!header_complete && !envelope.rejection_reason.has_value()) {
                    envelope.classification = malformed_header
                        ? HeaderScanClassification::RejectedNonstandardOrUnsupportedHeaderShape
                        : HeaderScanClassification::RejectedIncompleteHeaderBlock;
                    envelope.rejection_reason = to_string(envelope.classification);
                }

                if (header_complete && !envelope.rejection_reason.has_value()) {
                    bool envelope_complete = false;
                    while (input) {
                        const std::uint64_t line_start = current_offset(input);
                        if (line_start == range_end_exclusive) {
                            envelope.movetext_end_byte = line_start == 0 ? 0 : line_start - 1;
                            envelope.game_end_byte = envelope.movetext_end_byte;
                            envelope_complete = true;
                            break;
                        }
                        if (line_start > range_end_exclusive) {
                            envelope.classification = HeaderScanClassification::RejectedIncompleteGameEnvelope;
                            envelope.rejection_reason = to_string(envelope.classification);
                            envelope.movetext_end_byte = range_end_exclusive == 0 ? 0 : range_end_exclusive - 1;
                            envelope.game_end_byte = envelope.movetext_end_byte;
                            range_summary.notes.push_back("Movetext scan crossed the owned range boundary before reaching the next aligned game start.");
                            break;
                        }

                        const std::uint64_t before_read = line_start;
                        std::string line;
                        if (!std::getline(input, line)) {
                            if (input.eof()) {
                                envelope.movetext_end_byte = preflight_info.file_size_bytes == 0 ? 0 : preflight_info.file_size_bytes - 1;
                                envelope.game_end_byte = envelope.movetext_end_byte;
                                envelope_complete = true;
                            } else {
                                envelope.classification = HeaderScanClassification::RejectedIncompleteGameEnvelope;
                                envelope.rejection_reason = to_string(envelope.classification);
                                envelope.movetext_end_byte = preflight_info.file_size_bytes == 0 ? 0 : preflight_info.file_size_bytes - 1;
                                envelope.game_end_byte = envelope.movetext_end_byte;
                            }
                            break;
                        }
                        line = normalize_line(line);

                        if (starts_with(line, "[Event ")) {
                            envelope.movetext_end_byte = before_read == 0 ? 0 : before_read - 1;
                            envelope.game_end_byte = envelope.movetext_end_byte;
                            input.clear();
                            input.seekg(static_cast<std::streamoff>(before_read), std::ios::beg);
                            envelope_complete = true;
                            break;
                        }
                    }

                    if (envelope_complete && !envelope.rejection_reason.has_value()) {
                        envelope.classification = classify_headers(config, envelope.headers);
                        if (envelope.classification != HeaderScanClassification::Accepted) {
                            envelope.rejection_reason = to_string(envelope.classification);
                        }
                    }
                }
            }

            ++range_summary.games_scanned;
            ++result.summary.total_games_scanned;
            if (envelope.classification == HeaderScanClassification::Accepted) {
                ++range_summary.games_accepted;
                ++result.summary.total_games_accepted;
            } else {
                ++range_summary.games_rejected;
                ++result.summary.total_games_rejected;
                increment_reason(range_summary.rejection_counts, envelope.classification);
                increment_reason(result.summary.global_rejection_counts, envelope.classification);
            }

            const bool emit_preview = config.emit_header_preview &&
                (config.header_preview_limit < 0 || static_cast<int>(result.preview_rows.size()) < config.header_preview_limit);
            if (emit_preview) {
                result.preview_rows.push_back(envelope);
            }
        }

        result.summary.range_summaries.push_back(range_summary);
    }

    result.summary.total_ranges_executed = static_cast<int>(result.summary.range_summaries.size());
    result.summary.preview_row_count_emitted = static_cast<int>(result.preview_rows.size());
    return result;
}

std::string render_range_execution_summary_json(const HeaderScanSummary& summary) {
    std::ostringstream output;
    output << "{\n";
    output << "  \"source_path\": \"" << json_escape(summary.source_path) << "\",\n";
    output << "  \"source_size_bytes\": " << summary.source_size_bytes << ",\n";
    output << "  \"rating_policy\": \"" << json_escape(summary.rating_policy) << "\",\n";
    output << "  \"rating_lower_bound\": " << summary.min_rating << ",\n";
    output << "  \"rating_upper_bound\": " << summary.max_rating << ",\n";
    output << "  \"retained_ply\": " << summary.retained_ply << ",\n";
    output << "  \"target_range_bytes\": " << summary.target_range_bytes << ",\n";
    output << "  \"boundary_scan_bytes\": " << summary.boundary_scan_bytes << ",\n";
    output << "  \"total_ranges_executed\": " << summary.total_ranges_executed << ",\n";
    output << "  \"total_games_scanned\": " << summary.total_games_scanned << ",\n";
    output << "  \"total_games_accepted\": " << summary.total_games_accepted << ",\n";
    output << "  \"total_games_rejected\": " << summary.total_games_rejected << ",\n";
    output << "  \"rejection_counts\": {\n";
    for (auto it = summary.global_rejection_counts.begin(); it != summary.global_rejection_counts.end(); ++it) {
        output << "    \"" << json_escape(it->first) << "\": " << it->second;
        output << (std::next(it) != summary.global_rejection_counts.end() ? "," : "") << "\n";
    }
    output << "  },\n";
    output << "  \"ordered_range_summaries\": [\n";
    for (std::size_t index = 0; index < summary.range_summaries.size(); ++index) {
        const auto& range = summary.range_summaries[index];
        output << "    {\n";
        output << "      \"range_index\": " << range.range_index << ",\n";
        output << "      \"range_start_byte\": " << range.range_start_byte << ",\n";
        output << "      \"range_end_byte\": " << range.range_end_byte << ",\n";
        output << "      \"games_scanned\": " << range.games_scanned << ",\n";
        output << "      \"games_accepted\": " << range.games_accepted << ",\n";
        output << "      \"games_rejected\": " << range.games_rejected << ",\n";
        output << "      \"bytes_scanned\": " << range.bytes_scanned << ",\n";
        output << "      \"rejection_counts\": {\n";
        for (auto it = range.rejection_counts.begin(); it != range.rejection_counts.end(); ++it) {
            output << "        \"" << json_escape(it->first) << "\": " << it->second;
            output << (std::next(it) != range.rejection_counts.end() ? "," : "") << "\n";
        }
        output << "      },\n";
        output << "      \"notes\": [\n";
        for (std::size_t note_index = 0; note_index < range.notes.size(); ++note_index) {
            output << "        \"" << json_escape(range.notes[note_index]) << "\"";
            output << (note_index + 1 < range.notes.size() ? "," : "") << "\n";
        }
        output << "      ]\n";
        output << "    }" << (index + 1 < summary.range_summaries.size() ? "," : "") << "\n";
    }
    output << "  ],\n";
    output << "  \"notes\": [\n";
    for (std::size_t index = 0; index < summary.notes.size(); ++index) {
        output << "    \"" << json_escape(summary.notes[index]) << "\"";
        output << (index + 1 < summary.notes.size() ? "," : "") << "\n";
    }
    output << "  ]\n";
    output << "}\n";
    return output.str();
}

std::string render_range_execution_summary_text(const BuildConfig& config, const HeaderScanSummary& summary) {
    std::ostringstream output;
    output << "Range execution summary\n";
    output << "selected mode: " << to_string(config.mode) << "\n";
    output << "source path: " << summary.source_path << "\n";
    output << "source size bytes: " << summary.source_size_bytes << "\n";
    output << "rating policy: " << summary.rating_policy << "\n";
    output << "rating bounds: [" << summary.min_rating << ", " << summary.max_rating << "]\n";
    output << "retained ply: " << summary.retained_ply << "\n";
    output << "planned ranges: " << summary.total_ranges_executed << "\n";
    output << "executed ranges: " << summary.total_ranges_executed << "\n";
    output << "total games scanned: " << summary.total_games_scanned << "\n";
    output << "total games accepted: " << summary.total_games_accepted << "\n";
    output << "total games rejected: " << summary.total_games_rejected << "\n";
    output << "rejection counts by reason:\n";
    if (summary.global_rejection_counts.empty()) {
        output << "- none\n";
    } else {
        for (const auto& [reason, count] : summary.global_rejection_counts) {
            output << "- " << reason << ": " << count << "\n";
        }
    }
    output << "first range summaries:\n";
    const std::size_t preview_count = std::min<std::size_t>(5, summary.range_summaries.size());
    for (std::size_t index = 0; index < preview_count; ++index) {
        const auto& range = summary.range_summaries[index];
        output << "- range " << range.range_index
               << ": start=" << range.range_start_byte
               << ", end=" << range.range_end_byte
               << ", bytes=" << range.bytes_scanned
               << ", scanned=" << range.games_scanned
               << ", accepted=" << range.games_accepted
               << ", rejected=" << range.games_rejected << "\n";
    }
    output << "preview rows emitted: " << summary.preview_row_count_emitted << "\n";
    output << "movetext replay performed: no\n";
    output << "note: SAN replay and final payload construction remain deferred.\n";
    return output.str();
}

std::string render_header_scan_preview_jsonl(const std::vector<GameEnvelope>& preview_rows) {
    std::ostringstream output;
    for (const auto& row : preview_rows) {
        output << "{";
        output << "\"range_index\":" << row.range_index << ",";
        output << "\"header_start_byte\":" << row.header_start_byte << ",";
        output << "\"game_end_byte\":" << row.game_end_byte << ",";
        output << "\"white_elo\":" << (row.headers.white_elo ? std::to_string(*row.headers.white_elo) : "null") << ",";
        output << "\"black_elo\":" << (row.headers.black_elo ? std::to_string(*row.headers.black_elo) : "null") << ",";
        output << "\"result\":\"" << json_escape(row.headers.result.value_or("")) << "\",";
        output << "\"event\":\"" << json_escape(row.headers.event.value_or("")) << "\",";
        output << "\"site\":\"" << json_escape(row.headers.site.value_or("")) << "\",";
        output << "\"time_control\":\"" << json_escape(row.headers.time_control.value_or("")) << "\",";
        output << "\"classification\":\"" << json_escape(to_string(row.classification)) << "\",";
        output << "\"rejection_reason\":\"" << json_escape(row.rejection_reason.value_or("")) << "\",";
        output << "\"movetext_span_bytes\":" << (row.movetext_end_byte >= row.movetext_start_byte ? (row.movetext_end_byte - row.movetext_start_byte + 1) : 0);
        output << "}\n";
    }
    return output.str();
}

}  // namespace otcb
