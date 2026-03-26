#include "otcb/bundle_writer.hpp"

#include <fstream>
#include <stdexcept>

#include "otcb/aggregation.hpp"
#include "otcb/build_plan.hpp"
#include "otcb/header_scan.hpp"
#include "otcb/manifest.hpp"
#include "otcb/opening_extraction.hpp"
#include "otcb/progress.hpp"
#include "otcb/sqlite_writer.hpp"

namespace otcb {
namespace {

void write_text_file(const std::filesystem::path& path, const std::string& content) {
    std::ofstream output(path, std::ios::binary);
    if (!output) {
        throw std::runtime_error("Failed to open file for writing: " + path.string());
    }
    output << content;
    if (!output) {
        throw std::runtime_error("Failed to write file: " + path.string());
    }
}

void write_placeholder_payload(const std::filesystem::path& data_dir) {
    write_text_file(
        data_dir / "positions_placeholder.jsonl",
        "{\"payload_status\":\"placeholder_non_final_payload\",\"record_type\":\"scaffold\",\"notes\":[\"Raw aggregation is not present for this mode.\"]}\n");
}

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

void write_aggregate_jsonl_streaming(const std::filesystem::path& path, const std::vector<AggregatedPositionRecord>& positions, const BuildConfig& config) {
    std::ofstream output(path, std::ios::binary);
    if (!output) {
        throw std::runtime_error("Failed to open aggregate JSONL payload for writing: " + path.string());
    }
    for (const auto& record : positions) {
        output << '{'
               << "\"position_key\":\"" << json_escape(record.position_key) << "\","
               << "\"position_key_format\":\"" << json_escape(to_string(*config.position_key_format)) << "\","
               << "\"side_to_move\":\"" << json_escape(record.side_to_move) << "\","
               << "\"candidate_move_count\":" << record.candidate_move_count << ','
               << "\"total_observations\":" << record.total_observations << ','
               << "\"candidate_moves\":[";
        for (std::size_t i = 0; i < record.candidate_moves.size(); ++i) {
            const auto& move = record.candidate_moves[i];
            output << '{'
                   << "\"move_key\":\"" << json_escape(move.move_key) << "\","
                   << "\"move_key_format\":\"" << json_escape(to_string(*config.move_key_format)) << "\","
                   << "\"raw_count\":" << move.raw_count;
            if (!move.example_san.empty()) {
                output << ",\"example_san\":\"" << json_escape(move.example_san) << "\"";
            }
            output << '}';
            if (i + 1 < record.candidate_moves.size()) {
                output << ',';
            }
        }
        output << "]}\n";
    }
    output.flush();
    if (!output.good()) {
        throw std::runtime_error("Failed while writing aggregate JSONL payload: " + path.string());
    }
    if (!std::filesystem::exists(path)) {
        throw std::runtime_error("Aggregate JSONL payload missing after write: " + path.string());
    }
    if (std::filesystem::file_size(path) == 0) {
        throw std::runtime_error("Aggregate JSONL payload is empty after write: " + path.string());
    }
}

BundleWriteResult write_bundle(
    const BuildConfig& config,
    const BuildPlan& plan,
    const std::string& artifact_id,
    const bool emit_range_plan_files,
    ProgressReporter* progress,
    const HeaderScanSummary* scan_summary = nullptr,
    const std::vector<GameEnvelope>* preview_rows = nullptr,
    const ExtractionSummary* extraction_summary = nullptr,
    const std::vector<ExtractedOpeningSequence>* sequences = nullptr,
    const std::vector<ExtractedOpeningSequence>* extraction_preview_rows = nullptr,
    const AggregationSummary* aggregation_summary = nullptr,
    const std::vector<AggregatedPositionRecord>* aggregate_positions = nullptr,
    const std::vector<AggregatedPositionRecord>* aggregate_preview_rows = nullptr) {
    const std::filesystem::path bundle_root = config.output_dir / artifact_id;
    if (progress) {
        progress->stage_started(ProgressStage::WriteArtifacts, "writing artifact bundle files");
    }
    const std::filesystem::path data_dir = bundle_root / "data";
    const std::filesystem::path plans_dir = bundle_root / "plans";

    std::filesystem::create_directories(data_dir);
    if (emit_range_plan_files || scan_summary != nullptr || extraction_summary != nullptr || aggregation_summary != nullptr) {
        std::filesystem::create_directories(plans_dir);
    }

    AggregationSummary enriched_aggregation_summary;
    const AggregationSummary* manifest_aggregation_summary = aggregation_summary;
    if (aggregation_summary != nullptr) {
        enriched_aggregation_summary = *aggregation_summary;
    }

    if (aggregate_positions != nullptr) {
        if (config.payload_format == PayloadFormat::Sqlite) {
            const auto sqlite_stats = write_aggregate_payload_sqlite(data_dir / "corpus.sqlite", config, artifact_id, *aggregation_summary, *aggregate_positions);
            enriched_aggregation_summary.sqlite_positions_rows = sqlite_stats.positions_rows;
            enriched_aggregation_summary.sqlite_moves_rows = sqlite_stats.moves_rows;
            enriched_aggregation_summary.sqlite_position_moves_rows = sqlite_stats.position_moves_rows;
            manifest_aggregation_summary = &enriched_aggregation_summary;
        } else if (config.payload_format == PayloadFormat::ExactSqliteV2Compact) {
            const auto compact_stats = write_aggregate_payload_sqlite_compact_v2(data_dir / "corpus_compact.sqlite", config, artifact_id, *aggregation_summary, *aggregate_positions);
            enriched_aggregation_summary.sqlite_positions_rows = compact_stats.positions_rows;
            enriched_aggregation_summary.sqlite_moves_rows = compact_stats.moves_rows;
            enriched_aggregation_summary.sqlite_position_moves_rows = compact_stats.position_moves_rows;
            enriched_aggregation_summary.canonical_payload_file = "data/corpus_compact.sqlite";
            if (std::filesystem::exists(data_dir / "corpus_compact.sqlite")) {
                enriched_aggregation_summary.canonical_payload_size_bytes = std::filesystem::file_size(data_dir / "corpus_compact.sqlite");
            }
            if (config.emit_legacy_sqlite_mirror) {
                BuildConfig legacy_config = config;
                legacy_config.payload_format = PayloadFormat::Sqlite;
                const auto legacy_stats = write_aggregate_payload_sqlite(data_dir / "corpus.sqlite", legacy_config, artifact_id, *aggregation_summary, *aggregate_positions);
                enriched_aggregation_summary.compatibility_payload_file = "data/corpus.sqlite";
                enriched_aggregation_summary.compatibility_mirror_emitted = true;
                if (std::filesystem::exists(data_dir / "corpus.sqlite")) {
                    enriched_aggregation_summary.compatibility_payload_size_bytes = std::filesystem::file_size(data_dir / "corpus.sqlite");
                }
                (void)legacy_stats;
            }
            manifest_aggregation_summary = &enriched_aggregation_summary;
        } else {
            write_aggregate_jsonl_streaming(data_dir / "aggregated_position_move_counts.jsonl", *aggregate_positions, config);
        }
    }

    const ManifestData manifest = make_manifest_data(config, plan, artifact_id, scan_summary, extraction_summary, manifest_aggregation_summary);
    write_text_file(bundle_root / "manifest.json", render_manifest_json(manifest));
    write_text_file(bundle_root / "build_summary.txt", render_build_summary(config, plan, artifact_id, scan_summary, extraction_summary, manifest_aggregation_summary));
    write_placeholder_payload(data_dir);

    if (emit_range_plan_files && plan.range_plan) {
        write_text_file(plans_dir / "range_plan.json", render_range_plan_json(*plan.range_plan));
        write_text_file(plans_dir / "range_plan.txt", render_range_plan_text(*plan.range_plan));
    }
    if (scan_summary != nullptr) {
        write_text_file(plans_dir / "range_execution_summary.json", render_range_execution_summary_json(*scan_summary));
        write_text_file(plans_dir / "range_execution_summary.txt", render_range_execution_summary_text(config, *scan_summary));
    }
    if (preview_rows != nullptr && config.emit_header_preview) {
        write_text_file(data_dir / "header_scan_preview.jsonl", render_header_scan_preview_jsonl(*preview_rows));
    }
    if (extraction_summary != nullptr) {
        write_text_file(plans_dir / "extraction_summary.json", render_extraction_summary_json(*extraction_summary));
        write_text_file(plans_dir / "extraction_summary.txt", render_extraction_summary_text(config, *extraction_summary));
    }
    if (sequences != nullptr) {
        write_text_file(data_dir / "extracted_opening_sequences.jsonl", render_extracted_opening_sequences_jsonl(*sequences, config.include_fen_snapshots, config.include_uci_moves));
    }
    if (extraction_preview_rows != nullptr && config.emit_extraction_preview) {
        write_text_file(data_dir / "extraction_preview.jsonl", render_extraction_preview_jsonl(*extraction_preview_rows));
    }
    if (manifest_aggregation_summary != nullptr) {
        write_text_file(plans_dir / "aggregation_summary.json", render_aggregation_summary_json(*manifest_aggregation_summary));
        write_text_file(plans_dir / "aggregation_summary.txt", render_aggregation_summary_text(config, *manifest_aggregation_summary));
    }
    if (aggregate_preview_rows != nullptr && config.emit_aggregate_preview) {
        write_text_file(data_dir / "aggregate_preview.jsonl", render_aggregate_preview_jsonl(*aggregate_preview_rows, config));
    }

    if (progress) {
        progress->stage_completed("artifact bundle files written");
    }
    return BundleWriteResult{.bundle_root = bundle_root, .artifact_id = artifact_id};
}

}  // namespace

BundleWriteResult write_dry_run_bundle(const BuildConfig& config, ProgressReporter* progress) {
    const std::string artifact_id = config.artifact_id.value_or(derive_artifact_id(config));
    const BuildPlan plan = make_dry_run_build_plan();
    return write_bundle(config, plan, artifact_id, false, progress);
}

BundleWriteResult write_preflight_bundle(const BuildConfig& config, const SourcePreflightInfo& preflight_info, const RangePlan* range_plan, ProgressReporter* progress) {
    const std::string artifact_id = config.artifact_id.value_or(derive_artifact_id(config));
    const BuildPlan plan = make_preflight_build_plan(preflight_info, range_plan != nullptr, range_plan);
    return write_bundle(config, plan, artifact_id, range_plan != nullptr, progress);
}

BundleWriteResult write_plan_ranges_bundle(const BuildConfig& config, const SourcePreflightInfo& preflight_info, const RangePlan& range_plan, ProgressReporter* progress) {
    const std::string artifact_id = config.artifact_id.value_or(derive_artifact_id(config));
    const BuildPlan plan = make_plan_ranges_build_plan(preflight_info, range_plan);
    return write_bundle(config, plan, artifact_id, true, progress);
}

BundleWriteResult write_scan_headers_bundle(const BuildConfig& config, const SourcePreflightInfo& preflight_info, const RangePlan& range_plan, const HeaderScanResult& scan_result, ProgressReporter* progress) {
    const std::string artifact_id = config.artifact_id.value_or(derive_artifact_id(config));
    const BuildPlan plan = make_scan_headers_build_plan(preflight_info, range_plan);
    return write_bundle(config, plan, artifact_id, true, progress, &scan_result.summary, &scan_result.preview_rows);
}

BundleWriteResult write_extract_openings_bundle(const BuildConfig& config, const SourcePreflightInfo& preflight_info, const RangePlan& range_plan, const ExtractionResult& extraction_result, ProgressReporter* progress) {
    const std::string artifact_id = config.artifact_id.value_or(derive_artifact_id(config));
    const BuildPlan plan = make_extract_openings_build_plan(preflight_info, range_plan);
    return write_bundle(config, plan, artifact_id, true, progress, &extraction_result.scan_result.summary, &extraction_result.scan_result.preview_rows, &extraction_result.summary, &extraction_result.sequences, &extraction_result.preview_rows);
}

BundleWriteResult write_aggregate_counts_bundle(const BuildConfig& config, const SourcePreflightInfo& preflight_info, const RangePlan& range_plan, const AggregationResult& aggregation_result, ProgressReporter* progress) {
    const std::string artifact_id = config.artifact_id.value_or(derive_artifact_id(config));
    const BuildPlan plan = make_aggregate_counts_build_plan(preflight_info, range_plan);
    return write_bundle(config, plan, artifact_id, true, progress, &aggregation_result.extraction_result.scan_result.summary, &aggregation_result.extraction_result.scan_result.preview_rows, &aggregation_result.extraction_result.summary, &aggregation_result.extraction_result.sequences, &aggregation_result.extraction_result.preview_rows, &aggregation_result.summary, &aggregation_result.positions, &aggregation_result.preview_rows);
}

}  // namespace otcb
