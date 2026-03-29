#pragma once

#include <cstdint>
#include <filesystem>
#include <vector>

#include "otcb/aggregation.hpp"
#include "otcb/config.hpp"

namespace otcb {

struct SqliteWriteStats {
    int positions_rows = 0;
    int moves_rows = 0;
    int position_moves_rows = 0;
    std::int64_t total_raw_observations = 0;
};

struct CanonicalPredecessorSqliteWriteStats {
    int positions_rows = 0;
    int canonical_predecessor_rows = 0;
};

SqliteWriteStats write_aggregate_payload_sqlite(
    const std::filesystem::path& sqlite_path,
    const BuildConfig& config,
    const std::string& artifact_id,
    const AggregationSummary& summary,
    const std::vector<AggregatedPositionRecord>& positions);

SqliteWriteStats write_aggregate_payload_sqlite_compact_v2(
    const std::filesystem::path& sqlite_path,
    const BuildConfig& config,
    const std::string& artifact_id,
    const AggregationSummary& summary,
    const std::vector<AggregatedPositionRecord>& positions);

CanonicalPredecessorSqliteWriteStats write_canonical_predecessor_payload_sqlite(
    const std::filesystem::path& sqlite_path,
    const BuildConfig& config,
    const std::string& artifact_id,
    const AggregationSummary& summary,
    const std::vector<CanonicalPredecessorRecord>& predecessors);

}  // namespace otcb
