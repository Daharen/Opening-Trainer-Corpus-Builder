#include "otcb/build_plan.hpp"

namespace otcb {

BuildPlan make_dry_run_build_plan() {
    return BuildPlan{
        .mode = "dry_run_scaffold",
        .stages = {
            {"validate_configuration", "completed", "Validated required CLI parameters and scaffold constraints."},
            {"prepare_output_layout", "completed", "Create output, bundle, and data directories for scaffold artifacts."},
            {"scan_pgn_headers", "not_implemented", "Real PGN scanning/parsing is intentionally deferred in this baseline lane."},
            {"aggregate_positions", "not_implemented", "Position-keyed move-frequency aggregation is not implemented yet."},
            {"serialize_payload", "scaffold_placeholder", "Write a clearly non-final placeholder payload file."},
        },
        .not_yet_implemented = {
            "Full PGN parsing",
            "SAN move replay",
            "Worker-local aggregation",
            "Real position keys",
            "Final payload encoding",
        },
    };
}

}  // namespace otcb
