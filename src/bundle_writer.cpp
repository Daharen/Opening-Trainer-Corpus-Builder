#include "otcb/bundle_writer.hpp"

#include <fstream>
#include <stdexcept>

#include "otcb/build_plan.hpp"
#include "otcb/manifest.hpp"

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

}  // namespace

BundleWriteResult write_dry_run_bundle(const BuildConfig& config) {
    const std::string artifact_id = config.artifact_id.value_or(derive_artifact_id(config));
    const std::filesystem::path bundle_root = config.output_dir / artifact_id;
    const std::filesystem::path data_dir = bundle_root / "data";

    std::filesystem::create_directories(data_dir);

    const BuildPlan plan = make_dry_run_build_plan();
    const ManifestData manifest = make_manifest_data(config, plan, artifact_id);

    write_text_file(bundle_root / "manifest.json", render_manifest_json(manifest));
    write_text_file(bundle_root / "build_summary.txt", render_build_summary(config, plan, artifact_id));
    write_text_file(
        data_dir / "positions_placeholder.jsonl",
        "{\"payload_status\":\"placeholder_non_final_payload\",\"record_type\":\"scaffold\",\"notes\":[\"No real position data has been generated yet.\"]}\n");

    return BundleWriteResult{.bundle_root = bundle_root, .artifact_id = artifact_id};
}

}  // namespace otcb
