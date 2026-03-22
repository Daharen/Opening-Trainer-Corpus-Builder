#pragma once

#include <filesystem>
#include <string>

#include "otcb/config.hpp"

namespace otcb {

struct BundleWriteResult {
    std::filesystem::path bundle_root;
    std::string artifact_id;
};

BundleWriteResult write_dry_run_bundle(const BuildConfig& config);

}  // namespace otcb
