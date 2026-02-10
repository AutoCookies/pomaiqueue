#pragma once

#include <filesystem>

#include "src/core/util/fsync.h"
#include "src/core/util/status.h"

namespace pomai::queue::core {

util::Status DurableRename(const std::filesystem::path& tmp_path,
                           const std::filesystem::path& final_path,
                           util::FsyncPolicy fsync_policy);

}  // namespace pomai::queue::core
