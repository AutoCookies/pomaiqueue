#pragma once

#include <filesystem>
#include <functional>
#include <fstream>

#include "src/core/util/fsync.h"
#include "src/core/util/status.h"

namespace pomai::queue::core {

using TmpFileWriter = std::function<util::Status(std::ofstream& out)>;

util::Status DurableWriteFile(const std::filesystem::path& final_path,
                              TmpFileWriter writer,
                              util::FsyncPolicy fsync_policy);

util::Status DurableRename(const std::filesystem::path& tmp_path,
                           const std::filesystem::path& final_path,
                           util::FsyncPolicy fsync_policy);

}  // namespace pomai::queue::core
