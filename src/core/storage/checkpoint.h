#pragma once

#include <cstdint>
#include <filesystem>
#include <string>

#include "src/core/util/fsync.h"
#include "src/core/util/status.h"
#include "src/core/util/status_or.h"

namespace pomai::queue::storage {

class CheckpointStore {
 public:
  CheckpointStore(std::filesystem::path path, util::FsyncPolicy fsync_policy);

  util::StatusOr<uint64_t> Load();
  util::Status Save(uint64_t offset);

 private:
  std::filesystem::path path_;
  util::FsyncPolicy fsync_policy_;
};

}  // namespace pomai::queue::storage
