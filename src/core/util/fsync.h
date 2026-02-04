#pragma once

#include <string>

#include "src/core/util/status.h"

namespace pomai::queue::util {

enum class FsyncPolicy {
  kAlways,
  kInterval,
  kNever
};

Status FsyncFile(int fd);
Status FsyncDir(const std::string& path);

}  // namespace pomai::queue::util
