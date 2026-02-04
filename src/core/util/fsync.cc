#include "src/core/util/fsync.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace pomai::queue::util {

Status FsyncFile(int fd) {
  if (fd < 0) {
    return Status(StatusCode::kInvalidArgument, "invalid fd");
  }
  if (::fsync(fd) != 0) {
    return Status(StatusCode::kIOError, "fsync failed");
  }
  return Status::Ok();
}

Status FsyncDir(const std::string& path) {
  int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY);
  if (fd < 0) {
    return Status(StatusCode::kIOError, "open dir failed");
  }
  int result = ::fsync(fd);
  ::close(fd);
  if (result != 0) {
    return Status(StatusCode::kIOError, "fsync dir failed");
  }
  return Status::Ok();
}

}  // namespace pomai::queue::util
