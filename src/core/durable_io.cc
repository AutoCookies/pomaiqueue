#include "src/core/durable_io.h"

#include <fcntl.h>
#include <unistd.h>

#include <system_error>

namespace pomai::queue::core {

util::Status DurableRename(const std::filesystem::path& tmp_path,
                           const std::filesystem::path& final_path,
                           util::FsyncPolicy fsync_policy) {
  if (fsync_policy != util::FsyncPolicy::kNever) {
    int tmp_fd = ::open(tmp_path.c_str(), O_RDONLY);
    if (tmp_fd < 0) {
      return util::Status(util::StatusCode::kIOError, "open tmp for fsync failed");
    }
    util::Status s = util::FsyncFile(tmp_fd);
    ::close(tmp_fd);
    if (!s.ok()) {
      return s;
    }
  }

  std::error_code ec;
  std::filesystem::rename(tmp_path, final_path, ec);
  if (ec) {
    return util::Status(util::StatusCode::kIOError, "rename failed");
  }

  if (fsync_policy != util::FsyncPolicy::kNever) {
    util::Status s = util::FsyncDir(final_path.parent_path().string());
    if (!s.ok()) {
      return s;
    }
  }

  return util::Status::Ok();
}

}  // namespace pomai::queue::core
