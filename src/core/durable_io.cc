#include "src/core/durable_io.h"

#include <fcntl.h>
#include <unistd.h>

#include <fstream>
#include <system_error>

namespace pomai::queue::core {

util::Status DurableWriteFile(const std::filesystem::path& final_path,
                              TmpFileWriter writer,
                              util::FsyncPolicy fsync_policy) {
  std::filesystem::create_directories(final_path.parent_path());
  auto tmp_path = final_path;
  tmp_path += ".tmp";

  {
    std::ofstream out(tmp_path, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
      return util::Status(util::StatusCode::kIOError, "open durable tmp failed");
    }
    util::Status write_status = writer(out);
    if (!write_status.ok()) {
      return write_status;
    }
    out.flush();
    if (!out) {
      return util::Status(util::StatusCode::kIOError, "flush durable tmp failed");
    }
  }

  return DurableRename(tmp_path, final_path, fsync_policy);
}

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
