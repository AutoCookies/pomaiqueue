#include "src/core/storage/checkpoint.h"

#include <fcntl.h>
#include <unistd.h>

#include <fstream>

namespace pomai::queue::storage {

CheckpointStore::CheckpointStore(std::filesystem::path path, util::FsyncPolicy fsync_policy)
    : path_(std::move(path)), fsync_policy_(fsync_policy) {}

util::StatusOr<uint64_t> CheckpointStore::Load() {
  if (!std::filesystem::exists(path_)) {
    return uint64_t{0};
  }
  std::ifstream in(path_, std::ios::binary);
  if (!in.is_open()) {
    return util::Status(util::StatusCode::kIOError, "open checkpoint failed");
  }
  uint64_t offset = 0;
  in.read(reinterpret_cast<char*>(&offset), sizeof(offset));
  if (!in) {
    return util::Status(util::StatusCode::kCorruption, "checkpoint truncated");
  }
  return offset;
}

util::Status CheckpointStore::Save(uint64_t offset) {
  std::filesystem::create_directories(path_.parent_path());
  auto tmp_path = path_;
  tmp_path += ".tmp";

  {
    std::ofstream out(tmp_path, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
      return util::Status(util::StatusCode::kIOError, "open checkpoint tmp failed");
    }
    out.write(reinterpret_cast<const char*>(&offset), sizeof(offset));
    if (!out) {
      return util::Status(util::StatusCode::kIOError, "write checkpoint failed");
    }
    out.flush();
  }

  if (fsync_policy_ != util::FsyncPolicy::kNever) {
    int fd = ::open(tmp_path.c_str(), O_RDONLY);
    if (fd >= 0) {
      util::Status status = util::FsyncFile(fd);
      ::close(fd);
      if (!status.ok()) {
        return status;
      }
    }
  }

  std::filesystem::rename(tmp_path, path_);
  if (fsync_policy_ != util::FsyncPolicy::kNever) {
    util::Status status = util::FsyncDir(path_.parent_path().string());
    if (!status.ok()) {
      return status;
    }
  }

  return util::Status::Ok();
}

}  // namespace pomai::queue::storage
