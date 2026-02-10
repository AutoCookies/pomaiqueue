#include "src/core/storage/checkpoint.h"

#include <fcntl.h>
#include <unistd.h>

#include <fstream>

#include "src/core/util/crc64.h"

namespace pomai::queue::storage {

namespace {
constexpr uint32_t kCheckpointMagic = 0x504d434b;  // PMCK
constexpr uint16_t kCheckpointVersion = 1;

struct CheckpointBlob {
  uint32_t magic = kCheckpointMagic;
  uint16_t version = kCheckpointVersion;
  uint16_t reserved = 0;
  uint64_t offset = 0;
  uint64_t crc64 = 0;
};

uint64_t ComputeCrc(const CheckpointBlob& blob) {
  return util::Crc64::Compute(reinterpret_cast<const std::byte*>(&blob), sizeof(blob) - sizeof(blob.crc64));
}

}  // namespace

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
  CheckpointBlob blob;
  in.read(reinterpret_cast<char*>(&blob), sizeof(blob));
  if (!in) {
    return util::Status(util::StatusCode::kCorruption, "checkpoint truncated");
  }
  if (blob.magic != kCheckpointMagic || blob.version != kCheckpointVersion) {
    return util::Status(util::StatusCode::kCorruption, "checkpoint version mismatch");
  }
  if (blob.crc64 != ComputeCrc(blob)) {
    return util::Status(util::StatusCode::kCorruption, "checkpoint crc mismatch");
  }
  return blob.offset;
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
    CheckpointBlob blob;
    blob.offset = offset;
    blob.crc64 = ComputeCrc(blob);
    out.write(reinterpret_cast<const char*>(&blob), sizeof(blob));
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
