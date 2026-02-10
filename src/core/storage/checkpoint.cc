#include "src/core/storage/checkpoint.h"

#include <fcntl.h>
#include <unistd.h>

#include <fstream>

#include "src/core/durable_io.h"
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
  return core::DurableWriteFile(
      path_,
      [&](std::ofstream& out) {
        CheckpointBlob blob;
        blob.offset = offset;
        blob.crc64 = ComputeCrc(blob);
        out.write(reinterpret_cast<const char*>(&blob), sizeof(blob));
        if (!out) {
          return util::Status(util::StatusCode::kIOError, "write checkpoint failed");
        }
        return util::Status::Ok();
      },
      fsync_policy_);
}

}  // namespace pomai::queue::storage
