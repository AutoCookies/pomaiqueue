#pragma once

#include <cstdint>
#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "src/core/storage/record.h"
#include "src/core/util/fsync.h"
#include "src/core/util/status.h"
#include "src/core/util/status_or.h"

namespace pomai::queue::storage {

class SegmentLog {
 public:
  SegmentLog(std::filesystem::path path, util::FsyncPolicy fsync_policy);
  ~SegmentLog();
  SegmentLog(const SegmentLog&) = delete;
  SegmentLog& operator=(const SegmentLog&) = delete;
  SegmentLog(SegmentLog&& other) noexcept;
  SegmentLog& operator=(SegmentLog&& other) noexcept;

  util::Status Open();
  util::Status Recover();

  util::StatusOr<uint64_t> Append(const Record& record);
  util::StatusOr<Record> Read(uint64_t offset) const;
  std::vector<uint64_t> Offsets() const;
  uint64_t SizeBytes() const;

  uint64_t next_offset() const { return next_offset_; }
  const std::filesystem::path& path() const { return path_; }

 private:
  util::StatusOr<std::vector<std::byte>> ReadAt(uint64_t position, size_t size) const;
  util::Status WriteAll(const std::byte* data, size_t size);

  std::filesystem::path path_;
  util::FsyncPolicy fsync_policy_;
  int fd_ = -1;
  uint64_t next_offset_ = 0;
  std::map<uint64_t, uint64_t> offset_to_position_;
};

}  // namespace pomai::queue::storage
