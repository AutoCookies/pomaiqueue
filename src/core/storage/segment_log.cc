#include "src/core/storage/segment_log.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstring>
#include <vector>

#include "src/core/util/crc64.h"

namespace pomai::queue::storage {

SegmentLog::SegmentLog(std::filesystem::path path, util::FsyncPolicy fsync_policy)
    : path_(std::move(path)), fsync_policy_(fsync_policy) {}

SegmentLog::~SegmentLog() {
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

SegmentLog::SegmentLog(SegmentLog&& other) noexcept
    : path_(std::move(other.path_)),
      fsync_policy_(other.fsync_policy_),
      fd_(other.fd_),
      next_offset_(other.next_offset_),
      offset_to_position_(std::move(other.offset_to_position_)) {
  other.fd_ = -1;
  other.next_offset_ = 0;
}

SegmentLog& SegmentLog::operator=(SegmentLog&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  if (fd_ >= 0) {
    ::close(fd_);
  }
  path_ = std::move(other.path_);
  fsync_policy_ = other.fsync_policy_;
  fd_ = other.fd_;
  next_offset_ = other.next_offset_;
  offset_to_position_ = std::move(other.offset_to_position_);
  other.fd_ = -1;
  other.next_offset_ = 0;
  return *this;
}

util::Status SegmentLog::Open() {
  std::filesystem::create_directories(path_.parent_path());
  fd_ = ::open(path_.c_str(), O_CREAT | O_RDWR, 0644);
  if (fd_ < 0) {
    return util::Status(util::StatusCode::kIOError, "open segment failed");
  }
  return util::Status::Ok();
}

util::Status SegmentLog::Recover() {
  if (fd_ < 0) {
    return util::Status(util::StatusCode::kInvalidArgument, "segment not open");
  }
  offset_to_position_.clear();
  next_offset_ = 0;

  off_t position = 0;
  while (true) {
    RecordHeader header;
    ssize_t read_bytes = ::pread(fd_, &header, sizeof(header), position);
    if (read_bytes == 0) {
      break;
    }
    if (read_bytes < 0) {
      return util::Status(util::StatusCode::kIOError, "read segment failed");
    }
    if (static_cast<size_t>(read_bytes) < sizeof(header)) {
      ::ftruncate(fd_, position);
      break;
    }

    if (header.magic != kRecordMagic || header.version != kRecordVersion) {
      ::ftruncate(fd_, position);
      break;
    }

    uint64_t record_size = sizeof(RecordHeader) + header.routing_key_size + header.payload_size;
    off_t kv_cursor = position + sizeof(RecordHeader);
    for (uint16_t i = 0; i < header.header_kv_count; ++i) {
      uint32_t sizes[2];
      ssize_t size_bytes = ::pread(fd_, sizes, sizeof(sizes), kv_cursor);
      if (size_bytes < static_cast<ssize_t>(sizeof(sizes))) {
        ::ftruncate(fd_, position);
        return util::Status::Ok();
      }
      record_size += sizeof(uint32_t) * 2 + sizes[0] + sizes[1];
      kv_cursor += sizeof(uint32_t) * 2 + sizes[0] + sizes[1];
    }

    std::vector<std::byte> buffer(record_size);
    ssize_t total_read = ::pread(fd_, buffer.data(), record_size, position);
    if (total_read < static_cast<ssize_t>(record_size)) {
      ::ftruncate(fd_, position);
      break;
    }

    auto decoded = DecodeRecord(buffer);
    if (!decoded.ok()) {
      ::ftruncate(fd_, position);
      break;
    }

    offset_to_position_[header.offset] = static_cast<uint64_t>(position);
    next_offset_ = header.offset + 1;
    position += static_cast<off_t>(record_size);
  }

  return util::Status::Ok();
}

util::StatusOr<uint64_t> SegmentLog::Append(const Record& record) {
  if (fd_ < 0) {
    return util::Status(util::StatusCode::kInvalidArgument, "segment not open");
  }

  Record mutable_record = record;
  mutable_record.header.offset = next_offset_;
  auto encoded = EncodeRecord(mutable_record);
  if (!encoded.ok()) {
    return encoded.status();
  }
  auto& bytes = encoded.value();

  util::Status status = WriteAll(bytes.data(), bytes.size());
  if (!status.ok()) {
    return status;
  }

  if (fsync_policy_ == util::FsyncPolicy::kAlways) {
    status = util::FsyncFile(fd_);
    if (!status.ok()) {
      return status;
    }
  }

  offset_to_position_[next_offset_] = static_cast<uint64_t>(::lseek(fd_, 0, SEEK_CUR) - bytes.size());
  return next_offset_++;
}

util::StatusOr<Record> SegmentLog::Read(uint64_t offset) const {
  auto it = offset_to_position_.find(offset);
  if (it == offset_to_position_.end()) {
    return util::Status(util::StatusCode::kNotFound, "offset not found");
  }
  RecordHeader header;
  ssize_t read_bytes = ::pread(fd_, &header, sizeof(header), static_cast<off_t>(it->second));
  if (read_bytes < static_cast<ssize_t>(sizeof(header))) {
    return util::Status(util::StatusCode::kIOError, "read header failed");
  }
  uint64_t record_size = sizeof(RecordHeader) + header.routing_key_size + header.payload_size;
  off_t kv_cursor = static_cast<off_t>(it->second + sizeof(RecordHeader));
  for (uint16_t i = 0; i < header.header_kv_count; ++i) {
    uint32_t sizes[2];
    ssize_t size_bytes = ::pread(fd_, sizes, sizeof(sizes), kv_cursor);
    if (size_bytes < static_cast<ssize_t>(sizeof(sizes))) {
      return util::Status(util::StatusCode::kIOError, "read header kv failed");
    }
    record_size += sizeof(uint32_t) * 2 + sizes[0] + sizes[1];
    kv_cursor += sizeof(uint32_t) * 2 + sizes[0] + sizes[1];
  }
  auto buffer_or = ReadAt(it->second, record_size);
  if (!buffer_or.ok()) {
    return buffer_or.status();
  }
  return DecodeRecord(buffer_or.value());
}

std::vector<uint64_t> SegmentLog::Offsets() const {
  std::vector<uint64_t> offsets;
  offsets.reserve(offset_to_position_.size());
  for (const auto& [offset, _] : offset_to_position_) {
    offsets.push_back(offset);
  }
  return offsets;
}

uint64_t SegmentLog::SizeBytes() const {
  struct stat st {};
  if (::fstat(fd_, &st) != 0) {
    return 0;
  }
  return static_cast<uint64_t>(st.st_size);
}

util::StatusOr<std::vector<std::byte>> SegmentLog::ReadAt(uint64_t position, size_t size) const {
  std::vector<std::byte> buffer(size);
  ssize_t read_bytes = ::pread(fd_, buffer.data(), size, static_cast<off_t>(position));
  if (read_bytes < static_cast<ssize_t>(size)) {
    return util::Status(util::StatusCode::kIOError, "pread failed");
  }
  return buffer;
}

util::Status SegmentLog::WriteAll(const std::byte* data, size_t size) {
  size_t written = 0;
  while (written < size) {
    ssize_t result = ::write(fd_, data + written, size - written);
    if (result <= 0) {
      return util::Status(util::StatusCode::kIOError, "write failed");
    }
    written += static_cast<size_t>(result);
  }
  return util::Status::Ok();
}

}  // namespace pomai::queue::storage
