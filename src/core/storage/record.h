#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "src/core/model/message.h"
#include "src/core/util/status.h"
#include "src/core/util/status_or.h"

namespace pomai::queue::storage {

constexpr uint32_t kRecordMagic = 0x504D5151;  // "PMQQ"
constexpr uint16_t kRecordVersion = 1;

struct RecordHeader {
  uint32_t magic = kRecordMagic;
  uint16_t version = kRecordVersion;
  uint16_t header_kv_count = 0;
  uint32_t payload_size = 0;
  uint64_t crc64 = 0;
  uint64_t offset = 0;
  uint64_t enqueue_ts = 0;
  uint64_t msg_id_high = 0;
  uint64_t msg_id_low = 0;
  uint32_t routing_key_size = 0;
};

struct Record {
  RecordHeader header;
  std::vector<std::byte> payload;
  std::vector<std::pair<std::string, std::string>> headers;
  std::string routing_key;
};

util::StatusOr<std::vector<std::byte>> EncodeRecord(const Record& record);
util::StatusOr<Record> DecodeRecord(std::span<const std::byte> data);

}  // namespace pomai::queue::storage
