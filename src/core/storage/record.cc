#include "src/core/storage/record.h"

#include <cstring>

#include "src/core/util/crc64.h"

namespace pomai::queue::storage {

namespace {

template <typename T>
void Append(std::vector<std::byte>& out, const T& value) {
  const std::byte* ptr = reinterpret_cast<const std::byte*>(&value);
  out.insert(out.end(), ptr, ptr + sizeof(T));
}

void AppendBytes(std::vector<std::byte>& out, std::span<const std::byte> bytes) {
  out.insert(out.end(), bytes.begin(), bytes.end());
}

util::StatusOr<size_t> ConsumeHeader(std::span<const std::byte> data, RecordHeader& header) {
  if (data.size() < sizeof(RecordHeader)) {
    return util::Status(util::StatusCode::kCorruption, "record header too small");
  }
  std::memcpy(&header, data.data(), sizeof(RecordHeader));
  if (header.magic != kRecordMagic || header.version != kRecordVersion) {
    return util::Status(util::StatusCode::kCorruption, "record header magic/version mismatch");
  }
  return sizeof(RecordHeader);
}

}  // namespace

util::StatusOr<std::vector<std::byte>> EncodeRecord(const Record& record) {
  RecordHeader header = record.header;
  header.payload_size = static_cast<uint32_t>(record.payload.size());
  header.header_kv_count = static_cast<uint16_t>(record.headers.size());
  header.routing_key_size = static_cast<uint32_t>(record.routing_key.size());

  std::vector<std::byte> out;
  out.reserve(sizeof(RecordHeader) + record.payload.size());

  Append(out, header);

  for (const auto& kv : record.headers) {
    uint32_t key_size = static_cast<uint32_t>(kv.first.size());
    uint32_t value_size = static_cast<uint32_t>(kv.second.size());
    Append(out, key_size);
    Append(out, value_size);
    AppendBytes(out, std::span<const std::byte>(reinterpret_cast<const std::byte*>(kv.first.data()), key_size));
    AppendBytes(out, std::span<const std::byte>(reinterpret_cast<const std::byte*>(kv.second.data()), value_size));
  }

  AppendBytes(out, std::span<const std::byte>(reinterpret_cast<const std::byte*>(record.routing_key.data()), record.routing_key.size()));
  AppendBytes(out, record.payload);

  uint64_t crc = util::Crc64::Compute(out.data() + sizeof(RecordHeader), out.size() - sizeof(RecordHeader));
  std::memcpy(out.data() + offsetof(RecordHeader, crc64), &crc, sizeof(uint64_t));

  return out;
}

util::StatusOr<Record> DecodeRecord(std::span<const std::byte> data) {
  RecordHeader header;
  auto header_size = ConsumeHeader(data, header);
  if (!header_size.ok()) {
    return header_size.status();
  }

  size_t offset = header_size.value();
  size_t expected_payload_size = header.payload_size;
  size_t expected_routing_key_size = header.routing_key_size;

  auto span_size = data.size();

  std::vector<std::pair<std::string, std::string>> headers;
  headers.reserve(header.header_kv_count);
  for (uint16_t i = 0; i < header.header_kv_count; ++i) {
    if (offset + sizeof(uint32_t) * 2 > span_size) {
      return util::Status(util::StatusCode::kCorruption, "record header kv too small");
    }
    uint32_t key_size = 0;
    uint32_t value_size = 0;
    std::memcpy(&key_size, data.data() + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    std::memcpy(&value_size, data.data() + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    if (offset + key_size + value_size > span_size) {
      return util::Status(util::StatusCode::kCorruption, "record header kv truncated");
    }
    std::string key(reinterpret_cast<const char*>(data.data() + offset), key_size);
    offset += key_size;
    std::string value(reinterpret_cast<const char*>(data.data() + offset), value_size);
    offset += value_size;
    headers.emplace_back(std::move(key), std::move(value));
  }

  if (offset + expected_routing_key_size + expected_payload_size > span_size) {
    return util::Status(util::StatusCode::kCorruption, "record payload truncated");
  }

  std::string routing_key(reinterpret_cast<const char*>(data.data() + offset), expected_routing_key_size);
  offset += expected_routing_key_size;

  std::vector<std::byte> payload(expected_payload_size);
  std::memcpy(payload.data(), data.data() + offset, expected_payload_size);

  uint64_t crc = util::Crc64::Compute(data.data() + sizeof(RecordHeader), data.size() - sizeof(RecordHeader));
  if (crc != header.crc64) {
    return util::Status(util::StatusCode::kCorruption, "record crc mismatch");
  }

  Record record;
  record.header = header;
  record.headers = std::move(headers);
  record.routing_key = std::move(routing_key);
  record.payload = std::move(payload);
  return record;
}

}  // namespace pomai::queue::storage
