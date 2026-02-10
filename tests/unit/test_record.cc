#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "src/core/storage/record.h"

using pomai::queue::storage::DecodeRecord;
using pomai::queue::storage::EncodeRecord;
using pomai::queue::storage::Record;

int main() {
  Record record;
  record.header.msg_id_high = 42;
  record.header.msg_id_low = 7;
  record.header.enqueue_ts = 123456;
  record.payload = {std::byte{0x01}, std::byte{0x02}};
  record.routing_key = "rk";
  record.headers.emplace_back("key", "value");

  auto encoded = EncodeRecord(record);
  assert(encoded.ok());

  auto decoded = DecodeRecord(encoded.value());
  assert(decoded.ok());
  assert(decoded.value().header.msg_id_high == record.header.msg_id_high);
  assert(decoded.value().header.msg_id_low == record.header.msg_id_low);
  assert(decoded.value().header.enqueue_ts == record.header.enqueue_ts);
  assert(decoded.value().payload == record.payload);

  auto corrupted_crc = encoded.value();
  corrupted_crc.back() = std::byte{0x99};
  auto crc_fail = DecodeRecord(corrupted_crc);
  assert(!crc_fail.ok());

  auto truncated = encoded.value();
  truncated.resize(truncated.size() - 1);
  auto truncated_fail = DecodeRecord(truncated);
  assert(!truncated_fail.ok());

  auto bad_magic = encoded.value();
  reinterpret_cast<uint32_t*>(bad_magic.data())[0] = 0;
  auto magic_fail = DecodeRecord(bad_magic);
  assert(!magic_fail.ok());
  return 0;
}
