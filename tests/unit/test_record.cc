#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>

#include "src/core/storage/record.h"

int main() {
  pomai::queue::storage::Record record;
  record.header.msg_id_high = 42;
  record.header.msg_id_low = 7;
  record.header.enqueue_ts = 123456;
  record.payload = {std::byte{0x01}, std::byte{0x02}};
  record.routing_key = "rk";
  record.headers.emplace_back("key", "value");

  auto encoded = pomai::queue::storage::EncodeRecord(record);
  assert(encoded.ok());

  auto decoded = pomai::queue::storage::DecodeRecord(encoded.value());
  assert(decoded.ok());

  assert(decoded.value().header.msg_id_high == record.header.msg_id_high);
  assert(decoded.value().header.msg_id_low == record.header.msg_id_low);
  assert(decoded.value().header.enqueue_ts == record.header.enqueue_ts);
  assert(decoded.value().payload.size() == record.payload.size());
  assert(decoded.value().routing_key == record.routing_key);
  assert(decoded.value().headers.size() == 1);
  assert(decoded.value().headers[0].first == "key");
  assert(decoded.value().headers[0].second == "value");

  return 0;
}
