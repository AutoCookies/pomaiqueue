#include "src/core/util/crc64.h"

namespace pomai::queue::util {

namespace {
constexpr uint64_t kPoly = 0x42F0E1EBA9EA3693ULL;

uint64_t TableEntry(uint64_t idx) {
  uint64_t crc = idx << 56;
  for (int i = 0; i < 8; ++i) {
    if (crc & 0x8000000000000000ULL) {
      crc = (crc << 1) ^ kPoly;
    } else {
      crc <<= 1;
    }
  }
  return crc;
}

const uint64_t* Table() {
  static uint64_t table[256];
  static bool initialized = false;
  if (!initialized) {
    for (uint64_t i = 0; i < 256; ++i) {
      table[i] = TableEntry(i);
    }
    initialized = true;
  }
  return table;
}
}  // namespace

uint64_t Crc64::Compute(const std::byte* data, size_t size) {
  const uint64_t* table = Table();
  uint64_t crc = 0;
  for (size_t i = 0; i < size; ++i) {
    uint8_t byte = static_cast<uint8_t>(data[i]);
    uint8_t idx = static_cast<uint8_t>((crc >> 56) ^ byte);
    crc = table[idx] ^ (crc << 8);
  }
  return crc;
}

}  // namespace pomai::queue::util
