#pragma once

#include <cstddef>
#include <cstdint>

namespace pomai::queue::util {

class Crc64 {
 public:
  static uint64_t Compute(const std::byte* data, size_t size);
};

}  // namespace pomai::queue::util
