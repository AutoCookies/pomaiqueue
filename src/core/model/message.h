#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace pomai::queue::model {

struct MessageId {
  uint64_t high = 0;
  uint64_t low = 0;

  bool operator==(const MessageId& other) const {
    return high == other.high && low == other.low;
  }

  bool operator<(const MessageId& other) const {
    return high < other.high || (high == other.high && low < other.low);
  }
};

struct MessageIdHash {
  size_t operator()(const MessageId& id) const noexcept {
    return static_cast<size_t>(id.high ^ id.low);
  }
};

struct Message {
  MessageId id;
  uint64_t enqueue_ts = 0;
  std::vector<std::byte> payload;
  std::map<std::string, std::string> headers;
  std::string routing_key;
};

}  // namespace pomai::queue::model
