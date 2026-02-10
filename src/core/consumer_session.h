#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace pomai::queue::core {

struct ConsumerSession {
  std::string session_id;
  std::string consumer_id;
  std::string group_id;
  uint32_t max_inflight = 0;
  uint32_t credit_window = 0;
  uint32_t inflight = 0;
  uint64_t last_heartbeat_ts_ms = 0;

  bool CanDispatch() const;
  bool TryDispatch();
  void AddCredit(uint32_t credit);
  void AckOne();
  bool Expired(uint64_t now_ms, uint64_t heartbeat_timeout_ms) const;
};

class ConsumerSessionScheduler {
 public:
  explicit ConsumerSessionScheduler(std::vector<ConsumerSession> sessions);

  std::optional<size_t> NextDispatchable();
  std::vector<ConsumerSession>& sessions() { return sessions_; }

 private:
  std::vector<ConsumerSession> sessions_;
  size_t cursor_ = 0;
};

}  // namespace pomai::queue::core
