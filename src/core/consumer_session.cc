#include "src/core/consumer_session.h"

#include <algorithm>
#include <utility>

namespace pomai::queue::core {

bool ConsumerSession::CanDispatch() const {
  return credit_window > 0 && inflight < max_inflight;
}

bool ConsumerSession::TryDispatch() {
  if (!CanDispatch()) {
    return false;
  }
  --credit_window;
  ++inflight;
  return true;
}

void ConsumerSession::AddCredit(uint32_t credit) {
  credit_window += credit;
}

void ConsumerSession::AckOne() {
  if (inflight > 0) {
    --inflight;
  }
}

bool ConsumerSession::Expired(uint64_t now_ms, uint64_t heartbeat_timeout_ms) const {
  return now_ms > last_heartbeat_ts_ms &&
         (now_ms - last_heartbeat_ts_ms) > heartbeat_timeout_ms;
}

ConsumerSessionScheduler::ConsumerSessionScheduler(std::vector<ConsumerSession> sessions)
    : sessions_(std::move(sessions)) {
  std::sort(sessions_.begin(), sessions_.end(), [](const ConsumerSession& a, const ConsumerSession& b) {
    return a.session_id < b.session_id;
  });
}

std::optional<size_t> ConsumerSessionScheduler::NextDispatchable() {
  if (sessions_.empty()) {
    return std::nullopt;
  }
  const size_t scan_count = sessions_.size();
  for (size_t scanned = 0; scanned < scan_count; ++scanned) {
    const size_t idx = (cursor_ + scanned) % sessions_.size();
    if (sessions_[idx].CanDispatch()) {
      cursor_ = (idx + 1) % sessions_.size();
      return idx;
    }
  }
  return std::nullopt;
}

}  // namespace pomai::queue::core
