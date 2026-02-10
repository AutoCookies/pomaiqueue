#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <set>
#include <vector>

#include "src/core/api/pomai_queue.h"

using namespace pomai::queue;

namespace {
struct FakeClock {
  std::atomic<uint64_t> now{1000};
  uint64_t Now() const { return now.load(); }
  void Advance(uint64_t ms) { now.fetch_add(ms); }
};

model::Message Msg(uint64_t i, uint64_t ts) {
  model::Message m;
  m.id.high = 9;
  m.id.low = i;
  m.enqueue_ts = ts;
  m.payload = {std::byte{0x1}};
  m.routing_key = "int";
  return m;
}
}

int main() {
  auto base = std::filesystem::temp_directory_path() / "pomaiqueue-it";
  std::filesystem::remove_all(base);
  FakeClock clk;
  api::EngineOptions opt;
  opt.data_dir = base.string();
  opt.max_retry_count = 1;
  opt.visibility_timeout = std::chrono::milliseconds(10);
  opt.retry_backoff = std::chrono::milliseconds(2);
  opt.scheduler_tick = std::chrono::milliseconds(1);
  opt.now_fn = [&clk]() { return clk.Now(); };

  api::PomaiQueue q(opt);
  assert(q.Start().ok());
  assert(q.CreateQueue("q").ok());

  constexpr uint64_t kTotal = 50;
  for (uint64_t i = 1; i <= kTotal; ++i) {
    assert(q.Produce("q", Msg(i, 1000 + i)).ok());
  }

  std::set<uint64_t> acked;
  uint64_t dead = 0;
  for (uint64_t step = 0; step < 500 && acked.size() + dead < kTotal; ++step) {
    auto c = q.Consume("q", "g");
    assert(c.ok());
    if (!c.value().has_value()) {
      clk.Advance(3);
      continue;
    }
    const uint64_t id = c.value()->message.id.low;
    if (id % 10 == 0) {
      assert(q.Nack("q", "g", c.value()->message.id, false).ok());
    } else {
      auto ack = q.Ack("q", "g", c.value()->message.id);
      assert(ack.ok());
      acked.insert(id);
    }
    clk.Advance(1);
    auto st = q.GetStats("q", "g");
    assert(st.ok());
    dead = st.value().dead_count;
  }

  auto stats = q.GetStats("q", "g");
  assert(stats.ok());
  assert(stats.value().acked_count == acked.size());
  assert(stats.value().dead_count == kTotal / 10);

  assert(q.Stop().ok());

  // restart persistence + ordering on remaining queue/group
  api::PomaiQueue q2(opt);
  assert(q2.Start().ok());
  auto create2 = q2.CreateQueue("q");
  assert(create2.ok() || create2.code() == util::StatusCode::kAlreadyExists);
  auto replay = q2.ReplayToSequence("q", "g", UINT64_MAX);
  assert(replay.ok());
  assert(replay.value().stats.acked_count == acked.size());
  assert(q2.Stop().ok());
  return 0;
}
