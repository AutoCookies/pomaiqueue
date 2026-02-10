#include <cassert>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <random>

#include "src/core/api/pomai_queue.h"

using namespace pomai::queue;

namespace {

model::Message Msg(uint64_t i, uint64_t ts) {
  model::Message m;
  m.id.high = 7;
  m.id.low = i;
  m.enqueue_ts = ts;
  m.payload = {std::byte{0x1}};
  m.routing_key = "crash";
  return m;
}

void RunCrashOracleSample() {
  auto base = std::filesystem::temp_directory_path() / "pomaiqueue-crash-oracle";
  std::filesystem::remove_all(base);

  api::EngineOptions options;
  options.data_dir = base.string();
  options.visibility_timeout = std::chrono::milliseconds(1);
  options.retry_backoff = std::chrono::milliseconds(0);

  std::mt19937_64 rng(42);
  uint64_t produced = 0;

  for (int iter = 0; iter < 20; ++iter) {
    api::PomaiQueue q(options);
    assert(q.Start().ok());
    auto create = q.CreateQueue("q");
    assert(create.ok() || create.code() == util::StatusCode::kAlreadyExists);

    if ((rng() % 2) == 0) {
      ++produced;
      assert(q.Produce("q", Msg(produced, 1000 + produced)).ok());
    }

    auto c = q.Consume("q", "g");
    if (!c.ok()) {
      q.Stop();
      continue;
    }
    if (c.value().has_value()) {
      if ((rng() % 3) == 0) {
        assert(q.Ack("q", "g", c.value()->message.id).ok());
      } else if ((rng() % 2) == 0) {
        assert(q.Nack("q", "g", c.value()->message.id, true).ok());
      }
    }

    auto stats = q.GetStats("q", "g");
    assert(stats.ok());
    assert(stats.value().ready_count + stats.value().inflight_count + stats.value().acked_count +
           stats.value().dead_count <= produced);

    assert(q.Stop().ok());
  }
}

}  // namespace

int main() {
  RunCrashOracleSample();
  return 0;
}
