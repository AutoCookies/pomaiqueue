#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <filesystem>

#include "src/core/api/pomai_queue.h"

using namespace pomai::queue;

namespace {

struct FakeClock {
  std::atomic<uint64_t> now_ms{100};
  uint64_t Now() const { return now_ms.load(); }
  void Advance(uint64_t delta_ms) { now_ms.fetch_add(delta_ms); }
  void Set(uint64_t v) { now_ms.store(v); }
};

model::Message BuildMessage(uint64_t low, uint64_t ts, uint8_t b = 0x42) {
  model::Message message;
  message.id.high = 1;
  message.id.low = low;
  message.enqueue_ts = ts;
  message.payload = {std::byte{b}};
  message.routing_key = "jobs";
  return message;
}

api::EngineOptions BaseOptions(const std::filesystem::path& base, FakeClock& clock) {
  api::EngineOptions options;
  options.data_dir = base.string();
  options.retry_backoff = std::chrono::milliseconds(5);
  options.visibility_timeout = std::chrono::milliseconds(5);
  options.scheduler_tick = std::chrono::milliseconds(1);
  options.now_fn = [&clock]() { return clock.Now(); };
  return options;
}

void TestStateTransitionsAndIdempotency() {
  auto base = std::filesystem::temp_directory_path() / "pomaiqueue-test-transitions";
  std::filesystem::remove_all(base);
  FakeClock clock;
  auto options = BaseOptions(base, clock);
  options.max_retry_count = 1;

  api::PomaiQueue queue(options);
  assert(queue.Start().ok());
  assert(queue.CreateQueue("q").ok());
  assert(queue.Produce("q", BuildMessage(10, 100)).ok());
  assert(queue.Produce("q", BuildMessage(11, 101)).ok());

  auto first = queue.Consume("q", "g");
  assert(first.ok() && first.value().has_value());
  auto id = first.value()->message.id;

  auto ack_ok = queue.Ack("q", "g", id);
  assert(ack_ok.ok());
  auto dup_ack = queue.Ack("q", "g", id);
  assert(!dup_ack.ok());

  auto second = queue.Consume("q", "g");
  assert(second.ok() && second.value().has_value());
  clock.Advance(20);
  auto reap = queue.GetStats("q", "g");
  assert(reap.ok());
  auto ack_after_timeout = queue.Ack("q", "g", second.value()->message.id);
  assert(!ack_after_timeout.ok());

  clock.Advance(10);
  auto redelivery = queue.Consume("q", "g");
  assert(redelivery.ok() && redelivery.value().has_value());
  assert(redelivery.value()->message.id.low == 11);
  assert(queue.Nack("q", "g", redelivery.value()->message.id, false).ok());

  auto stats = queue.GetStats("q", "g");
  assert(stats.ok());
  assert(stats.value().dead_count == 1);

  auto inspect = queue.InspectMessage("q", "g", redelivery.value()->message.id);
  assert(inspect.ok());
  assert(inspect.value().state == engine::MessageState::kDead);
  assert(inspect.value().retry_count >= 1);

  assert(queue.Stop().ok());
}

void TestBackpressureBounds() {
  auto base = std::filesystem::temp_directory_path() / "pomaiqueue-test-backpressure";
  std::filesystem::remove_all(base);
  FakeClock clock;
  auto options = BaseOptions(base, clock);
  options.max_queue_depth = 2;
  options.max_inflight = 1;

  api::PomaiQueue queue(options);
  assert(queue.Start().ok());
  assert(queue.CreateQueue("q").ok());
  assert(queue.Produce("q", BuildMessage(1, 100)).ok());
  assert(queue.Produce("q", BuildMessage(2, 101)).ok());
  auto third = queue.Produce("q", BuildMessage(3, 102));
  assert(!third.ok());

  auto c1 = queue.Consume("q", "g");
  assert(c1.ok() && c1.value().has_value());
  auto c2 = queue.Consume("q", "g");
  assert(c2.ok() && !c2.value().has_value());
  assert(queue.Stop().ok());
}

void TestRestartInflightRecoveredToReady() {
  auto base = std::filesystem::temp_directory_path() / "pomaiqueue-test-restart";
  std::filesystem::remove_all(base);
  FakeClock clock;
  auto options = BaseOptions(base, clock);
  {
    api::PomaiQueue queue(options);
    assert(queue.Start().ok());
    assert(queue.CreateQueue("q").ok());
    assert(queue.Produce("q", BuildMessage(20, 200)).ok());
    auto first = queue.Consume("q", "g");
    assert(first.ok() && first.value().has_value());
    assert(queue.Stop().ok());
  }
  {
    api::PomaiQueue queue(options);
    assert(queue.Start().ok());
    auto create = queue.CreateQueue("q");
    assert(create.ok() || create.code() == util::StatusCode::kAlreadyExists);
    auto c = queue.Consume("q", "g");
    assert(c.ok() && c.value().has_value());
    assert(c.value()->message.id.low == 20);
    assert(queue.Stop().ok());
  }
}

}  // namespace

int main() {
  TestStateTransitionsAndIdempotency();
  TestBackpressureBounds();
  TestRestartInflightRecoveredToReady();
  return 0;
}
