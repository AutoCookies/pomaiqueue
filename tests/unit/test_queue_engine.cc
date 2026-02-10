#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <filesystem>

#include "src/core/api/pomai_queue.h"

using namespace pomai::queue;

namespace {

model::Message BuildMessage(uint64_t low, uint64_t ts) {
  model::Message message;
  message.id.high = 1;
  message.id.low = low;
  message.enqueue_ts = ts;
  message.payload = {std::byte{0x42}};
  message.routing_key = "jobs";
  return message;
}

void TestRetryAndDlqWithFakeClock() {
  auto base = std::filesystem::temp_directory_path() / "pomaiqueue-test-retry";
  std::filesystem::remove_all(base);

  std::atomic<uint64_t> fake_now{100};
  api::EngineOptions options;
  options.data_dir = base.string();
  options.max_retry_count = 1;
  options.retry_backoff = std::chrono::milliseconds(1);
  options.visibility_timeout = std::chrono::milliseconds(1);
  options.scheduler_tick = std::chrono::milliseconds(1);
  options.now_fn = [&fake_now]() { return fake_now.load(); };

  api::PomaiQueue queue(options);
  assert(queue.Start().ok());
  assert(queue.CreateQueue("q").ok());
  assert(queue.Produce("q", BuildMessage(10, 100)).ok());

  auto first = queue.Consume("q", "g");
  assert(first.ok());
  assert(first.value().has_value());

  fake_now.store(106);
  bool saw_redelivery = false;
  for (int i = 0; i < 3; ++i) {
    auto second = queue.Consume("q", "g");
    assert(second.ok());
    if (second.value().has_value()) {
      saw_redelivery = true;
      break;
    }
    fake_now.fetch_add(1);
  }
  assert(saw_redelivery);

  fake_now.store(120);
  auto third = queue.Consume("q", "g");
  assert(third.ok());
  assert(!third.value().has_value());

  auto stats = queue.GetStats("q", "g");
  assert(stats.ok());
  assert(stats.value().dead_count == 1);

  auto inspect = queue.InspectMessage("q", "g", BuildMessage(10, 100).id);
  assert(inspect.ok());
  assert(inspect.value().retry_count >= 2);
  assert(inspect.value().sequence > 0);
  assert(queue.Stop().ok());
}

void TestCrashRecoveryInflightToReady() {
  auto base = std::filesystem::temp_directory_path() / "pomaiqueue-test-crash";
  std::filesystem::remove_all(base);

  std::atomic<uint64_t> fake_now{1000};
  api::EngineOptions options;
  options.data_dir = base.string();
  options.now_fn = [&fake_now]() { return fake_now.load(); };

  {
    api::PomaiQueue queue(options);
    assert(queue.Start().ok());
    assert(queue.CreateQueue("q").ok());
    assert(queue.Produce("q", BuildMessage(20, 200)).ok());
    auto first = queue.Consume("q", "g");
    assert(first.ok());
    assert(first.value().has_value());
    assert(queue.Stop().ok());
  }

  {
    api::PomaiQueue queue(options);
    assert(queue.Start().ok());
    auto create_status = queue.CreateQueue("q");
    assert(create_status.code() == pomai::queue::util::StatusCode::kAlreadyExists || create_status.ok());
    auto redelivery = queue.Consume("q", "g");
    assert(redelivery.ok());
    assert(redelivery.value().has_value());
    assert(queue.Ack("q", "g", redelivery.value()->message.id).ok());
    auto stats = queue.GetStats("q", "g");
    assert(stats.ok());
    assert(stats.value().acked_count == 1);
    assert(queue.Stop().ok());
  }
}


void TestInvalidShardConfigDoesNotCrash() {
  auto base = std::filesystem::temp_directory_path() / "pomaiqueue-test-invalid-shards";
  std::filesystem::remove_all(base);

  api::EngineOptions options;
  options.data_dir = base.string();
  options.shard_count = 0;

  api::PomaiQueue queue(options);
  auto start = queue.Start();
  assert(!start.ok());
  assert(start.code() == util::StatusCode::kInvalidArgument);

  auto consume = queue.Consume("q", "g");
  assert(!consume.ok());
}

void TestReplayToSequence() {
  auto base = std::filesystem::temp_directory_path() / "pomaiqueue-test-replay";
  std::filesystem::remove_all(base);

  std::atomic<uint64_t> fake_now{200};
  api::EngineOptions options;
  options.data_dir = base.string();
  options.now_fn = [&fake_now]() { return fake_now.load(); };
  options.visibility_timeout = std::chrono::milliseconds(5);

  api::PomaiQueue queue(options);
  assert(queue.Start().ok());
  assert(queue.CreateQueue("q").ok());
  assert(queue.Produce("q", BuildMessage(30, 200)).ok());
  auto c = queue.Consume("q", "g");
  assert(c.ok() && c.value().has_value());
  assert(queue.Ack("q", "g", c.value()->message.id).ok());

  auto replay_mid = queue.ReplayToSequence("q", "g", 1);
  assert(replay_mid.ok());
  assert(replay_mid.value().stats.inflight_count == 1);

  auto replay_end = queue.ReplayToSequence("q", "g", 2);
  assert(replay_end.ok());
  assert(replay_end.value().stats.acked_count == 1);
  assert(queue.Stop().ok());
}

}  // namespace

int main() {
  TestRetryAndDlqWithFakeClock();
  TestCrashRecoveryInflightToReady();
  TestReplayToSequence();
  TestInvalidShardConfigDoesNotCrash();
  return 0;
}
