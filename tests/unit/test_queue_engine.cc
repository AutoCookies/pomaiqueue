#include <cassert>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <thread>

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

void TestRetryAndDlq() {
  auto base = std::filesystem::temp_directory_path() / "pomaiqueue-test-retry";
  std::filesystem::remove_all(base);

  api::EngineOptions options;
  options.data_dir = base.string();
  options.max_retry_count = 1;
  options.retry_backoff = std::chrono::milliseconds(0);
  options.visibility_timeout = std::chrono::milliseconds(1);

  api::PomaiQueue queue(options);
  assert(queue.Start().ok());
  assert(queue.CreateQueue("q").ok());
  assert(queue.Produce("q", BuildMessage(10, 100)).ok());

  auto first = queue.Consume("q", "g");
  assert(first.ok());
  assert(first.value().has_value());

  std::this_thread::sleep_for(std::chrono::milliseconds(5));

  auto second = queue.Consume("q", "g");
  assert(second.ok());
  assert(second.value().has_value());

  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  auto third = queue.Consume("q", "g");
  assert(third.ok());
  assert(!third.value().has_value());

  auto stats = queue.GetStats("q", "g");
  assert(stats.ok());
  assert(stats.value().dead_count == 1);

  auto inspect = queue.InspectMessage("q", "g", BuildMessage(10, 100).id);
  assert(inspect.ok());
  assert(inspect.value().retry_count >= 2);
  assert(queue.Stop().ok());
}

void TestCrashRecoveryInflightToReady() {
  auto base = std::filesystem::temp_directory_path() / "pomaiqueue-test-crash";
  std::filesystem::remove_all(base);

  api::EngineOptions options;
  options.data_dir = base.string();

  {
    api::PomaiQueue queue(options);
    assert(queue.Start().ok());
    assert(queue.CreateQueue("q").ok());
    assert(queue.Produce("q", BuildMessage(20, 200)).ok());
    auto first = queue.Consume("q", "g");
    assert(first.ok());
    assert(first.value().has_value());
    // no ack: simulate crash
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

}  // namespace

int main() {
  TestRetryAndDlq();
  TestCrashRecoveryInflightToReady();
  return 0;
}
