#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <iostream>

#include "src/core/api/pomai_queue.h"

using namespace pomai::queue;

int main() {
  std::atomic<uint64_t> now{100};
  auto base = std::filesystem::temp_directory_path() / "pomaiqueue-bench-timeout";
  std::filesystem::remove_all(base);
  api::EngineOptions o;
  o.data_dir = base.string();
  o.max_retry_count = 2;
  o.max_inflight = 1000;
  o.visibility_timeout = std::chrono::milliseconds(5);
  o.retry_backoff = std::chrono::milliseconds(1);
  o.scheduler_tick = std::chrono::milliseconds(1);
  o.now_fn = [&now]() { return now.load(); };

  api::PomaiQueue q(o);
  assert(q.Start().ok());
  assert(q.CreateQueue("q").ok());
  constexpr uint64_t kN = 500;
  for (uint64_t i = 0; i < kN; ++i) {
    model::Message m;
    m.id.high = 3;
    m.id.low = i + 1;
    m.enqueue_ts = i;
    m.payload = {std::byte{0x4}};
    m.routing_key = "to";
    assert(q.Produce("q", m).ok());
  }

  auto s = std::chrono::steady_clock::now();
  for (uint64_t i = 0; i < kN; ++i) {
    auto c = q.Consume("q", "g");
    assert(c.ok() && c.value().has_value());
  }
  now.store(1000);
  for (uint64_t i = 0; i < kN; ++i) {
    auto c = q.Consume("q", "g");
    if (c.ok() && c.value().has_value()) {
      assert(q.Ack("q", "g", c.value()->message.id).ok());
    }
  }
  auto e = std::chrono::steady_clock::now();

  auto st = q.GetStats("q", "g");
  assert(st.ok());
  assert(st.value().acked_count <= kN);
  const double ms = std::chrono::duration<double, std::milli>(e - s).count();
  std::cout << "{\"bench\":\"timeout_retry\",\"processed\":" << (st.value().acked_count + st.value().dead_count)
            << ",\"elapsed_ms\":" << ms << ",\"drift_ms\":" << (now.load() - 100) << "}\n";
  q.Stop();
  return 0;
}
