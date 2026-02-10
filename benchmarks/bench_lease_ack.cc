#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <iostream>
#include <vector>

#include "src/core/api/pomai_queue.h"

using namespace pomai::queue;

int main() {
  auto base = std::filesystem::temp_directory_path() / "pomaiqueue-bench-lease-ack";
  std::filesystem::remove_all(base);
  api::EngineOptions o;
  o.data_dir = base.string();
  api::PomaiQueue q(o);
  assert(q.Start().ok());
  assert(q.CreateQueue("q").ok());

  constexpr size_t kN = 3000;
  for (size_t i = 0; i < kN; ++i) {
    model::Message m;
    m.id.high = 4;
    m.id.low = i + 1;
    m.enqueue_ts = i + 1;
    m.payload = {std::byte{0x9}};
    m.routing_key = "b";
    assert(q.Produce("q", m).ok());
  }

  auto start = std::chrono::steady_clock::now();
  std::vector<double> lat;
  lat.reserve(kN);
  for (size_t i = 0; i < kN; ++i) {
    const auto s = std::chrono::steady_clock::now();
    auto c = q.Consume("q", "g");
    assert(c.ok() && c.value().has_value());
    assert(q.Ack("q", "g", c.value()->message.id).ok());
    const auto e = std::chrono::steady_clock::now();
    lat.push_back(std::chrono::duration<double, std::micro>(e - s).count());
  }
  auto end = std::chrono::steady_clock::now();

  std::sort(lat.begin(), lat.end());
  double p95 = lat[static_cast<size_t>(lat.size() * 0.95)];
  double p99 = lat[static_cast<size_t>(lat.size() * 0.99)];
  double sec = std::chrono::duration<double>(end - start).count();
  double tput = static_cast<double>(kN) / sec;
  assert(!(p95 < 0 || p99 < 0));

  auto stats = q.GetStats("q", "g");
  assert(stats.ok());
  assert(stats.value().acked_count == kN);

  std::cout << "{\"bench\":\"lease_ack\",\"ops_per_sec\":" << tput << ",\"p95_us\":" << p95
            << ",\"p99_us\":" << p99 << "}\n";
  q.Stop();
  return 0;
}
