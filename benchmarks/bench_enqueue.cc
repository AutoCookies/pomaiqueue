#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <iostream>
#include <vector>

#include "src/core/api/pomai_queue.h"

using namespace pomai::queue;

double NowSec() {
  return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

int main() {
  auto base = std::filesystem::temp_directory_path() / "pomaiqueue-bench-enqueue";
  std::filesystem::remove_all(base);
  const std::vector<size_t> sizes{64, 1024, 16 * 1024};
  constexpr size_t kN = 2000;

  std::cout << "{\"bench\":\"enqueue\",\"cases\":[";
  for (size_t i = 0; i < sizes.size(); ++i) {
    api::EngineOptions o;
    o.data_dir = (base / std::to_string(i)).string();
    api::PomaiQueue q(o);
    q.Start();
    q.CreateQueue("q");

    const double start = NowSec();
    double p95_us = 0;
    std::vector<double> lat;
    lat.reserve(kN);
    for (size_t n = 0; n < kN; ++n) {
      model::Message m;
      m.id.high = 1;
      m.id.low = n + 1;
      m.enqueue_ts = n;
      m.payload.assign(sizes[i], std::byte{0x1});
      m.routing_key = "bench";
      const auto s = std::chrono::steady_clock::now();
      auto st = q.Produce("q", m);
      assert(st.ok());
      const auto e = std::chrono::steady_clock::now();
      lat.push_back(std::chrono::duration<double, std::micro>(e - s).count());
    }
    std::sort(lat.begin(), lat.end());
    p95_us = lat[static_cast<size_t>(lat.size() * 0.95)];
    const double sec = NowSec() - start;
    const double msgps = static_cast<double>(kN) / sec;
    const double bytesps = msgps * static_cast<double>(sizes[i]);

    auto stats = q.GetStats("q", "g");
    assert(stats.ok());
    assert(stats.value().ready_count == kN);

    q.Stop();
    std::cout << "{\"payload\":" << sizes[i] << ",\"msgs_per_sec\":" << msgps
              << ",\"bytes_per_sec\":" << bytesps << ",\"p95_enqueue_us\":" << p95_us << "}";
    if (i + 1 < sizes.size()) std::cout << ",";
  }
  std::cout << "]}\n";
  return 0;
}
