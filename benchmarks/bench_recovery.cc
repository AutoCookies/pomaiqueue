#include <cassert>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <iostream>
#include <vector>

#include "src/core/api/pomai_queue.h"

using namespace pomai::queue;

int main() {
  auto base = std::filesystem::temp_directory_path() / "pomaiqueue-bench-recovery";
  std::filesystem::remove_all(base);
  std::vector<int> counts{1000, 5000, 10000};
  std::cout << "{\"bench\":\"recovery\",\"cases\":[";
  for (size_t i = 0; i < counts.size(); ++i) {
    auto dir = base / std::to_string(i);
    api::EngineOptions o;
    o.data_dir = dir.string();
    {
      api::PomaiQueue q(o);
      assert(q.Start().ok());
      assert(q.CreateQueue("q").ok());
      for (int n = 0; n < counts[i]; ++n) {
        model::Message m;
        m.id.high = 10;
        m.id.low = n + 1;
        m.enqueue_ts = n;
        m.payload = {std::byte{0x3}};
        m.routing_key = "r";
        assert(q.Produce("q", m).ok());
      }
      q.Stop();
    }
    auto start = std::chrono::steady_clock::now();
    api::PomaiQueue qr(o);
    assert(qr.Start().ok());
    auto create = qr.CreateQueue("q");
    assert(create.ok() || create.code() == util::StatusCode::kAlreadyExists);
    auto end = std::chrono::steady_clock::now();
    auto stats = qr.GetStats("q", "g");
    assert(stats.ok());
    qr.Stop();

    double ms = std::chrono::duration<double, std::milli>(end - start).count();
    std::cout << "{\"messages\":" << counts[i] << ",\"recovery_ms\":" << ms << "}";
    if (i + 1 < counts.size()) std::cout << ",";
  }
  std::cout << "]}\n";
  return 0;
}
