#include <chrono>
#include <cstddef>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "src/core/api/pomai_queue.h"

using namespace pomai::queue;

namespace {

double RunEnqueue(const std::filesystem::path& dir, size_t payload_size, size_t n) {
  api::EngineOptions options;
  options.data_dir = dir.string();
  api::PomaiQueue q(options);
  q.Start();
  q.CreateQueue("q");

  auto start = std::chrono::steady_clock::now();
  for (size_t i = 0; i < n; ++i) {
    model::Message m;
    m.id.high = 1;
    m.id.low = i + 1;
    m.enqueue_ts = i;
    m.payload.assign(payload_size, std::byte{0x1});
    m.routing_key = "bench";
    q.Produce("q", m);
  }
  auto end = std::chrono::steady_clock::now();
  q.Stop();
  const double sec = std::chrono::duration<double>(end - start).count();
  return static_cast<double>(n) / sec;
}

}  // namespace

int main() {
  auto base = std::filesystem::temp_directory_path() / "pomaiqueue-bench";
  std::filesystem::remove_all(base);
  std::filesystem::create_directories(base);

  const std::vector<size_t> sizes{64, 1024, 16 * 1024};
  std::cout << "{\n  \"bench_enqueue\": [\n";
  for (size_t i = 0; i < sizes.size(); ++i) {
    auto dir = base / std::to_string(sizes[i]);
    std::filesystem::remove_all(dir);
    const double tput = RunEnqueue(dir, sizes[i], 2000);
    std::cout << "    {\"payload\": " << sizes[i] << ", \"msgs_per_sec\": " << tput << "}";
    if (i + 1 < sizes.size()) {
      std::cout << ",";
    }
    std::cout << "\n";
  }
  std::cout << "  ]\n}\n";
  return 0;
}
