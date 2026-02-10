#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "src/core/api/pomai_queue.h"

using namespace pomai::queue;

int main(int argc, char** argv) {
  int seconds = 3;
  int producers = 2;
  int consumers = 2;
  int payload_bytes = 64;
  int queues = 1;
  for (int i = 1; i + 1 < argc; i += 2) {
    std::string k = argv[i];
    int v = std::stoi(argv[i + 1]);
    if (k == "--seconds") seconds = v;
    if (k == "--producers") producers = v;
    if (k == "--consumers") consumers = v;
    if (k == "--payload_bytes") payload_bytes = v;
    if (k == "--queues") queues = v;
  }

  auto base = std::filesystem::temp_directory_path() / "pomaiqueue-stress";
  std::filesystem::remove_all(base);
  api::EngineOptions o;
  o.data_dir = base.string();
  o.max_queue_depth = 500000;
  api::PomaiQueue q(o);
  assert(q.Start().ok());
  for (int qi = 0; qi < queues; ++qi) {
    assert(q.CreateQueue("q" + std::to_string(qi)).ok());
  }

  uint64_t produced = 0;
  std::vector<double> consumer_lat;
  auto start = std::chrono::steady_clock::now();
  while (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start).count() < seconds) {
    for (int p = 0; p < producers; ++p) {
      for (int qi = 0; qi < queues; ++qi) {
        model::Message m;
        m.id.high = static_cast<uint64_t>(p + 1);
        m.id.low = ++produced;
        m.enqueue_ts = produced;
        m.payload.assign(payload_bytes, std::byte{0x7});
        m.routing_key = "stress";
        auto st = q.Produce("q" + std::to_string(qi), m);
        if (!st.ok()) break;
      }
    }
    for (int c = 0; c < consumers; ++c) {
      for (int qi = 0; qi < queues; ++qi) {
        auto s = std::chrono::steady_clock::now();
        auto con = q.Consume("q" + std::to_string(qi), "g");
        auto e = std::chrono::steady_clock::now();
        consumer_lat.push_back(std::chrono::duration<double, std::micro>(e - s).count());
        if (con.ok() && con.value().has_value()) {
          assert(q.Ack("q" + std::to_string(qi), "g", con.value()->message.id).ok());
        }
      }
    }
  }

  uint64_t acked = 0;
  for (int qi = 0; qi < queues; ++qi) {
    auto st = q.GetStats("q" + std::to_string(qi), "g");
    assert(st.ok());
    acked += st.value().acked_count;
  }
  std::sort(consumer_lat.begin(), consumer_lat.end());
  auto pct = [&](double p) {
    return consumer_lat.empty() ? 0.0 : consumer_lat[static_cast<size_t>(p * (consumer_lat.size() - 1))];
  };
  double sec = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
  std::cout << "{\"throughput_enqueue\":" << (produced / sec) << ",\"throughput_ack\":" << (acked / sec)
            << ",\"p50_us\":" << pct(0.50) << ",\"p95_us\":" << pct(0.95) << ",\"p99_us\":" << pct(0.99)
            << ",\"rss_bytes\":0}\n";
  q.Stop();
  if (acked > produced) return 2;
  return 0;
}
