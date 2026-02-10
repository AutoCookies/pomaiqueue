#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <random>
#include <string>
#include <vector>

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

void SetFailpoint(const char* name) {
  if (name == nullptr) {
    unsetenv("POMAIQUEUE_FAILPOINT");
    return;
  }
  setenv("POMAIQUEUE_FAILPOINT", name, 1);
}

void Scenario(const std::filesystem::path& base, const char* failpoint, int seed) {
  std::mt19937_64 rng(seed);
  api::EngineOptions options;
  options.data_dir = base.string();
  options.max_retry_count = 1;
  options.visibility_timeout = std::chrono::milliseconds(1);
  options.retry_backoff = std::chrono::milliseconds(0);

  SetFailpoint(failpoint);
  {
    api::PomaiQueue q(options);
    assert(q.Start().ok());
    auto c = q.CreateQueue("q");
    assert(c.ok() || c.code() == util::StatusCode::kAlreadyExists);
    for (int i = 0; i < 10; ++i) {
      const uint64_t id = static_cast<uint64_t>(seed * 100 + i + 1);
      auto p = q.Produce("q", Msg(id, id));
      assert(p.ok());
      auto take = q.Consume("q", "g");
      if (take.ok() && take.value().has_value()) {
        if ((rng() % 3) == 0) {
          q.Ack("q", "g", take.value()->message.id);
        } else {
          q.Nack("q", "g", take.value()->message.id, (rng() % 2) == 0);
        }
      }
    }
    q.Stop();
  }

  SetFailpoint(nullptr);
  {
    api::PomaiQueue q(options);
    assert(q.Start().ok());
    auto c = q.CreateQueue("q");
    assert(c.ok() || c.code() == util::StatusCode::kAlreadyExists);
    auto st = q.GetStats("q", "g");
    assert(st.ok());
    const auto total = st.value().ready_count + st.value().inflight_count + st.value().acked_count + st.value().dead_count;
    assert(total <= 10U * static_cast<uint64_t>(seed));
    // Ensure no permanently stuck inflight after restart contract.
    assert(st.value().inflight_count == 0);
    q.Stop();
  }
}
}  // namespace

int main(int argc, char** argv) {
  int iterations = 5;
  if (argc > 1) {
    iterations = std::stoi(argv[1]);
  }
  auto base = std::filesystem::temp_directory_path() / "pomaiqueue-crash-oracle";
  std::filesystem::remove_all(base);

  const std::vector<std::string> failpoints = {
      "before_log_append",           // enqueue transition write path
      "before_lease_issue",          // lease issuance
      "before_ack_transition",       // ack
      "before_timeout_transition",   // timeout processing
      "before_dlq_move",             // DLQ move
      "before_checkpoint_rename",    // checkpoint atomic replace
  };

  for (int i = 1; i <= iterations; ++i) {
    for (const auto& fp : failpoints) {
      Scenario(base / (fp + std::to_string(i)), fp.c_str(), i);
    }
  }
  return 0;
}
