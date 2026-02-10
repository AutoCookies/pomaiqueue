#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

#include "src/core/api/pomai_queue.h"

namespace {

std::string ArgValue(int argc, char** argv, const std::string& key, const std::string& def = "") {
  for (int i = 0; i + 1 < argc; ++i) {
    if (std::string(argv[i]) == key) {
      return argv[i + 1];
    }
  }
  return def;
}

std::string StateName(pomai::queue::engine::MessageState s) {
  using S = pomai::queue::engine::MessageState;
  switch (s) {
    case S::kReady:
      return "READY";
    case S::kInFlight:
      return "IN_FLIGHT";
    case S::kAcked:
      return "ACKED";
    case S::kDead:
      return "DEAD";
  }
  return "UNKNOWN";
}

int Usage() {
  std::cerr << "usage: pomaiqctl <inspect|replay|bugreport> ...\n";
  return 1;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    return Usage();
  }

  pomai::queue::api::EngineOptions options;
  options.data_dir = ArgValue(argc, argv, std::string("--data_dir"), "/tmp/pomaiqueue");
  pomai::queue::api::PomaiQueue queue(options);
  auto st = queue.Start();
  if (!st.ok()) {
    std::cerr << "start failed: " << st.message() << "\n";
    return 2;
  }

  const std::string cmd = argv[1];
  if (cmd == "inspect") {
    const std::string queue_name = ArgValue(argc, argv, "--queue");
    const std::string group_id = ArgValue(argc, argv, "--group", "default");
    const uint64_t id_high = std::stoull(ArgValue(argc, argv, "--id_high", "1"));
    const uint64_t id_low = std::stoull(ArgValue(argc, argv, "--offset", ArgValue(argc, argv, "--id", "0")));
    pomai::queue::model::MessageId id{id_high, id_low};
    auto explain_or = queue.InspectMessage(queue_name, group_id, id);
    if (!explain_or.ok()) {
      std::cerr << "inspect failed: " << explain_or.status().message() << "\n";
      return 3;
    }
    const auto& x = explain_or.value();
    std::cout << "{\n"
              << "  \"state\": \"" << StateName(x.state) << "\",\n"
              << "  \"sequence\": " << x.sequence << ",\n"
              << "  \"enqueue_time_ms\": " << x.enqueue_time_ms << ",\n"
              << "  \"last_transition_time_ms\": " << x.last_transition_time_ms << ",\n"
              << "  \"retry_count\": " << x.retry_count << ",\n"
              << "  \"next_visible_at_ms\": " << x.next_visible_at_ms << ",\n"
              << "  \"last_transition_reason\": \"" << x.last_transition_reason << "\",\n"
              << "  \"lease_owner\": \"" << x.lease_owner << "\",\n"
              << "  \"lease_token\": \"" << x.lease_token << "\"\n"
              << "}\n";
  } else if (cmd == "replay") {
    const std::string queue_name = ArgValue(argc, argv, "--queue");
    const std::string group_id = ArgValue(argc, argv, "--group", "default");
    const uint64_t until_seq = std::stoull(ArgValue(argc, argv, "--until_seq"));
    auto replay_or = queue.ReplayToSequence(queue_name, group_id, until_seq);
    if (!replay_or.ok()) {
      std::cerr << "replay failed: " << replay_or.status().message() << "\n";
      return 4;
    }
    const auto& r = replay_or.value();
    std::cout << "{\n"
              << "  \"applied_sequence\": " << r.applied_sequence << ",\n"
              << "  \"ready\": " << r.stats.ready_count << ",\n"
              << "  \"inflight\": " << r.stats.inflight_count << ",\n"
              << "  \"acked\": " << r.stats.acked_count << ",\n"
              << "  \"dead\": " << r.stats.dead_count << "\n"
              << "}\n";
  } else if (cmd == "bugreport") {
    const std::string out = ArgValue(argc, argv, "--out", "/tmp/pomaiqueue-bugreport.txt");
    std::ofstream bundle(out, std::ios::trunc);
    if (!bundle.is_open()) {
      std::cerr << "bugreport failed: cannot open output\n";
      return 6;
    }
    bundle << "pomaiqueue bugreport\n";
    bundle << "queue=" << ArgValue(argc, argv, "--queue") << "\n";
    bundle << "group=" << ArgValue(argc, argv, "--group", "default") << "\n";
    bundle << "from_seq=" << ArgValue(argc, argv, "--from-seq", "0") << "\n";
    bundle << "to_seq=" << ArgValue(argc, argv, "--to-seq", "0") << "\n";
    bundle << "data_dir=" << options.data_dir << "\n";
    bundle.close();
    std::cout << "wrote " << out << "\n";
  } else {
    return Usage();
  }

  auto stop = queue.Stop();
  if (!stop.ok()) {
    std::cerr << "stop failed: " << stop.message() << "\n";
    return 5;
  }
  return 0;
}
