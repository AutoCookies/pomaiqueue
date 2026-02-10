#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <string>
#include <thread>

#include "src/core/api/pomai_queue.h"
#include "src/core/util/status.h"
#include "src/server/config.h"

namespace {

std::atomic<bool> g_shutdown{false};

void HandleSignal(int signal) {
  (void)signal;
  g_shutdown.store(true);
}

std::string ArgValue(int argc, char** argv, const std::string& key) {
  for (int i = 1; i + 1 < argc; ++i) {
    if (std::string(argv[i]) == key) {
      return argv[i + 1];
    }
  }
  return "";
}

}  // namespace

int main(int argc, char** argv) {
  const std::string config_path = ArgValue(argc, argv, "--config");
  if (config_path.empty()) {
    std::cerr << "usage: pomaiqueue_server --config <path>\n";
    return 2;
  }

  auto cfg_or = pomai::queue::server::LoadServerConfig(config_path);
  if (!cfg_or.ok()) {
    std::cerr << "[startup] config error: " << cfg_or.status().message() << "\n";
    return 3;
  }
  auto cfg = std::move(cfg_or.value());

  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  std::cout << "[startup] pomaiqueue_server starting\n";
  std::cout << "[startup] data_dir=" << cfg.engine.data_dir
            << " shards=" << cfg.engine.shard_count
            << " max_inflight=" << cfg.engine.max_inflight << "\n";

  pomai::queue::api::PomaiQueue queue(cfg.engine);
  auto start = queue.Start();
  if (!start.ok()) {
    std::cerr << "[startup] engine start failed: " << start.message() << "\n";
    return 4;
  }

  for (const auto& q : cfg.bootstrap_queues) {
    auto create = queue.CreateQueue(q);
    if (!create.ok() && create.code() != pomai::queue::util::StatusCode::kAlreadyExists) {
      std::cerr << "[startup] bootstrap queue failed queue=" << q << " err=" << create.message() << "\n";
      queue.Stop();
      return 5;
    }
    std::cout << "[startup] queue ready: " << q << "\n";
  }

  std::cout << "[ready] server loop active; waiting for SIGINT/SIGTERM\n";
  while (!g_shutdown.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  std::cout << "[shutdown] signal received, stopping engine\n";
  auto stop = queue.Stop();
  if (!stop.ok()) {
    std::cerr << "[shutdown] engine stop failed: " << stop.message() << "\n";
    return 6;
  }

  std::cout << "[shutdown] complete\n";
  return 0;
}
