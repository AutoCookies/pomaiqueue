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
    std::cerr << "usage: pomaiqueue-server --config <path>\n";
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
  }

  while (!g_shutdown.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  auto stop = queue.Stop();
  if (!stop.ok()) {
    std::cerr << "[shutdown] engine stop failed: " << stop.message() << "\n";
    return 6;
  }

  return 0;
}
