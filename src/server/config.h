#pragma once

#include <chrono>
#include <filesystem>
#include <string>
#include <vector>

#include "src/core/api/pomai_queue.h"
#include "src/core/util/status_or.h"

namespace pomai::queue::server {

struct ServerConfig {
  api::EngineOptions engine;
  std::vector<std::string> bootstrap_queues;
  std::chrono::milliseconds shutdown_grace{5000};
};

util::StatusOr<ServerConfig> LoadServerConfig(const std::filesystem::path& path);

}  // namespace pomai::queue::server
