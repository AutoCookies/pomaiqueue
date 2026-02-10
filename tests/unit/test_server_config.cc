#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

#include "src/server/config.h"

namespace {

std::filesystem::path TempPath(const std::string& name) {
  return std::filesystem::temp_directory_path() / name;
}

void WriteFile(const std::filesystem::path& path, const std::string& body) {
  std::ofstream out(path);
  out << body;
}

bool Expect(bool condition, const std::string& message) {
  if (!condition) {
    std::cerr << "assertion failed: " << message << "\n";
    return false;
  }
  return true;
}

}  // namespace

int main() {
  const auto ok_path = TempPath("pomaiqueue_server_config_ok.cfg");
  WriteFile(ok_path,
            "data_dir=/tmp/pomaiqueue-test\n"
            "shard_count=2\n"
            "max_inflight=32\n"
            "max_queue_depth=999\n"
            "max_retry_count=4\n"
            "max_segment_size_bytes=2048\n"
            "visibility_timeout=3s\n"
            "retry_backoff=1500ms\n"
            "scheduler_tick=50ms\n"
            "retention=2h\n"
            "fsync_policy=interval\n"
            "bootstrap_queues=jobs,dlq\n"
            "shutdown_grace=7s\n");

  auto cfg_or = pomai::queue::server::LoadServerConfig(ok_path);
  if (!Expect(cfg_or.ok(), "valid config should parse")) {
    return 1;
  }
  const auto& cfg = cfg_or.value();
  if (!Expect(cfg.engine.shard_count == 2, "shard_count")) return 1;
  if (!Expect(cfg.bootstrap_queues.size() == 2, "bootstrap queue size")) return 1;
  if (!Expect(cfg.engine.visibility_timeout.count() == 3000, "visibility timeout")) return 1;
  if (!Expect(cfg.shutdown_grace.count() == 7000, "shutdown grace")) return 1;

  const auto bad_path = TempPath("pomaiqueue_server_config_bad.cfg");
  WriteFile(bad_path, "unknown_key=123\n");
  auto bad_or = pomai::queue::server::LoadServerConfig(bad_path);
  if (!Expect(!bad_or.ok(), "unknown key should fail")) {
    return 1;
  }

  std::filesystem::remove(ok_path);
  std::filesystem::remove(bad_path);
  return 0;
}
