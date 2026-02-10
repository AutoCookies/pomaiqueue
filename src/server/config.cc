#include "src/server/config.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <fstream>
#include <sstream>

#include "src/core/util/status.h"

namespace pomai::queue::server {
namespace {

std::string Trim(std::string value) {
  auto not_space = [](unsigned char c) { return !std::isspace(c); };
  value.erase(value.begin(), std::find_if(value.begin(), value.end(), not_space));
  value.erase(std::find_if(value.rbegin(), value.rend(), not_space).base(), value.end());
  return value;
}

util::StatusOr<uint64_t> ParseUint(const std::string& value, const std::string& key, uint64_t line_no) {
  try {
    size_t idx = 0;
    uint64_t parsed = std::stoull(value, &idx);
    if (idx != value.size()) {
      return util::Status(util::StatusCode::kInvalidArgument,
                          "invalid integer for key '" + key + "' at line " + std::to_string(line_no));
    }
    return parsed;
  } catch (...) {
    return util::Status(util::StatusCode::kInvalidArgument,
                        "invalid integer for key '" + key + "' at line " + std::to_string(line_no));
  }
}

util::StatusOr<std::chrono::milliseconds> ParseDurationMs(const std::string& value,
                                                          const std::string& key,
                                                          uint64_t line_no) {
  if (value.empty()) {
    return util::Status(util::StatusCode::kInvalidArgument,
                        "empty duration for key '" + key + "' at line " + std::to_string(line_no));
  }
  uint64_t multiplier = 1;
  std::string numeric = value;
  if (value.size() >= 2 && value.substr(value.size() - 2) == "ms") {
    numeric = value.substr(0, value.size() - 2);
    multiplier = 1;
  } else if (value.back() == 's') {
    numeric = value.substr(0, value.size() - 1);
    multiplier = 1000;
  } else if (value.back() == 'm') {
    numeric = value.substr(0, value.size() - 1);
    multiplier = 60 * 1000;
  } else if (value.back() == 'h') {
    numeric = value.substr(0, value.size() - 1);
    multiplier = 60 * 60 * 1000;
  }

  auto count_or = ParseUint(Trim(numeric), key, line_no);
  if (!count_or.ok()) {
    return count_or.status();
  }
  return std::chrono::milliseconds(count_or.value() * multiplier);
}

std::vector<std::string> SplitComma(const std::string& value) {
  std::vector<std::string> out;
  std::stringstream ss(value);
  std::string token;
  while (std::getline(ss, token, ',')) {
    token = Trim(token);
    if (!token.empty()) {
      out.push_back(token);
    }
  }
  return out;
}

}  // namespace

util::StatusOr<ServerConfig> LoadServerConfig(const std::filesystem::path& path) {
  std::ifstream in(path);
  if (!in.is_open()) {
    return util::Status(util::StatusCode::kNotFound, "cannot open config: " + path.string());
  }

  ServerConfig cfg;
  cfg.engine.data_dir = "/tmp/pomaiqueue";

  std::string line;
  uint64_t line_no = 0;
  while (std::getline(in, line)) {
    ++line_no;
    const auto hash_pos = line.find('#');
    if (hash_pos != std::string::npos) {
      line = line.substr(0, hash_pos);
    }
    line = Trim(line);
    if (line.empty()) {
      continue;
    }

    const auto eq = line.find('=');
    if (eq == std::string::npos) {
      return util::Status(util::StatusCode::kInvalidArgument,
                          "expected key=value at line " + std::to_string(line_no));
    }

    std::string key = Trim(line.substr(0, eq));
    std::string value = Trim(line.substr(eq + 1));
    if (key.empty()) {
      return util::Status(util::StatusCode::kInvalidArgument,
                          "empty key at line " + std::to_string(line_no));
    }

    if (key == "data_dir") {
      cfg.engine.data_dir = value;
    } else if (key == "shard_count") {
      auto v = ParseUint(value, key, line_no);
      if (!v.ok()) return v.status();
      cfg.engine.shard_count = static_cast<uint32_t>(v.value());
    } else if (key == "max_inflight") {
      auto v = ParseUint(value, key, line_no);
      if (!v.ok()) return v.status();
      cfg.engine.max_inflight = static_cast<uint32_t>(v.value());
    } else if (key == "max_queue_depth") {
      auto v = ParseUint(value, key, line_no);
      if (!v.ok()) return v.status();
      cfg.engine.max_queue_depth = static_cast<uint32_t>(v.value());
    } else if (key == "max_retry_count") {
      auto v = ParseUint(value, key, line_no);
      if (!v.ok()) return v.status();
      cfg.engine.max_retry_count = static_cast<uint32_t>(v.value());
    } else if (key == "max_segment_size_bytes") {
      auto v = ParseUint(value, key, line_no);
      if (!v.ok()) return v.status();
      cfg.engine.max_segment_size_bytes = v.value();
    } else if (key == "visibility_timeout") {
      auto v = ParseDurationMs(value, key, line_no);
      if (!v.ok()) return v.status();
      cfg.engine.visibility_timeout = v.value();
    } else if (key == "retry_backoff") {
      auto v = ParseDurationMs(value, key, line_no);
      if (!v.ok()) return v.status();
      cfg.engine.retry_backoff = v.value();
    } else if (key == "scheduler_tick") {
      auto v = ParseDurationMs(value, key, line_no);
      if (!v.ok()) return v.status();
      cfg.engine.scheduler_tick = v.value();
    } else if (key == "retention") {
      auto v = ParseDurationMs(value, key, line_no);
      if (!v.ok()) return v.status();
      cfg.engine.retention = v.value();
    } else if (key == "shutdown_grace") {
      auto v = ParseDurationMs(value, key, line_no);
      if (!v.ok()) return v.status();
      cfg.shutdown_grace = v.value();
    } else if (key == "fsync_policy") {
      if (value == "always") {
        cfg.engine.fsync_policy = util::FsyncPolicy::kAlways;
      } else if (value == "interval") {
        cfg.engine.fsync_policy = util::FsyncPolicy::kInterval;
      } else if (value == "never") {
        cfg.engine.fsync_policy = util::FsyncPolicy::kNever;
      } else {
        return util::Status(util::StatusCode::kInvalidArgument,
                            "invalid fsync_policy at line " + std::to_string(line_no));
      }
    } else if (key == "bootstrap_queues") {
      cfg.bootstrap_queues = SplitComma(value);
    } else {
      return util::Status(util::StatusCode::kInvalidArgument,
                          "unknown config key '" + key + "' at line " + std::to_string(line_no));
    }
  }

  if (cfg.engine.data_dir.empty()) {
    return util::Status(util::StatusCode::kInvalidArgument, "data_dir must not be empty");
  }
  return cfg;
}

}  // namespace pomai::queue::server
