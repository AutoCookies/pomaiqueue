#include <cassert>
#include <filesystem>
#include <fstream>

#include "src/core/durable_io.h"

int main() {
  const std::filesystem::path dir = std::filesystem::temp_directory_path() / "pomaiqueue_durable_io_test";
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);

  const auto tmp = dir / "state.bin.tmp";
  const auto final = dir / "state.bin";

  {
    std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
    out << "durable-state-v1";
  }

  auto status = pomai::queue::core::DurableRename(tmp, final, pomai::queue::util::FsyncPolicy::kAlways);
  assert(status.ok());
  assert(std::filesystem::exists(final));
  assert(!std::filesystem::exists(tmp));

  std::ifstream in(final, std::ios::binary);
  std::string payload;
  std::getline(in, payload);
  assert(payload == "durable-state-v1");

  std::filesystem::remove_all(dir);
  return 0;
}
