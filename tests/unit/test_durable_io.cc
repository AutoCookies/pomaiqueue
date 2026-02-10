#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <fstream>

#include "src/core/durable_io.h"

namespace {

void SetFailpoint(const char* value) {
  if (value == nullptr) {
    unsetenv("POMAIQUEUE_FAILPOINT");
    return;
  }
  setenv("POMAIQUEUE_FAILPOINT", value, 1);
}

}  // namespace

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

  const auto write_final = dir / "write.bin";
  status = pomai::queue::core::DurableWriteFile(
      write_final,
      [](std::ofstream& out) {
        out << "durable-write-v1";
        if (!out) {
          return pomai::queue::util::Status(pomai::queue::util::StatusCode::kIOError, "write failed");
        }
        return pomai::queue::util::Status::Ok();
      },
      pomai::queue::util::FsyncPolicy::kAlways);
  assert(status.ok());
  assert(std::filesystem::exists(write_final));

  SetFailpoint("fsync_file");
  {
    std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
    out << "should-not-rename";
  }
  status = pomai::queue::core::DurableRename(tmp, final, pomai::queue::util::FsyncPolicy::kAlways);
  assert(!status.ok());
  assert(std::filesystem::exists(tmp));
  assert(std::filesystem::exists(final));

  SetFailpoint("fsync_dir");
  status = pomai::queue::core::DurableWriteFile(
      write_final,
      [](std::ofstream& out) {
        out << "dir-fsync-fail";
        if (!out) {
          return pomai::queue::util::Status(pomai::queue::util::StatusCode::kIOError, "write failed");
        }
        return pomai::queue::util::Status::Ok();
      },
      pomai::queue::util::FsyncPolicy::kAlways);
  assert(!status.ok());
  assert(std::filesystem::exists(write_final));

  SetFailpoint(nullptr);
  std::filesystem::remove_all(dir);
  return 0;
}
