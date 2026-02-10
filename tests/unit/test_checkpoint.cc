#include <cassert>
#include <cstdint>
#include <filesystem>
#include <fstream>

#include "src/core/storage/checkpoint.h"

using pomai::queue::storage::CheckpointStore;
using pomai::queue::util::FsyncPolicy;

int main() {
  auto dir = std::filesystem::temp_directory_path() / "pomaiqueue-test-checkpoint";
  std::filesystem::remove_all(dir);
  auto path = dir / "g.checkpoint";

  CheckpointStore store(path, FsyncPolicy::kNever);
  assert(store.Save(123).ok());
  auto loaded = store.Load();
  assert(loaded.ok());
  assert(loaded.value() == 123);

  {
    std::fstream io(path, std::ios::binary | std::ios::in | std::ios::out);
    io.seekp(2);
    io.put('\xFF');
  }
  auto corrupted = store.Load();
  assert(!corrupted.ok());
  return 0;
}
