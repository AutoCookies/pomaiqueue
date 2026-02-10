#include <cassert>
#include <cstddef>
#include <filesystem>
#include <fstream>

#include "src/core/storage/segment_log.h"

using pomai::queue::storage::Record;
using pomai::queue::storage::SegmentLog;
using pomai::queue::util::FsyncPolicy;

namespace {
Record MakeRecord(uint64_t id, size_t payload_bytes) {
  Record r;
  r.header.msg_id_high = 1;
  r.header.msg_id_low = id;
  r.header.enqueue_ts = id;
  r.routing_key = "rk";
  r.payload.assign(payload_bytes, std::byte{0x11});
  return r;
}
}

int main() {
  auto dir = std::filesystem::temp_directory_path() / "pomaiqueue-test-segment";
  std::filesystem::remove_all(dir);
  auto path = dir / "seg-000001.log";

  SegmentLog log(path, FsyncPolicy::kNever);
  assert(log.Open().ok());
  for (uint64_t i = 0; i < 3; ++i) {
    auto off = log.Append(MakeRecord(i + 1, 16));
    assert(off.ok());
    assert(off.value() == i);
  }
  assert(log.next_offset() == 3);

  {
    std::ofstream out(path, std::ios::binary | std::ios::app);
    out.put('\xAA');
  }

  SegmentLog recovered(path, FsyncPolicy::kNever);
  assert(recovered.Open().ok());
  assert(recovered.Recover().ok());
  assert(recovered.next_offset() == 3);
  assert(recovered.Offsets().size() == 3);

  auto third = recovered.Read(2);
  assert(third.ok());
  assert(third.value().payload.size() == 16);

  std::ofstream truncate(path, std::ios::binary | std::ios::in | std::ios::out);
  truncate.seekp(-5, std::ios::end);
  truncate.flush();

  SegmentLog recovered2(path, FsyncPolicy::kNever);
  assert(recovered2.Open().ok());
  assert(recovered2.Recover().ok());
  assert(recovered2.next_offset() <= 3);
  return 0;
}
