#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <mutex>
#include <vector>

#include "src/core/engine/shard_actor.h"
#include "src/core/model/message.h"
#include "src/core/storage/checkpoint.h"
#include "src/core/storage/segment_log.h"
#include "src/core/util/status.h"
#include "src/core/util/status_or.h"

namespace pomai::queue::engine {

struct ConsumeResult {
  model::Message message;
  uint64_t offset = 0;
  uint32_t delivery_count = 0;
};

struct EngineOptions {
  std::string data_dir;
  uint32_t shard_count = 1;
  util::FsyncPolicy fsync_policy = util::FsyncPolicy::kAlways;
  uint32_t max_inflight = 100;
  std::chrono::milliseconds visibility_timeout{30000};
};

class QueueEngine {
 public:
  explicit QueueEngine(EngineOptions options);
  ~QueueEngine();

  util::Status Start();
  util::Status Stop();

  util::Status CreateQueue(const std::string& queue_name);
  util::StatusOr<uint64_t> Produce(const std::string& queue_name, const model::Message& message);
  util::StatusOr<std::optional<ConsumeResult>> Consume(const std::string& queue_name,
                                                      const std::string& group_id);
  util::Status Ack(const std::string& queue_name, const std::string& group_id, const model::MessageId& id);
  util::Status Nack(const std::string& queue_name,
                    const std::string& group_id,
                    const model::MessageId& id,
                    bool requeue);

 private:
  struct InflightEntry {
    uint64_t offset = 0;
    std::chrono::steady_clock::time_point deadline;
    uint32_t delivery_count = 0;
  };

  struct ConsumerGroupState {
    uint64_t committed_offset = 0;
    std::unordered_map<model::MessageId, InflightEntry, model::MessageIdHash> inflight;
    storage::CheckpointStore checkpoint_store;

    explicit ConsumerGroupState(storage::CheckpointStore store)
        : checkpoint_store(std::move(store)) {}
  };

  struct QueueState {
    storage::SegmentLog segment;
    std::unordered_map<std::string, std::unique_ptr<ConsumerGroupState>> groups;

    explicit QueueState(storage::SegmentLog segment_log)
        : segment(std::move(segment_log)) {}
  };

  size_t ShardForQueue(const std::string& queue_name) const;

  util::StatusOr<QueueState*> GetQueue(const std::string& queue_name);
  util::StatusOr<ConsumerGroupState*> GetGroup(QueueState& queue_state, const std::string& group_id);

  util::Status CreateQueueOnShard(const std::string& queue_name);
  util::StatusOr<uint64_t> ProduceOnShard(const std::string& queue_name, const model::Message& message);
  util::StatusOr<std::optional<ConsumeResult>> ConsumeOnShard(const std::string& queue_name,
                                                              const std::string& group_id);
  util::Status AckOnShard(const std::string& queue_name, const std::string& group_id, const model::MessageId& id);
  util::Status NackOnShard(const std::string& queue_name,
                           const std::string& group_id,
                           const model::MessageId& id,
                           bool requeue);

  EngineOptions options_;
  std::vector<std::unique_ptr<ShardActor>> shards_;
  std::unordered_map<std::string, std::unique_ptr<QueueState>> queues_;
  std::mutex queues_mutex_;
};

}  // namespace pomai::queue::engine
