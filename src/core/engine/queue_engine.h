#pragma once

#include <chrono>
#include <cstdint>
#include <deque>
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

enum class MessageState {
  kReady,
  kInFlight,
  kAcked,
  kDead,
};

struct ConsumeResult {
  model::Message message;
  uint64_t offset = 0;
  uint32_t delivery_count = 0;
};

struct QueueStats {
  uint64_t ready_count = 0;
  uint64_t inflight_count = 0;
  uint64_t acked_count = 0;
  uint64_t dead_count = 0;
  uint64_t oldest_ready_age_ms = 0;
};

struct MessageDebugView {
  MessageState state = MessageState::kReady;
  uint32_t retry_count = 0;
  std::string last_transition_reason;
};

struct EngineOptions {
  std::string data_dir;
  uint32_t shard_count = 1;
  util::FsyncPolicy fsync_policy = util::FsyncPolicy::kAlways;
  uint32_t max_inflight = 100;
  uint32_t max_queue_depth = 10000;
  uint32_t max_retry_count = 5;
  std::chrono::milliseconds visibility_timeout{30000};
  std::chrono::milliseconds retry_backoff{1000};
  std::chrono::milliseconds retention{std::chrono::hours(24)};
  uint64_t max_segment_size_bytes = 64 * 1024 * 1024;
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
  util::StatusOr<QueueStats> GetStats(const std::string& queue_name, const std::string& group_id);
  util::StatusOr<MessageDebugView> InspectMessage(const std::string& queue_name,
                                                  const std::string& group_id,
                                                  const model::MessageId& id);

 private:
  struct InflightEntry {
    uint64_t offset = 0;
    uint64_t available_after_ms = 0;
    uint64_t deadline_ms = 0;
  };

  struct MessageRuntime {
    MessageState state = MessageState::kReady;
    uint64_t offset = 0;
    uint32_t retry_count = 0;
    uint64_t enqueue_ts = 0;
    uint64_t available_after_ms = 0;
    std::string last_transition_reason;
  };

  struct ConsumerGroupState {
    std::deque<uint64_t> ready_offsets;
    std::unordered_map<model::MessageId, InflightEntry, model::MessageIdHash> inflight;
    std::unordered_map<model::MessageId, MessageRuntime, model::MessageIdHash> runtime;
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

  struct PersistedMessage {
    uint64_t id_high = 0;
    uint64_t id_low = 0;
    uint64_t offset = 0;
    uint32_t state = 0;
    uint32_t retry_count = 0;
    uint64_t enqueue_ts = 0;
    uint64_t available_after_ms = 0;
    std::string reason;
  };

  size_t ShardForQueue(const std::string& queue_name) const;

  static uint64_t NowMs();
  static std::filesystem::path GroupStatePath(const std::string& data_dir,
                                              const std::string& queue_name,
                                              const std::string& group_id);

  util::StatusOr<QueueState*> GetQueue(const std::string& queue_name);
  util::StatusOr<ConsumerGroupState*> GetGroup(const std::string& queue_name,
                                               QueueState& queue_state,
                                               const std::string& group_id);

  util::Status LoadGroupState(const std::string& queue_name,
                             const std::string& group_id,
                             QueueState& queue_state,
                             ConsumerGroupState& group);
  util::Status PersistGroupState(const std::string& queue_name,
                                 const std::string& group_id,
                                 const ConsumerGroupState& group);
  void ReapExpiredInflight(ConsumerGroupState& group);
  void BuildReadyQueue(ConsumerGroupState& group);

  util::Status CreateQueueOnShard(const std::string& queue_name);
  util::StatusOr<uint64_t> ProduceOnShard(const std::string& queue_name, const model::Message& message);
  util::StatusOr<std::optional<ConsumeResult>> ConsumeOnShard(const std::string& queue_name,
                                                              const std::string& group_id);
  util::Status AckOnShard(const std::string& queue_name, const std::string& group_id, const model::MessageId& id);
  util::Status NackOnShard(const std::string& queue_name,
                           const std::string& group_id,
                           const model::MessageId& id,
                           bool requeue);
  util::StatusOr<QueueStats> GetStatsOnShard(const std::string& queue_name, const std::string& group_id);
  util::StatusOr<MessageDebugView> InspectMessageOnShard(const std::string& queue_name,
                                                         const std::string& group_id,
                                                         const model::MessageId& id);

  EngineOptions options_;
  std::vector<std::unique_ptr<ShardActor>> shards_;
  std::unordered_map<std::string, std::unique_ptr<QueueState>> queues_;
  std::mutex queues_mutex_;
};

}  // namespace pomai::queue::engine
