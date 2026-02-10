#pragma once

#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <filesystem>
#include <map>
#include <set>
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
  uint64_t sequence = 0;
  uint64_t enqueue_time_ms = 0;
  uint64_t last_transition_time_ms = 0;
  uint32_t retry_count = 0;
  uint64_t next_visible_at_ms = 0;
  std::string last_transition_reason;
  std::string lease_owner;
  std::string lease_token;
};

enum class TransitionReason {
  kProduced,
  kDelivered,
  kAcked,
  kConsumerNackRequeued,
  kConsumerNackDead,
  kVisibilityTimeoutRequeued,
  kVisibilityTimeoutDead,
  kRecoveredInflightToReady,
};

struct ReplayResult {
  uint64_t applied_sequence = 0;
  QueueStats stats;
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
  std::chrono::milliseconds scheduler_tick{100};
  std::chrono::milliseconds retention{std::chrono::hours(24)};
  uint64_t max_segment_size_bytes = 64 * 1024 * 1024;
  std::function<uint64_t()> now_fn;
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
  util::StatusOr<ReplayResult> ReplayToSequence(const std::string& queue_name,
                                                const std::string& group_id,
                                                uint64_t until_sequence);

 private:
  struct InflightEntry {
    uint64_t offset = 0;
    uint64_t deadline_ms = 0;
    uint64_t bucket_tick = 0;
    std::string owner;
    std::string lease_token;
  };

  struct MessageRuntime {
    MessageState state = MessageState::kReady;
    uint64_t offset = 0;
    uint32_t retry_count = 0;
    uint64_t enqueue_ts = 0;
    uint64_t last_transition_ts = 0;
    uint64_t available_after_ms = 0;
    uint64_t sequence = 0;
    std::string last_transition_reason;
  };

  struct TransitionEvent {
    uint64_t sequence = 0;
    model::MessageId id;
    MessageState state = MessageState::kReady;
    uint32_t retry_count = 0;
    uint64_t available_after_ms = 0;
    uint64_t transition_ts = 0;
    uint64_t deadline_ms = 0;
    std::string owner;
    std::string lease_token;
    TransitionReason reason = TransitionReason::kProduced;
  };

  struct ConsumerGroupState {
    std::deque<uint64_t> ready_offsets;
    std::unordered_map<model::MessageId, InflightEntry, model::MessageIdHash> inflight;
    std::unordered_map<model::MessageId, MessageRuntime, model::MessageIdHash> runtime;
    std::vector<std::vector<model::MessageId>> deadline_wheel;
    uint64_t wheel_base_tick = 0;
    uint64_t next_sequence = 1;
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
    uint64_t last_transition_ts = 0;
    uint64_t available_after_ms = 0;
    uint64_t sequence = 0;
    std::string reason;
  };

  size_t ShardForQueue(const std::string& queue_name) const;

  static uint64_t NowMs();
  uint64_t CurrentTimeMs() const;
  static std::filesystem::path GroupStatePath(const std::string& data_dir,
                                              const std::string& queue_name,
                                              const std::string& group_id);
  static std::filesystem::path GroupEventPath(const std::string& data_dir,
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
  util::Status AppendTransition(const std::string& queue_name,
                                const std::string& group_id,
                                ConsumerGroupState& group,
                                const TransitionEvent& event);
  util::StatusOr<std::vector<TransitionEvent>> LoadTransitions(const std::string& queue_name,
                                                               const std::string& group_id,
                                                               uint64_t max_sequence);
  void ApplyTransition(ConsumerGroupState& group, const TransitionEvent& event);
  void InitScheduler(ConsumerGroupState& group);
  void ScheduleInflight(ConsumerGroupState& group,
                        const model::MessageId& id,
                        uint64_t deadline_ms,
                        const std::string& owner,
                        const std::string& lease_token,
                        uint64_t offset);
  void ReapExpiredInflight(const std::string& queue_name,
                          const std::string& group_id,
                          ConsumerGroupState& group);
  void BuildReadyQueue(ConsumerGroupState& group);
  static std::string ReasonToString(TransitionReason reason);

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
  util::StatusOr<ReplayResult> ReplayToSequenceOnShard(const std::string& queue_name,
                                                       const std::string& group_id,
                                                       uint64_t until_sequence);

  EngineOptions options_;
  std::vector<std::unique_ptr<ShardActor>> shards_;
  std::unordered_map<std::string, std::unique_ptr<QueueState>> queues_;
  std::mutex queues_mutex_;
};

}  // namespace pomai::queue::engine
