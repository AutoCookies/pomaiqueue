#include "src/core/engine/queue_engine.h"

#include "src/core/durable_io.h"

#include <algorithm>
#include <filesystem>
#include <fstream>

#include "src/core/util/failpoint.h"

namespace pomai::queue::engine {

namespace {

constexpr uint32_t kStateReady = 0;
constexpr uint32_t kStateInFlight = 1;
constexpr uint32_t kStateAcked = 2;
constexpr uint32_t kStateDead = 3;

uint32_t EncodeState(MessageState state) {
  switch (state) {
    case MessageState::kReady:
      return kStateReady;
    case MessageState::kInFlight:
      return kStateInFlight;
    case MessageState::kAcked:
      return kStateAcked;
    case MessageState::kDead:
      return kStateDead;
  }
  return kStateDead;
}

MessageState DecodeState(uint32_t state) {
  switch (state) {
    case kStateReady:
      return MessageState::kReady;
    case kStateInFlight:
      return MessageState::kInFlight;
    case kStateAcked:
      return MessageState::kAcked;
    default:
      return MessageState::kDead;
  }
}

uint32_t EncodeReason(TransitionReason reason) {
  return static_cast<uint32_t>(reason);
}

TransitionReason DecodeReason(uint32_t code) {
  return static_cast<TransitionReason>(code);
}

}  // namespace

QueueEngine::QueueEngine(EngineOptions options) : options_(std::move(options)) {
  if (!options_.now_fn) {
    options_.now_fn = []() { return QueueEngine::NowMs(); };
  }
}

QueueEngine::~QueueEngine() {
  Stop();
}

uint64_t QueueEngine::NowMs() {
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count());
}

uint64_t QueueEngine::CurrentTimeMs() const {
  return options_.now_fn();
}

std::filesystem::path QueueEngine::GroupStatePath(const std::string& data_dir,
                                                  const std::string& queue_name,
                                                  const std::string& group_id) {
  return std::filesystem::path(data_dir) / queue_name / (group_id + ".state");
}

std::filesystem::path QueueEngine::GroupEventPath(const std::string& data_dir,
                                                  const std::string& queue_name,
                                                  const std::string& group_id) {
  return std::filesystem::path(data_dir) / queue_name / (group_id + ".events");
}

util::Status QueueEngine::Start() {
  if (options_.shard_count == 0) {
    return util::Status(util::StatusCode::kInvalidArgument, "shard_count must be >= 1");
  }
  shards_.clear();
  shards_.reserve(options_.shard_count);
  for (uint32_t i = 0; i < options_.shard_count; ++i) {
    auto shard = std::make_unique<ShardActor>();
    shard->Start();
    shards_.push_back(std::move(shard));
  }
  return util::Status::Ok();
}

util::Status QueueEngine::Stop() {
  for (auto& shard : shards_) {
    shard->Stop();
  }
  shards_.clear();
  return util::Status::Ok();
}

util::Status QueueEngine::ValidateShardConfig() const {
  if (options_.shard_count == 0) {
    return util::Status(util::StatusCode::kInvalidArgument, "shard_count must be >= 1");
  }
  if (shards_.empty()) {
    return util::Status(util::StatusCode::kUnavailable, "engine not started");
  }
  return util::Status::Ok();
}

size_t QueueEngine::ShardForQueue(const std::string& queue_name) const {
  return std::hash<std::string>{}(queue_name) % options_.shard_count;
}

util::Status QueueEngine::CreateQueue(const std::string& queue_name) {
  auto valid = ValidateShardConfig();
  if (!valid.ok()) {
    return valid;
  }
  auto shard_id = ShardForQueue(queue_name);
  return shards_[shard_id]->Submit([this, queue_name]() { return CreateQueueOnShard(queue_name); });
}

util::StatusOr<uint64_t> QueueEngine::Produce(const std::string& queue_name, const model::Message& message) {
  auto valid = ValidateShardConfig();
  if (!valid.ok()) {
    return valid;
  }
  auto shard_id = ShardForQueue(queue_name);
  return shards_[shard_id]->SubmitValue<uint64_t>(
      [this, queue_name, message]() { return ProduceOnShard(queue_name, message); });
}

util::StatusOr<std::optional<ConsumeResult>> QueueEngine::Consume(const std::string& queue_name,
                                                                  const std::string& group_id) {
  auto valid = ValidateShardConfig();
  if (!valid.ok()) {
    return valid;
  }
  auto shard_id = ShardForQueue(queue_name);
  return shards_[shard_id]->SubmitValue<std::optional<ConsumeResult>>(
      [this, queue_name, group_id]() { return ConsumeOnShard(queue_name, group_id); });
}

util::Status QueueEngine::Ack(const std::string& queue_name,
                              const std::string& group_id,
                              const model::MessageId& id) {
  auto valid = ValidateShardConfig();
  if (!valid.ok()) {
    return valid;
  }
  auto shard_id = ShardForQueue(queue_name);
  return shards_[shard_id]->Submit([this, queue_name, group_id, id]() { return AckOnShard(queue_name, group_id, id); });
}

util::Status QueueEngine::Nack(const std::string& queue_name,
                               const std::string& group_id,
                               const model::MessageId& id,
                               bool requeue) {
  auto valid = ValidateShardConfig();
  if (!valid.ok()) {
    return valid;
  }
  auto shard_id = ShardForQueue(queue_name);
  return shards_[shard_id]->Submit(
      [this, queue_name, group_id, id, requeue]() { return NackOnShard(queue_name, group_id, id, requeue); });
}

util::StatusOr<QueueStats> QueueEngine::GetStats(const std::string& queue_name, const std::string& group_id) {
  auto valid = ValidateShardConfig();
  if (!valid.ok()) {
    return valid;
  }
  auto shard_id = ShardForQueue(queue_name);
  return shards_[shard_id]->SubmitValue<QueueStats>(
      [this, queue_name, group_id]() { return GetStatsOnShard(queue_name, group_id); });
}

util::StatusOr<MessageDebugView> QueueEngine::InspectMessage(const std::string& queue_name,
                                                             const std::string& group_id,
                                                             const model::MessageId& id) {
  auto valid = ValidateShardConfig();
  if (!valid.ok()) {
    return valid;
  }
  auto shard_id = ShardForQueue(queue_name);
  return shards_[shard_id]->SubmitValue<MessageDebugView>(
      [this, queue_name, group_id, id]() { return InspectMessageOnShard(queue_name, group_id, id); });
}

util::StatusOr<ReplayResult> QueueEngine::ReplayToSequence(const std::string& queue_name,
                                                           const std::string& group_id,
                                                           uint64_t until_sequence) {
  auto valid = ValidateShardConfig();
  if (!valid.ok()) {
    return valid;
  }
  auto shard_id = ShardForQueue(queue_name);
  return shards_[shard_id]->SubmitValue<ReplayResult>(
      [this, queue_name, group_id, until_sequence]() {
        return ReplayToSequenceOnShard(queue_name, group_id, until_sequence);
      });
}

util::StatusOr<QueueEngine::QueueState*> QueueEngine::GetQueue(const std::string& queue_name) {
  std::lock_guard<std::mutex> lock(queues_mutex_);
  auto it = queues_.find(queue_name);
  if (it == queues_.end()) {
    return util::Status(util::StatusCode::kNotFound, "queue not found");
  }
  return it->second.get();
}

void QueueEngine::InitScheduler(ConsumerGroupState& group) {
  const uint64_t tick_ms = std::max<uint64_t>(1, options_.scheduler_tick.count());
  group.deadline_wheel.assign(512, {});
  group.wheel_base_tick = CurrentTimeMs() / tick_ms;
}

void QueueEngine::ScheduleInflight(ConsumerGroupState& group,
                                   const model::MessageId& id,
                                   uint64_t deadline_ms,
                                   const std::string& owner,
                                   const std::string& lease_token,
                                   uint64_t offset) {
  const uint64_t tick_ms = std::max<uint64_t>(1, options_.scheduler_tick.count());
  if (group.deadline_wheel.empty()) {
    InitScheduler(group);
  }
  const uint64_t tick = deadline_ms / tick_ms;
  const size_t slot = tick % group.deadline_wheel.size();
  group.deadline_wheel[slot].push_back(id);

  InflightEntry entry;
  entry.offset = offset;
  entry.deadline_ms = deadline_ms;
  entry.bucket_tick = tick;
  entry.owner = owner;
  entry.lease_token = lease_token;
  group.inflight[id] = std::move(entry);
}

std::string QueueEngine::ReasonToString(TransitionReason reason) {
  switch (reason) {
    case TransitionReason::kProduced:
      return "produced";
    case TransitionReason::kDelivered:
      return "delivered-to-consumer";
    case TransitionReason::kAcked:
      return "acked-by-consumer";
    case TransitionReason::kConsumerNackRequeued:
      return "consumer-nack-requeued";
    case TransitionReason::kConsumerNackDead:
      return "consumer-nack-dead-lettered";
    case TransitionReason::kVisibilityTimeoutRequeued:
      return "visibility-timeout-requeued";
    case TransitionReason::kVisibilityTimeoutDead:
      return "visibility-timeout-retry-exhausted";
    case TransitionReason::kRecoveredInflightToReady:
      return "recovered-inflight-to-ready";
  }
  return "unknown";
}

util::StatusOr<std::vector<QueueEngine::TransitionEvent>> QueueEngine::LoadTransitions(
    const std::string& queue_name,
    const std::string& group_id,
    uint64_t max_sequence) {
  std::vector<TransitionEvent> events;
  auto path = GroupEventPath(options_.data_dir, queue_name, group_id);
  if (!std::filesystem::exists(path)) {
    return events;
  }

  std::ifstream in(path, std::ios::binary);
  if (!in.is_open()) {
    return util::Status(util::StatusCode::kIOError, "open events failed");
  }

  while (in) {
    TransitionEvent event;
    uint32_t state = 0;
    uint32_t reason = 0;
    uint64_t owner_size = 0;
    uint64_t token_size = 0;
    in.read(reinterpret_cast<char*>(&event.sequence), sizeof(event.sequence));
    if (in.eof()) {
      break;
    }
    in.read(reinterpret_cast<char*>(&event.id.high), sizeof(event.id.high));
    in.read(reinterpret_cast<char*>(&event.id.low), sizeof(event.id.low));
    in.read(reinterpret_cast<char*>(&state), sizeof(state));
    in.read(reinterpret_cast<char*>(&event.retry_count), sizeof(event.retry_count));
    in.read(reinterpret_cast<char*>(&event.available_after_ms), sizeof(event.available_after_ms));
    in.read(reinterpret_cast<char*>(&event.transition_ts), sizeof(event.transition_ts));
    in.read(reinterpret_cast<char*>(&event.deadline_ms), sizeof(event.deadline_ms));
    in.read(reinterpret_cast<char*>(&reason), sizeof(reason));
    in.read(reinterpret_cast<char*>(&owner_size), sizeof(owner_size));
    event.owner.resize(owner_size);
    in.read(event.owner.data(), static_cast<std::streamsize>(owner_size));
    in.read(reinterpret_cast<char*>(&token_size), sizeof(token_size));
    event.lease_token.resize(token_size);
    in.read(event.lease_token.data(), static_cast<std::streamsize>(token_size));
    if (!in) {
      return util::Status(util::StatusCode::kCorruption, "events corrupted");
    }
    event.state = DecodeState(state);
    event.reason = DecodeReason(reason);
    if (event.sequence <= max_sequence) {
      events.push_back(std::move(event));
    }
  }

  return events;
}

void QueueEngine::ApplyTransition(ConsumerGroupState& group, const TransitionEvent& event) {
  auto it = group.runtime.find(event.id);
  if (it == group.runtime.end()) {
    return;
  }
  it->second.state = event.state;
  it->second.retry_count = event.retry_count;
  it->second.available_after_ms = event.available_after_ms;
  it->second.last_transition_ts = event.transition_ts;
  it->second.sequence = event.sequence;
  it->second.last_transition_reason = ReasonToString(event.reason);

  group.inflight.erase(event.id);
  if (event.state == MessageState::kInFlight) {
    ScheduleInflight(group, event.id, event.deadline_ms, event.owner, event.lease_token, it->second.offset);
  }
}

util::Status QueueEngine::LoadGroupState(const std::string& queue_name,
                                         const std::string& group_id,
                                         QueueState& queue_state,
                                         ConsumerGroupState& group) {
  InitScheduler(group);
  auto offsets = queue_state.segment.Offsets();
  std::unordered_map<uint64_t, model::Message> offset_to_message;
  for (uint64_t offset : offsets) {
    auto record_or = queue_state.segment.Read(offset);
    if (!record_or.ok()) {
      return record_or.status();
    }
    model::Message msg;
    msg.id.high = record_or.value().header.msg_id_high;
    msg.id.low = record_or.value().header.msg_id_low;
    msg.enqueue_ts = record_or.value().header.enqueue_ts;
    offset_to_message[offset] = msg;
  }

  for (const auto& [offset, msg] : offset_to_message) {
    MessageRuntime runtime;
    runtime.state = MessageState::kReady;
    runtime.offset = offset;
    runtime.enqueue_ts = msg.enqueue_ts;
    runtime.last_transition_ts = msg.enqueue_ts;
    runtime.last_transition_reason = "recovered-default-ready";
    group.runtime[msg.id] = runtime;
  }

  auto state_path = GroupStatePath(options_.data_dir, queue_name, group_id);
  if (std::filesystem::exists(state_path)) {
    std::ifstream in(state_path, std::ios::binary);
    if (!in.is_open()) {
      return util::Status(util::StatusCode::kIOError, "open group state failed");
    }

    uint64_t count = 0;
    in.read(reinterpret_cast<char*>(&count), sizeof(count));
    in.read(reinterpret_cast<char*>(&group.next_sequence), sizeof(group.next_sequence));
    if (!in) {
      return util::Status(util::StatusCode::kCorruption, "group state corrupted");
    }
    for (uint64_t i = 0; i < count; ++i) {
      PersistedMessage item;
      uint64_t reason_size = 0;
      in.read(reinterpret_cast<char*>(&item.id_high), sizeof(item.id_high));
      in.read(reinterpret_cast<char*>(&item.id_low), sizeof(item.id_low));
      in.read(reinterpret_cast<char*>(&item.offset), sizeof(item.offset));
      in.read(reinterpret_cast<char*>(&item.state), sizeof(item.state));
      in.read(reinterpret_cast<char*>(&item.retry_count), sizeof(item.retry_count));
      in.read(reinterpret_cast<char*>(&item.enqueue_ts), sizeof(item.enqueue_ts));
      in.read(reinterpret_cast<char*>(&item.last_transition_ts), sizeof(item.last_transition_ts));
      in.read(reinterpret_cast<char*>(&item.available_after_ms), sizeof(item.available_after_ms));
      in.read(reinterpret_cast<char*>(&item.sequence), sizeof(item.sequence));
      in.read(reinterpret_cast<char*>(&reason_size), sizeof(reason_size));
      item.reason.resize(reason_size);
      in.read(item.reason.data(), static_cast<std::streamsize>(reason_size));
      if (!in) {
        return util::Status(util::StatusCode::kCorruption, "group state corrupted");
      }

      model::MessageId id{item.id_high, item.id_low};
      auto rt = group.runtime.find(id);
      if (rt == group.runtime.end()) {
        continue;
      }
      rt->second.state = DecodeState(item.state);
      rt->second.offset = item.offset;
      rt->second.retry_count = item.retry_count;
      rt->second.enqueue_ts = item.enqueue_ts;
      rt->second.last_transition_ts = item.last_transition_ts;
      rt->second.available_after_ms = item.available_after_ms;
      rt->second.sequence = item.sequence;
      rt->second.last_transition_reason = std::move(item.reason);
    }
  }

  auto events_or = LoadTransitions(queue_name, group_id, UINT64_MAX);
  if (!events_or.ok()) {
    return events_or.status();
  }
  for (const auto& event : events_or.value()) {
    ApplyTransition(group, event);
    group.next_sequence = std::max(group.next_sequence, event.sequence + 1);
  }

  const uint64_t now = CurrentTimeMs();
  for (auto& [id, rt] : group.runtime) {
    if (rt.state == MessageState::kInFlight) {
      rt.state = MessageState::kReady;
      rt.available_after_ms = now;
      rt.last_transition_reason = ReasonToString(TransitionReason::kRecoveredInflightToReady);
      rt.last_transition_ts = now;
    }
    (void)id;
  }

  BuildReadyQueue(group);
  return util::Status::Ok();
}

void QueueEngine::BuildReadyQueue(ConsumerGroupState& group) {
  group.ready_offsets.clear();
  const uint64_t now = CurrentTimeMs();
  for (auto& [id, runtime] : group.runtime) {
    if (runtime.state == MessageState::kReady && runtime.available_after_ms <= now) {
      group.ready_offsets.push_back(runtime.offset);
    }
    (void)id;
  }
  std::sort(group.ready_offsets.begin(), group.ready_offsets.end());
}

util::Status QueueEngine::PersistGroupState(const std::string& queue_name,
                                            const std::string& group_id,
                                            const ConsumerGroupState& group) {
  auto path = GroupStatePath(options_.data_dir, queue_name, group_id);
  if (util::FailpointActive("before_checkpoint_rename")) {
    return util::Status(util::StatusCode::kIOError, "failpoint before_checkpoint_rename");
  }

  return core::DurableWriteFile(
      path,
      [&](std::ofstream& out) {
        uint64_t count = group.runtime.size();
        out.write(reinterpret_cast<const char*>(&count), sizeof(count));
        out.write(reinterpret_cast<const char*>(&group.next_sequence), sizeof(group.next_sequence));
        for (const auto& [id, runtime] : group.runtime) {
          uint64_t reason_size = runtime.last_transition_reason.size();
          uint64_t id_high = id.high;
          uint64_t id_low = id.low;
          uint64_t offset = runtime.offset;
          uint32_t state = EncodeState(runtime.state);
          uint32_t retry = runtime.retry_count;
          uint64_t enqueue_ts = runtime.enqueue_ts;
          uint64_t last_transition_ts = runtime.last_transition_ts;
          uint64_t available_after = runtime.available_after_ms;
          uint64_t sequence = runtime.sequence;
          out.write(reinterpret_cast<const char*>(&id_high), sizeof(id_high));
          out.write(reinterpret_cast<const char*>(&id_low), sizeof(id_low));
          out.write(reinterpret_cast<const char*>(&offset), sizeof(offset));
          out.write(reinterpret_cast<const char*>(&state), sizeof(state));
          out.write(reinterpret_cast<const char*>(&retry), sizeof(retry));
          out.write(reinterpret_cast<const char*>(&enqueue_ts), sizeof(enqueue_ts));
          out.write(reinterpret_cast<const char*>(&last_transition_ts), sizeof(last_transition_ts));
          out.write(reinterpret_cast<const char*>(&available_after), sizeof(available_after));
          out.write(reinterpret_cast<const char*>(&sequence), sizeof(sequence));
          out.write(reinterpret_cast<const char*>(&reason_size), sizeof(reason_size));
          out.write(runtime.last_transition_reason.data(), static_cast<std::streamsize>(reason_size));
        }
        if (!out) {
          return util::Status(util::StatusCode::kIOError, "write group state failed");
        }
        return util::Status::Ok();
      },
      options_.fsync_policy);
}

util::Status QueueEngine::AppendTransition(const std::string& queue_name,
                                           const std::string& group_id,
                                           ConsumerGroupState& group,
                                           const TransitionEvent& event) {
  auto path = GroupEventPath(options_.data_dir, queue_name, group_id);
  std::filesystem::create_directories(path.parent_path());
  if (util::FailpointActive("before_log_append")) {
    return util::Status(util::StatusCode::kIOError, "failpoint before_log_append");
  }
  std::ofstream out(path, std::ios::binary | std::ios::app);
  if (!out.is_open()) {
    return util::Status(util::StatusCode::kIOError, "open event log failed");
  }

  const uint32_t state = EncodeState(event.state);
  const uint32_t reason = EncodeReason(event.reason);
  const uint64_t owner_size = event.owner.size();
  const uint64_t token_size = event.lease_token.size();

  out.write(reinterpret_cast<const char*>(&event.sequence), sizeof(event.sequence));
  out.write(reinterpret_cast<const char*>(&event.id.high), sizeof(event.id.high));
  out.write(reinterpret_cast<const char*>(&event.id.low), sizeof(event.id.low));
  out.write(reinterpret_cast<const char*>(&state), sizeof(state));
  out.write(reinterpret_cast<const char*>(&event.retry_count), sizeof(event.retry_count));
  out.write(reinterpret_cast<const char*>(&event.available_after_ms), sizeof(event.available_after_ms));
  out.write(reinterpret_cast<const char*>(&event.transition_ts), sizeof(event.transition_ts));
  out.write(reinterpret_cast<const char*>(&event.deadline_ms), sizeof(event.deadline_ms));
  out.write(reinterpret_cast<const char*>(&reason), sizeof(reason));
  out.write(reinterpret_cast<const char*>(&owner_size), sizeof(owner_size));
  out.write(event.owner.data(), static_cast<std::streamsize>(owner_size));
  out.write(reinterpret_cast<const char*>(&token_size), sizeof(token_size));
  out.write(event.lease_token.data(), static_cast<std::streamsize>(token_size));

  if (util::FailpointActive("after_log_append")) {
    return util::Status(util::StatusCode::kIOError, "failpoint after_log_append");
  }

  out.flush();
  if (!out) {
    return util::Status(util::StatusCode::kIOError, "append event failed");
  }

  ApplyTransition(group, event);
  return util::Status::Ok();
}

void QueueEngine::ReapExpiredInflight(const std::string& queue_name,
                                     const std::string& group_id,
                                     ConsumerGroupState& group) {
  if (group.deadline_wheel.empty()) {
    InitScheduler(group);
  }

  const uint64_t now = CurrentTimeMs();
  const uint64_t tick_ms = std::max<uint64_t>(1, options_.scheduler_tick.count());
  const uint64_t now_tick = now / tick_ms;

  while (group.wheel_base_tick <= now_tick) {
    const size_t slot = group.wheel_base_tick % group.deadline_wheel.size();
    auto bucket = std::move(group.deadline_wheel[slot]);
    group.deadline_wheel[slot].clear();

    for (const auto& id : bucket) {
      auto inflight_it = group.inflight.find(id);
      if (inflight_it == group.inflight.end()) {
        continue;
      }
      if (inflight_it->second.bucket_tick > group.wheel_base_tick) {
        continue;
      }
      if (inflight_it->second.deadline_ms > now) {
        const size_t re_slot = inflight_it->second.bucket_tick % group.deadline_wheel.size();
        group.deadline_wheel[re_slot].push_back(id);
        continue;
      }

      auto rt = group.runtime.find(id);
      if (rt == group.runtime.end()) {
        group.inflight.erase(inflight_it);
        continue;
      }

      TransitionEvent event;
      event.sequence = group.next_sequence++;
      event.id = id;
      event.retry_count = rt->second.retry_count + 1;
      event.transition_ts = now;
      if (event.retry_count > options_.max_retry_count) {
        event.state = MessageState::kDead;
        event.reason = TransitionReason::kVisibilityTimeoutDead;
      } else {
        event.state = MessageState::kReady;
        event.available_after_ms = now + options_.retry_backoff.count();
        event.reason = TransitionReason::kVisibilityTimeoutRequeued;
      }
      if (util::FailpointActive("before_timeout_transition")) {
        continue;
      }
      auto append = AppendTransition(queue_name, group_id, group, event);
      if (!append.ok()) {
        continue;
      }
    }

    group.wheel_base_tick += 1;
  }

  BuildReadyQueue(group);
}

util::StatusOr<QueueEngine::ConsumerGroupState*> QueueEngine::GetGroup(const std::string& queue_name,
                                                                        QueueState& queue_state,
                                                                        const std::string& group_id) {
  auto it = queue_state.groups.find(group_id);
  if (it != queue_state.groups.end()) {
    return it->second.get();
  }

  auto checkpoint_path = std::filesystem::path(options_.data_dir) / queue_name /
                         (group_id + ".checkpoint");
  storage::CheckpointStore store(checkpoint_path, options_.fsync_policy);
  auto group = std::make_unique<ConsumerGroupState>(std::move(store));
  auto* group_ptr = group.get();
  auto status = LoadGroupState(queue_name, group_id, queue_state, *group_ptr);
  if (!status.ok()) {
    return status;
  }
  queue_state.groups[group_id] = std::move(group);
  return group_ptr;
}

util::Status QueueEngine::CreateQueueOnShard(const std::string& queue_name) {
  std::lock_guard<std::mutex> lock(queues_mutex_);
  if (queues_.find(queue_name) != queues_.end()) {
    return util::Status(util::StatusCode::kAlreadyExists, "queue already exists");
  }
  auto queue_dir = std::filesystem::path(options_.data_dir) / queue_name;
  auto segment_path = queue_dir / "seg-000001.log";
  storage::SegmentLog segment(segment_path, options_.fsync_policy);
  util::Status status = segment.Open();
  if (!status.ok()) {
    return status;
  }
  status = segment.Recover();
  if (!status.ok()) {
    return status;
  }
  queues_[queue_name] = std::make_unique<QueueState>(std::move(segment));
  return util::Status::Ok();
}

util::StatusOr<uint64_t> QueueEngine::ProduceOnShard(const std::string& queue_name, const model::Message& message) {
  auto queue_or = GetQueue(queue_name);
  if (!queue_or.ok()) {
    return queue_or.status();
  }

  if (queue_or.value()->segment.next_offset() >= options_.max_queue_depth) {
    return util::Status(util::StatusCode::kUnavailable, "queue is full");
  }
  if (queue_or.value()->segment.SizeBytes() >= options_.max_segment_size_bytes) {
    return util::Status(util::StatusCode::kUnavailable, "segment size limit reached");
  }

  storage::Record record;
  record.header.msg_id_high = message.id.high;
  record.header.msg_id_low = message.id.low;
  record.header.enqueue_ts = message.enqueue_ts;
  record.payload = message.payload;
  record.routing_key = message.routing_key;
  for (const auto& kv : message.headers) {
    record.headers.emplace_back(kv.first, kv.second);
  }

  return queue_or.value()->segment.Append(record);
}

util::StatusOr<std::optional<ConsumeResult>> QueueEngine::ConsumeOnShard(const std::string& queue_name,
                                                                         const std::string& group_id) {
  auto queue_or = GetQueue(queue_name);
  if (!queue_or.ok()) {
    return queue_or.status();
  }
  auto group_or = GetGroup(queue_name, *queue_or.value(), group_id);
  if (!group_or.ok()) {
    return group_or.status();
  }
  auto* group = group_or.value();

  ReapExpiredInflight(queue_name, group_id, *group);

  if (group->inflight.size() >= options_.max_inflight) {
    return std::optional<ConsumeResult>();
  }

  if (group->ready_offsets.empty()) {
    return std::optional<ConsumeResult>();
  }

  const uint64_t offset = group->ready_offsets.front();
  group->ready_offsets.pop_front();

  auto record_or = queue_or.value()->segment.Read(offset);
  if (!record_or.ok()) {
    return record_or.status();
  }

  auto& record = record_or.value();
  model::MessageId id{record.header.msg_id_high, record.header.msg_id_low};
  auto runtime_it = group->runtime.find(id);
  if (runtime_it == group->runtime.end()) {
    MessageRuntime runtime;
    runtime.state = MessageState::kReady;
    runtime.offset = offset;
    runtime.enqueue_ts = record.header.enqueue_ts;
    runtime.last_transition_ts = CurrentTimeMs();
    runtime.last_transition_reason = "runtime-created-on-consume";
    group->runtime[id] = runtime;
    runtime_it = group->runtime.find(id);
  }

  const uint64_t now = CurrentTimeMs();
  TransitionEvent event;
  event.sequence = group->next_sequence++;
  event.id = id;
  event.state = MessageState::kInFlight;
  event.retry_count = runtime_it->second.retry_count;
  event.available_after_ms = now;
  event.transition_ts = now;
  event.deadline_ms = now + options_.visibility_timeout.count();
  event.owner = group_id;
  event.lease_token = std::to_string(id.high) + ":" + std::to_string(id.low) + ":" + std::to_string(event.sequence);
  event.reason = TransitionReason::kDelivered;
  if (util::FailpointActive("before_lease_issue")) {
    return util::Status(util::StatusCode::kIOError, "failpoint before_lease_issue");
  }
  auto append = AppendTransition(queue_name, group_id, *group, event);
  if (!append.ok()) {
    return append;
  }

  ConsumeResult result;
  result.offset = offset;
  result.delivery_count = runtime_it->second.retry_count + 1;
  result.message.id = id;
  result.message.enqueue_ts = record.header.enqueue_ts;
  result.message.payload = std::move(record.payload);
  result.message.routing_key = std::move(record.routing_key);
  for (const auto& kv : record.headers) {
    result.message.headers[kv.first] = kv.second;
  }

  auto persist = PersistGroupState(queue_name, group_id, *group);
  if (!persist.ok()) {
    return persist;
  }
  return std::optional<ConsumeResult>(result);
}

util::Status QueueEngine::AckOnShard(const std::string& queue_name,
                                     const std::string& group_id,
                                     const model::MessageId& id) {
  auto queue_or = GetQueue(queue_name);
  if (!queue_or.ok()) {
    return queue_or.status();
  }
  auto group_or = GetGroup(queue_name, *queue_or.value(), group_id);
  if (!group_or.ok()) {
    return group_or.status();
  }

  auto* group = group_or.value();
  auto it = group->inflight.find(id);
  if (it == group->inflight.end()) {
    return util::Status(util::StatusCode::kNotFound, "inflight not found");
  }

  auto runtime_it = group->runtime.find(id);
  if (runtime_it == group->runtime.end()) {
    return util::Status(util::StatusCode::kCorruption, "runtime missing");
  }

  TransitionEvent event;
  event.sequence = group->next_sequence++;
  event.id = id;
  event.state = MessageState::kAcked;
  event.retry_count = runtime_it->second.retry_count;
  event.available_after_ms = 0;
  event.transition_ts = CurrentTimeMs();
  event.reason = TransitionReason::kAcked;
  if (util::FailpointActive("before_ack_transition")) {
    return util::Status(util::StatusCode::kIOError, "failpoint before_ack_transition");
  }
  auto append = AppendTransition(queue_name, group_id, *group, event);
  if (!append.ok()) {
    return append;
  }

  auto persist = PersistGroupState(queue_name, group_id, *group);
  if (!persist.ok()) {
    return persist;
  }
  return util::Status::Ok();
}

util::Status QueueEngine::NackOnShard(const std::string& queue_name,
                                      const std::string& group_id,
                                      const model::MessageId& id,
                                      bool requeue) {
  auto queue_or = GetQueue(queue_name);
  if (!queue_or.ok()) {
    return queue_or.status();
  }
  auto group_or = GetGroup(queue_name, *queue_or.value(), group_id);
  if (!group_or.ok()) {
    return group_or.status();
  }

  auto* group = group_or.value();
  auto it = group->inflight.find(id);
  if (it == group->inflight.end()) {
    return util::Status(util::StatusCode::kNotFound, "inflight not found");
  }

  auto runtime_it = group->runtime.find(id);
  if (runtime_it == group->runtime.end()) {
    return util::Status(util::StatusCode::kCorruption, "runtime missing");
  }

  TransitionEvent event;
  event.sequence = group->next_sequence++;
  event.id = id;
  event.retry_count = runtime_it->second.retry_count;
  event.transition_ts = CurrentTimeMs();
  if (!requeue) {
    event.state = MessageState::kDead;
    event.reason = TransitionReason::kConsumerNackDead;
  } else {
    event.retry_count += 1;
    if (event.retry_count > options_.max_retry_count) {
      event.state = MessageState::kDead;
      event.reason = TransitionReason::kConsumerNackDead;
    } else {
      event.state = MessageState::kReady;
      event.available_after_ms = CurrentTimeMs() + options_.retry_backoff.count();
      event.reason = TransitionReason::kConsumerNackRequeued;
    }
  }

  if (event.state == MessageState::kDead && util::FailpointActive("before_dlq_move")) {
    return util::Status(util::StatusCode::kIOError, "failpoint before_dlq_move");
  }
  auto append = AppendTransition(queue_name, group_id, *group, event);
  if (!append.ok()) {
    return append;
  }

  BuildReadyQueue(*group);
  auto persist = PersistGroupState(queue_name, group_id, *group);
  if (!persist.ok()) {
    return persist;
  }
  return util::Status::Ok();
}

util::StatusOr<QueueStats> QueueEngine::GetStatsOnShard(const std::string& queue_name, const std::string& group_id) {
  auto queue_or = GetQueue(queue_name);
  if (!queue_or.ok()) {
    return queue_or.status();
  }
  auto group_or = GetGroup(queue_name, *queue_or.value(), group_id);
  if (!group_or.ok()) {
    return group_or.status();
  }
  auto* group = group_or.value();

  ReapExpiredInflight(queue_name, group_id, *group);

  QueueStats stats;
  uint64_t now = CurrentTimeMs();
  uint64_t oldest_ts = 0;
  for (const auto& [_, runtime] : group->runtime) {
    switch (runtime.state) {
      case MessageState::kReady:
        stats.ready_count += 1;
        if (oldest_ts == 0 || runtime.enqueue_ts < oldest_ts) {
          oldest_ts = runtime.enqueue_ts;
        }
        break;
      case MessageState::kInFlight:
        stats.inflight_count += 1;
        break;
      case MessageState::kAcked:
        stats.acked_count += 1;
        break;
      case MessageState::kDead:
        stats.dead_count += 1;
        break;
    }
  }

  if (oldest_ts > 0 && now > oldest_ts) {
    stats.oldest_ready_age_ms = now - oldest_ts;
  }
  return stats;
}

util::StatusOr<MessageDebugView> QueueEngine::InspectMessageOnShard(const std::string& queue_name,
                                                                    const std::string& group_id,
                                                                    const model::MessageId& id) {
  auto queue_or = GetQueue(queue_name);
  if (!queue_or.ok()) {
    return queue_or.status();
  }
  auto group_or = GetGroup(queue_name, *queue_or.value(), group_id);
  if (!group_or.ok()) {
    return group_or.status();
  }
  auto* group = group_or.value();

  auto it = group->runtime.find(id);
  if (it == group->runtime.end()) {
    return util::Status(util::StatusCode::kNotFound, "message not found");
  }

  MessageDebugView view;
  view.state = it->second.state;
  view.sequence = it->second.sequence;
  view.retry_count = it->second.retry_count;
  view.enqueue_time_ms = it->second.enqueue_ts;
  view.last_transition_time_ms = it->second.last_transition_ts;
  view.next_visible_at_ms = it->second.available_after_ms;
  view.last_transition_reason = it->second.last_transition_reason;
  auto inflight_it = group->inflight.find(id);
  if (inflight_it != group->inflight.end()) {
    view.lease_owner = inflight_it->second.owner;
    view.lease_token = inflight_it->second.lease_token;
    view.next_visible_at_ms = inflight_it->second.deadline_ms;
  }
  return view;
}

util::StatusOr<ReplayResult> QueueEngine::ReplayToSequenceOnShard(const std::string& queue_name,
                                                                   const std::string& group_id,
                                                                   uint64_t until_sequence) {
  auto queue_or = GetQueue(queue_name);
  if (!queue_or.ok()) {
    return queue_or.status();
  }

  ConsumerGroupState replay(storage::CheckpointStore(std::filesystem::path("/tmp/not-used"), options_.fsync_policy));
  InitScheduler(replay);

  auto offsets = queue_or.value()->segment.Offsets();
  for (uint64_t offset : offsets) {
    auto record_or = queue_or.value()->segment.Read(offset);
    if (!record_or.ok()) {
      return record_or.status();
    }
    model::MessageId id{record_or.value().header.msg_id_high, record_or.value().header.msg_id_low};
    MessageRuntime runtime;
    runtime.state = MessageState::kReady;
    runtime.offset = offset;
    runtime.enqueue_ts = record_or.value().header.enqueue_ts;
    runtime.last_transition_ts = runtime.enqueue_ts;
    runtime.last_transition_reason = "replay-default-ready";
    replay.runtime[id] = runtime;
  }

  auto events_or = LoadTransitions(queue_name, group_id, until_sequence);
  if (!events_or.ok()) {
    return events_or.status();
  }
  for (const auto& event : events_or.value()) {
    ApplyTransition(replay, event);
  }
  BuildReadyQueue(replay);

  ReplayResult result;
  result.applied_sequence = events_or.value().empty() ? 0 : events_or.value().back().sequence;
  for (const auto& [_, runtime] : replay.runtime) {
    switch (runtime.state) {
      case MessageState::kReady:
        result.stats.ready_count += 1;
        break;
      case MessageState::kInFlight:
        result.stats.inflight_count += 1;
        break;
      case MessageState::kAcked:
        result.stats.acked_count += 1;
        break;
      case MessageState::kDead:
        result.stats.dead_count += 1;
        break;
    }
  }
  return result;
}


}  // namespace pomai::queue::engine
