#include "src/core/engine/queue_engine.h"

#include <algorithm>
#include <filesystem>
#include <fstream>

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

}  // namespace

QueueEngine::QueueEngine(EngineOptions options) : options_(std::move(options)) {}

QueueEngine::~QueueEngine() {
  Stop();
}

uint64_t QueueEngine::NowMs() {
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count());
}

std::filesystem::path QueueEngine::GroupStatePath(const std::string& data_dir,
                                                  const std::string& queue_name,
                                                  const std::string& group_id) {
  return std::filesystem::path(data_dir) / queue_name / (group_id + ".state");
}

util::Status QueueEngine::Start() {
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

size_t QueueEngine::ShardForQueue(const std::string& queue_name) const {
  return std::hash<std::string>{}(queue_name) % options_.shard_count;
}

util::Status QueueEngine::CreateQueue(const std::string& queue_name) {
  auto shard_id = ShardForQueue(queue_name);
  return shards_[shard_id]->Submit([this, queue_name]() { return CreateQueueOnShard(queue_name); });
}

util::StatusOr<uint64_t> QueueEngine::Produce(const std::string& queue_name, const model::Message& message) {
  auto shard_id = ShardForQueue(queue_name);
  return shards_[shard_id]->SubmitValue<uint64_t>(
      [this, queue_name, message]() { return ProduceOnShard(queue_name, message); });
}

util::StatusOr<std::optional<ConsumeResult>> QueueEngine::Consume(const std::string& queue_name,
                                                                  const std::string& group_id) {
  auto shard_id = ShardForQueue(queue_name);
  return shards_[shard_id]->SubmitValue<std::optional<ConsumeResult>>(
      [this, queue_name, group_id]() { return ConsumeOnShard(queue_name, group_id); });
}

util::Status QueueEngine::Ack(const std::string& queue_name,
                              const std::string& group_id,
                              const model::MessageId& id) {
  auto shard_id = ShardForQueue(queue_name);
  return shards_[shard_id]->Submit([this, queue_name, group_id, id]() { return AckOnShard(queue_name, group_id, id); });
}

util::Status QueueEngine::Nack(const std::string& queue_name,
                               const std::string& group_id,
                               const model::MessageId& id,
                               bool requeue) {
  auto shard_id = ShardForQueue(queue_name);
  return shards_[shard_id]->Submit(
      [this, queue_name, group_id, id, requeue]() { return NackOnShard(queue_name, group_id, id, requeue); });
}

util::StatusOr<QueueStats> QueueEngine::GetStats(const std::string& queue_name, const std::string& group_id) {
  auto shard_id = ShardForQueue(queue_name);
  return shards_[shard_id]->SubmitValue<QueueStats>(
      [this, queue_name, group_id]() { return GetStatsOnShard(queue_name, group_id); });
}

util::StatusOr<MessageDebugView> QueueEngine::InspectMessage(const std::string& queue_name,
                                                             const std::string& group_id,
                                                             const model::MessageId& id) {
  auto shard_id = ShardForQueue(queue_name);
  return shards_[shard_id]->SubmitValue<MessageDebugView>(
      [this, queue_name, group_id, id]() { return InspectMessageOnShard(queue_name, group_id, id); });
}

util::StatusOr<QueueEngine::QueueState*> QueueEngine::GetQueue(const std::string& queue_name) {
  std::lock_guard<std::mutex> lock(queues_mutex_);
  auto it = queues_.find(queue_name);
  if (it == queues_.end()) {
    return util::Status(util::StatusCode::kNotFound, "queue not found");
  }
  return it->second.get();
}

util::Status QueueEngine::LoadGroupState(const std::string& queue_name,
                                        const std::string& group_id,
                                        QueueState& queue_state,
                                        ConsumerGroupState& group) {
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

  auto cp_or = group.checkpoint_store.Load();
  if (!cp_or.ok()) {
    return cp_or.status();
  }
  auto state_path = GroupStatePath(options_.data_dir, queue_name, group_id);
  if (!std::filesystem::exists(state_path)) {
    for (uint64_t offset : offsets) {
      auto& msg = offset_to_message[offset];
      MessageRuntime runtime;
      runtime.state = MessageState::kReady;
      runtime.offset = offset;
      runtime.enqueue_ts = msg.enqueue_ts;
      runtime.last_transition_reason = "recovered-default-ready";
      group.runtime[msg.id] = runtime;
    }
    BuildReadyQueue(group);
    return util::Status::Ok();
  }

  std::ifstream in(state_path, std::ios::binary);
  if (!in.is_open()) {
    return util::Status(util::StatusCode::kIOError, "open group state failed");
  }

  uint64_t count = 0;
  in.read(reinterpret_cast<char*>(&count), sizeof(count));
  for (uint64_t i = 0; i < count; ++i) {
    PersistedMessage item;
    uint64_t reason_size = 0;
    in.read(reinterpret_cast<char*>(&item.id_high), sizeof(item.id_high));
    in.read(reinterpret_cast<char*>(&item.id_low), sizeof(item.id_low));
    in.read(reinterpret_cast<char*>(&item.offset), sizeof(item.offset));
    in.read(reinterpret_cast<char*>(&item.state), sizeof(item.state));
    in.read(reinterpret_cast<char*>(&item.retry_count), sizeof(item.retry_count));
    in.read(reinterpret_cast<char*>(&item.enqueue_ts), sizeof(item.enqueue_ts));
    in.read(reinterpret_cast<char*>(&item.available_after_ms), sizeof(item.available_after_ms));
    in.read(reinterpret_cast<char*>(&reason_size), sizeof(reason_size));
    item.reason.resize(reason_size);
    in.read(item.reason.data(), static_cast<std::streamsize>(reason_size));
    if (!in) {
      return util::Status(util::StatusCode::kCorruption, "group state corrupted");
    }

    model::MessageId id{item.id_high, item.id_low};
    MessageRuntime runtime;
    runtime.state = DecodeState(item.state);
    runtime.offset = item.offset;
    runtime.retry_count = item.retry_count;
    runtime.enqueue_ts = item.enqueue_ts;
    runtime.available_after_ms = item.available_after_ms;
    runtime.last_transition_reason = std::move(item.reason);
    group.runtime[id] = std::move(runtime);
  }

  // Clear dangling offsets and ensure every persisted message maps to an existing log message.
  for (auto it = group.runtime.begin(); it != group.runtime.end();) {
    if (offset_to_message.find(it->second.offset) == offset_to_message.end()) {
      it = group.runtime.erase(it);
      continue;
    }
    ++it;
  }

  BuildReadyQueue(group);
  return util::Status::Ok();
}

void QueueEngine::BuildReadyQueue(ConsumerGroupState& group) {
  group.ready_offsets.clear();
  group.inflight.clear();
  const uint64_t now = NowMs();
  for (auto& [id, runtime] : group.runtime) {
    if (runtime.state == MessageState::kReady && runtime.available_after_ms <= now) {
      group.ready_offsets.push_back(runtime.offset);
    }
    if (runtime.state == MessageState::kInFlight) {
      runtime.state = MessageState::kReady;
      runtime.last_transition_reason = "recovered-inflight-to-ready";
      runtime.available_after_ms = now;
      group.ready_offsets.push_back(runtime.offset);
    }
  }
  std::sort(group.ready_offsets.begin(), group.ready_offsets.end());
}

util::Status QueueEngine::PersistGroupState(const std::string& queue_name,
                                            const std::string& group_id,
                                            const ConsumerGroupState& group) {
  auto path = GroupStatePath(options_.data_dir, queue_name, group_id);
  std::filesystem::create_directories(path.parent_path());
  auto tmp = path;
  tmp += ".tmp";

  {
    std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
      return util::Status(util::StatusCode::kIOError, "open group state tmp failed");
    }

    uint64_t count = group.runtime.size();
    out.write(reinterpret_cast<const char*>(&count), sizeof(count));
    for (const auto& [id, runtime] : group.runtime) {
      uint64_t reason_size = runtime.last_transition_reason.size();
      uint64_t id_high = id.high;
      uint64_t id_low = id.low;
      uint64_t offset = runtime.offset;
      uint32_t state = EncodeState(runtime.state);
      uint32_t retry = runtime.retry_count;
      uint64_t enqueue_ts = runtime.enqueue_ts;
      uint64_t available_after = runtime.available_after_ms;
      out.write(reinterpret_cast<const char*>(&id_high), sizeof(id_high));
      out.write(reinterpret_cast<const char*>(&id_low), sizeof(id_low));
      out.write(reinterpret_cast<const char*>(&offset), sizeof(offset));
      out.write(reinterpret_cast<const char*>(&state), sizeof(state));
      out.write(reinterpret_cast<const char*>(&retry), sizeof(retry));
      out.write(reinterpret_cast<const char*>(&enqueue_ts), sizeof(enqueue_ts));
      out.write(reinterpret_cast<const char*>(&available_after), sizeof(available_after));
      out.write(reinterpret_cast<const char*>(&reason_size), sizeof(reason_size));
      out.write(runtime.last_transition_reason.data(), static_cast<std::streamsize>(reason_size));
    }
    out.flush();
    if (!out) {
      return util::Status(util::StatusCode::kIOError, "write group state failed");
    }
  }

  std::filesystem::rename(tmp, path);
  return util::Status::Ok();
}

void QueueEngine::ReapExpiredInflight(ConsumerGroupState& group) {
  const uint64_t now = NowMs();
  std::vector<model::MessageId> expired;
  for (const auto& [id, inflight] : group.inflight) {
    if (inflight.deadline_ms <= now) {
      expired.push_back(id);
    }
  }

  for (const auto& id : expired) {
    auto inflight_it = group.inflight.find(id);
    if (inflight_it == group.inflight.end()) {
      continue;
    }
    auto rt = group.runtime.find(id);
    if (rt == group.runtime.end()) {
      group.inflight.erase(inflight_it);
      continue;
    }

    rt->second.retry_count += 1;
    if (rt->second.retry_count > options_.max_retry_count) {
      rt->second.state = MessageState::kDead;
      rt->second.last_transition_reason = "visibility-timeout-retry-exhausted";
    } else {
      rt->second.state = MessageState::kReady;
      rt->second.available_after_ms = now + options_.retry_backoff.count();
      rt->second.last_transition_reason = "visibility-timeout-requeued";
    }
    group.inflight.erase(inflight_it);
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

  ReapExpiredInflight(*group);

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
    return util::Status(util::StatusCode::kCorruption, "runtime state missing");
  }

  const uint64_t now = NowMs();
  runtime_it->second.state = MessageState::kInFlight;
  runtime_it->second.last_transition_reason = "delivered-to-consumer";

  InflightEntry entry;
  entry.offset = offset;
  entry.available_after_ms = now;
  entry.deadline_ms = now + options_.visibility_timeout.count();
  group->inflight[id] = entry;

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

  runtime_it->second.state = MessageState::kAcked;
  runtime_it->second.last_transition_reason = "acked-by-consumer";
  group->inflight.erase(it);

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

  if (!requeue) {
    runtime_it->second.state = MessageState::kDead;
    runtime_it->second.last_transition_reason = "consumer-nack-dead-lettered";
    group->inflight.erase(it);
  } else {
    runtime_it->second.retry_count += 1;
    if (runtime_it->second.retry_count > options_.max_retry_count) {
      runtime_it->second.state = MessageState::kDead;
      runtime_it->second.last_transition_reason = "retry-limit-exceeded";
    } else {
      runtime_it->second.state = MessageState::kReady;
      runtime_it->second.available_after_ms = NowMs() + options_.retry_backoff.count();
      runtime_it->second.last_transition_reason = "consumer-nack-requeued";
    }
    group->inflight.erase(it);
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

  ReapExpiredInflight(*group);

  QueueStats stats;
  uint64_t now = NowMs();
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
  view.retry_count = it->second.retry_count;
  view.last_transition_reason = it->second.last_transition_reason;
  return view;
}

}  // namespace pomai::queue::engine
