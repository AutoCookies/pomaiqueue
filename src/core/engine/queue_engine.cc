#include "src/core/engine/queue_engine.h"

#include <algorithm>
#include <filesystem>

namespace pomai::queue::engine {

QueueEngine::QueueEngine(EngineOptions options) : options_(std::move(options)) {}

QueueEngine::~QueueEngine() {
  Stop();
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

util::StatusOr<QueueEngine::QueueState*> QueueEngine::GetQueue(const std::string& queue_name) {
  std::lock_guard<std::mutex> lock(queues_mutex_);
  auto it = queues_.find(queue_name);
  if (it == queues_.end()) {
    return util::Status(util::StatusCode::kNotFound, "queue not found");
  }
  return it->second.get();
}

util::StatusOr<QueueEngine::ConsumerGroupState*> QueueEngine::GetGroup(QueueState& queue_state,
                                                                       const std::string& group_id) {
  auto it = queue_state.groups.find(group_id);
  if (it != queue_state.groups.end()) {
    return it->second.get();
  }

  auto checkpoint_path = std::filesystem::path(options_.data_dir) / queue_state.segment.path().parent_path().filename() /
                        (group_id + ".checkpoint");
  storage::CheckpointStore store(checkpoint_path, options_.fsync_policy);
  auto loaded = store.Load();
  if (!loaded.ok()) {
    return loaded.status();
  }

  auto group = std::make_unique<ConsumerGroupState>(std::move(store));
  group->committed_offset = loaded.value();
  auto* group_ptr = group.get();
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
  auto group_or = GetGroup(*queue_or.value(), group_id);
  if (!group_or.ok()) {
    return group_or.status();
  }
  auto* group = group_or.value();

  if (group->inflight.size() >= options_.max_inflight) {
    return std::optional<ConsumeResult>();
  }

  uint64_t next_offset = group->committed_offset + 1;
  auto record_or = queue_or.value()->segment.Read(next_offset);
  if (!record_or.ok()) {
    if (record_or.status().code() == util::StatusCode::kNotFound) {
      return std::optional<ConsumeResult>();
    }
    return record_or.status();
  }

  auto& record = record_or.value();
  ConsumeResult result;
  result.offset = next_offset;
  result.delivery_count = 1;
  result.message.id.high = record.header.msg_id_high;
  result.message.id.low = record.header.msg_id_low;
  result.message.enqueue_ts = record.header.enqueue_ts;
  result.message.payload = std::move(record.payload);
  result.message.routing_key = std::move(record.routing_key);
  for (const auto& kv : record.headers) {
    result.message.headers[kv.first] = kv.second;
  }

  InflightEntry entry;
  entry.offset = next_offset;
  entry.deadline = std::chrono::steady_clock::now() + options_.visibility_timeout;
  entry.delivery_count = 1;
  group->inflight[result.message.id] = entry;

  return std::optional<ConsumeResult>(result);
}

util::Status QueueEngine::AckOnShard(const std::string& queue_name,
                                     const std::string& group_id,
                                     const model::MessageId& id) {
  auto queue_or = GetQueue(queue_name);
  if (!queue_or.ok()) {
    return queue_or.status();
  }
  auto group_or = GetGroup(*queue_or.value(), group_id);
  if (!group_or.ok()) {
    return group_or.status();
  }

  auto* group = group_or.value();
  auto it = group->inflight.find(id);
  if (it == group->inflight.end()) {
    return util::Status(util::StatusCode::kNotFound, "inflight not found");
  }

  group->committed_offset = std::max(group->committed_offset, it->second.offset);
  group->inflight.erase(it);
  return group->checkpoint_store.Save(group->committed_offset);
}

util::Status QueueEngine::NackOnShard(const std::string& queue_name,
                                      const std::string& group_id,
                                      const model::MessageId& id,
                                      bool requeue) {
  auto queue_or = GetQueue(queue_name);
  if (!queue_or.ok()) {
    return queue_or.status();
  }
  auto group_or = GetGroup(*queue_or.value(), group_id);
  if (!group_or.ok()) {
    return group_or.status();
  }

  auto* group = group_or.value();
  auto it = group->inflight.find(id);
  if (it == group->inflight.end()) {
    return util::Status(util::StatusCode::kNotFound, "inflight not found");
  }

  if (!requeue) {
    group->inflight.erase(it);
    return util::Status::Ok();
  }

  it->second.deadline = std::chrono::steady_clock::now();
  return util::Status::Ok();
}

}  // namespace pomai::queue::engine
