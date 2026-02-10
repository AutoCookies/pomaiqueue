#include "src/core/api/pomai_queue.h"

namespace pomai::queue::api {

PomaiQueue::PomaiQueue(EngineOptions options) : engine_(std::move(options)) {}

util::Status PomaiQueue::Start() {
  return engine_.Start();
}

util::Status PomaiQueue::Stop() {
  return engine_.Stop();
}

util::Status PomaiQueue::CreateQueue(const std::string& queue_name) {
  return engine_.CreateQueue(queue_name);
}

util::StatusOr<uint64_t> PomaiQueue::Produce(const std::string& queue_name, const model::Message& message) {
  return engine_.Produce(queue_name, message);
}

util::StatusOr<std::optional<ConsumeResult>> PomaiQueue::Consume(const std::string& queue_name,
                                                                 const std::string& group_id) {
  return engine_.Consume(queue_name, group_id);
}

util::Status PomaiQueue::Ack(const std::string& queue_name, const std::string& group_id, const model::MessageId& id) {
  return engine_.Ack(queue_name, group_id, id);
}

util::Status PomaiQueue::Nack(const std::string& queue_name,
                              const std::string& group_id,
                              const model::MessageId& id,
                              bool requeue) {
  return engine_.Nack(queue_name, group_id, id, requeue);
}

util::StatusOr<engine::QueueStats> PomaiQueue::GetStats(const std::string& queue_name, const std::string& group_id) {
  return engine_.GetStats(queue_name, group_id);
}

util::StatusOr<engine::MessageDebugView> PomaiQueue::InspectMessage(const std::string& queue_name,
                                                                    const std::string& group_id,
                                                                    const model::MessageId& id) {
  return engine_.InspectMessage(queue_name, group_id, id);
}

util::StatusOr<ReplayResult> PomaiQueue::ReplayToSequence(const std::string& queue_name,
                                                     const std::string& group_id,
                                                     uint64_t until_sequence) {
  return engine_.ReplayToSequence(queue_name, group_id, until_sequence);
}

}  // namespace pomai::queue::api
