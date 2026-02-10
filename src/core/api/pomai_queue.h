#pragma once

#include <optional>
#include <string>

#include "src/core/engine/queue_engine.h"
#include "src/core/util/status.h"
#include "src/core/util/status_or.h"

namespace pomai::queue::api {

using EngineOptions = engine::EngineOptions;
using ConsumeResult = engine::ConsumeResult;
using ReplayResult = engine::ReplayResult;

class PomaiQueue {
 public:
  explicit PomaiQueue(EngineOptions options);

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
  util::StatusOr<engine::QueueStats> GetStats(const std::string& queue_name, const std::string& group_id);
  util::StatusOr<engine::MessageDebugView> InspectMessage(const std::string& queue_name,
                                                          const std::string& group_id,
                                                          const model::MessageId& id);
  util::StatusOr<ReplayResult> ReplayToSequence(const std::string& queue_name,
                                                const std::string& group_id,
                                                uint64_t until_sequence);

 private:
  engine::QueueEngine engine_;
};

}  // namespace pomai::queue::api
