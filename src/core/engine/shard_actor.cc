#include "src/core/engine/shard_actor.h"

namespace pomai::queue::engine {

ShardActor::ShardActor() = default;

ShardActor::~ShardActor() {
  Stop();
}

void ShardActor::Start() {
  running_ = true;
  worker_ = std::thread([this]() { RunLoop(); });
}

void ShardActor::Stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    running_ = false;
  }
  cv_.notify_all();
  if (worker_.joinable()) {
    worker_.join();
  }
}

util::Status ShardActor::Submit(const std::function<util::Status()>& fn) {
  auto promise = std::make_shared<std::promise<util::Status>>();
  auto future = promise->get_future();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    tasks_.push([fn, promise]() mutable { promise->set_value(fn()); });
  }
  cv_.notify_one();
  return future.get();
}

void ShardActor::RunLoop() {
  while (true) {
    std::function<void()> task;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [this]() { return !running_ || !tasks_.empty(); });
      if (!running_ && tasks_.empty()) {
        return;
      }
      task = std::move(tasks_.front());
      tasks_.pop();
    }
    task();
  }
}

}  // namespace pomai::queue::engine
