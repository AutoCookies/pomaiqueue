#pragma once

#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <memory>
#include <thread>

#include "src/core/util/status.h"
#include "src/core/util/status_or.h"

namespace pomai::queue::engine {

class ShardActor {
 public:
  ShardActor();
  ~ShardActor();

  void Start();
  void Stop();

  util::Status Submit(const std::function<util::Status()>& fn);

  template <typename T>
  util::StatusOr<T> SubmitValue(const std::function<util::StatusOr<T>()>& fn) {
    auto promise = std::make_shared<std::promise<util::StatusOr<T>>>();
    auto future = promise->get_future();
    {
      std::lock_guard<std::mutex> lock(mutex_);
      tasks_.push([fn, promise]() mutable { promise->set_value(fn()); });
    }
    cv_.notify_one();
    return future.get();
  }

 private:
  void RunLoop();

  std::mutex mutex_;
  std::condition_variable cv_;
  std::queue<std::function<void()>> tasks_;
  bool running_ = false;
  std::thread worker_;
};

}  // namespace pomai::queue::engine
