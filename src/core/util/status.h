#pragma once

#include <string>
#include <utility>

namespace pomai::queue::util {

enum class StatusCode {
  kOk = 0,
  kNotFound,
  kAlreadyExists,
  kInvalidArgument,
  kIOError,
  kCorruption,
  kUnavailable,
  kDeadlineExceeded,
  kInternal
};

class Status {
 public:
  Status() : code_(StatusCode::kOk) {}
  Status(StatusCode code, std::string message)
      : code_(code), message_(std::move(message)) {}

  static Status Ok() { return Status(); }

  bool ok() const { return code_ == StatusCode::kOk; }
  StatusCode code() const { return code_; }
  const std::string& message() const { return message_; }

 private:
  StatusCode code_;
  std::string message_;
};

}  // namespace pomai::queue::util
