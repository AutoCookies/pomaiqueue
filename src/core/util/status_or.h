#pragma once

#include <utility>

#include "src/core/util/status.h"

namespace pomai::queue::util {

template <typename T>
class StatusOr {
 public:
  StatusOr(Status status) : status_(std::move(status)), has_value_(false) {}
  StatusOr(const T& value) : status_(Status::Ok()), value_(value), has_value_(true) {}
  StatusOr(T&& value) : status_(Status::Ok()), value_(std::move(value)), has_value_(true) {}

  bool ok() const { return status_.ok(); }
  const Status& status() const { return status_; }

  const T& value() const & { return value_; }
  T& value() & { return value_; }
  T&& value() && { return std::move(value_); }

 private:
  Status status_;
  T value_{};
  bool has_value_ = false;
};

}  // namespace pomai::queue::util
