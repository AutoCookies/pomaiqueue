#pragma once

#include <string_view>

namespace pomai::queue::util {

bool FailpointActive(std::string_view name);

}  // namespace pomai::queue::util
