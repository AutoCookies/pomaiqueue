#include "src/core/util/failpoint.h"

#include <cstdlib>
#include <string>

namespace pomai::queue::util {

bool FailpointActive(std::string_view name) {
  const char* configured = std::getenv("POMAIQUEUE_FAILPOINT");
  if (configured == nullptr || *configured == '\0') {
    return false;
  }
  return std::string(configured) == name;
}

}  // namespace pomai::queue::util
