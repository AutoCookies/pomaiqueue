#include <cassert>
#include <vector>

#include "src/core/consumer_session.h"

using pomai::queue::core::ConsumerSession;
using pomai::queue::core::ConsumerSessionScheduler;

int main() {
  ConsumerSession session{.session_id = "s-1", .consumer_id = "c1", .group_id = "g", .max_inflight = 2, .credit_window = 2};
  assert(session.CanDispatch());
  assert(session.TryDispatch());
  assert(session.inflight == 1);
  assert(session.credit_window == 1);
  assert(session.TryDispatch());
  assert(!session.TryDispatch());
  session.AckOne();
  session.AddCredit(1);
  assert(session.CanDispatch());

  std::vector<ConsumerSession> sessions{
      {.session_id = "s-b", .consumer_id = "cb", .group_id = "g", .max_inflight = 100, .credit_window = 1},
      {.session_id = "s-a", .consumer_id = "ca", .group_id = "g", .max_inflight = 100, .credit_window = 2},
  };
  ConsumerSessionScheduler scheduler(std::move(sessions));

  auto i1 = scheduler.NextDispatchable();
  auto i2 = scheduler.NextDispatchable();
  auto i3 = scheduler.NextDispatchable();
  assert(i1.has_value() && i2.has_value() && i3.has_value());

  // Lexicographic session order provides deterministic tie-break behavior.
  assert(scheduler.sessions()[*i1].session_id == "s-a");
  scheduler.sessions()[*i1].TryDispatch();
  assert(scheduler.sessions()[*i2].session_id == "s-b");
  scheduler.sessions()[*i2].TryDispatch();
  assert(scheduler.sessions()[*i3].session_id == "s-a");

  return 0;
}
