# Invariants

1. A message is in exactly one state (`READY`, `IN_FLIGHT`, `ACKED`, `DEAD`). (`src/core/engine/queue_engine.cc`)
2. Queue/group transitions are monotonic by `sequence` in `<group>.events`. (`src/core/engine/queue_engine.cc`)
3. `IN_FLIGHT` transitions carry lease metadata and deadline scheduler registration. (`src/core/engine/queue_engine.cc`)
4. Recovery starts from durable segment log records and replays transitions deterministically.
5. Retry budget overflow transitions to `DEAD`.
6. Scheduler work is bounded by current wheel bucket, not full map scans.
