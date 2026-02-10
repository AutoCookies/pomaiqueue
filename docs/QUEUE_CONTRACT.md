# Pomai Queue Contract (Single Source of Truth)

Pomai Queue is a **deterministic, bounded, crash-safe backend task queue**.

## Canonical semantics

### Delivery
- **At-least-once** delivery for pull consumers.
- Messages are delivered only via `Consume(queue, group)` and require explicit `Ack`.
- If visibility timeout expires before `Ack`, the message is retried with deterministic fixed backoff.
- If retry limit is exceeded, the message transitions to `DEAD` (DLQ semantics).

### Ordering
- Ordering is **per queue + consumer group** by persisted message offset.
- Retries re-enter readiness with backoff and preserve deterministic offset-based fairness.

### Message states
A message for a given consumer group is in exactly one of:
- `READY`
- `IN_FLIGHT`
- `ACKED`
- `DEAD`

No dual-state membership is allowed.

### Persistence
- Message payloads are persisted in an append-only segment log (`seg-000001.log`) with CRC64.
- Group state is atomically snapshotted (`<group>.state`) and renamed.
- Segment recovery truncates partial/corrupt tail records.

### Crash behavior
- Crash mid-enqueue: tail-truncated on recovery; no partial message replay.
- Crash mid-ack / mid-retry transition: state is recovered from last atomic snapshot.
- Crash with in-flight messages: on restart, `IN_FLIGHT` messages deterministically become `READY`.

### Consumer death
- Unacked `IN_FLIGHT` messages are retried after visibility timeout.
- After retry limit exhaustion, they move to `DEAD`.

## Bounded-by-design limits
Each queue enforces hard limits:
- `max_queue_depth`
- `max_segment_size_bytes`
- `max_inflight`
- `max_retry_count`
- `retention` (contracted; current implementation surfaces this in configuration and policy docs)

On producer overflow, enqueue is rejected with `kUnavailable`.

## Observability contract
For each queue+group, engine exposes:
- READY count
- IN_FLIGHT count
- ACKED count
- DEAD count (DLQ count)
- Oldest READY message age

For each message ID, engine exposes:
- Current state
- Retry count
- Last transition reason
