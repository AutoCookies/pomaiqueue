# Phase 0 — System Understanding (Deterministic Contract Baseline)

This document is a pre-implementation design artifact. It captures the current ingest→persist→consume lifecycle, where determinism is enforced, and what semantics are explicitly immutable.

## Architecture Summary

PomaiQueue is a single-host deterministic queue engine with shard-local single-writer execution. API calls route by queue hash to one shard actor. Queue data is persisted in an append-only segment log, while per `queue+group` delivery state is derived from a persistent snapshot (`<group>.state`) plus append-only transition events (`<group>.events`).

- **Ingress path**: `Produce` appends a record to queue segment log and initializes/updates per-group runtime state on demand.
- **Delivery path**: `Consume` marks one READY message `IN_FLIGHT` with lease metadata and deadline-wheel registration.
- **Terminal path**: `Ack` transitions `IN_FLIGHT -> ACKED`; `Nack` transitions to `READY` (requeue with fixed backoff) or `DEAD`.
- **Recovery path**: restart reconstructs runtime from segment offsets + state snapshot + transition replay, then converts residual `IN_FLIGHT` to `READY` deterministically.

## End-to-End Flow (Current)

### 1) Ingest
1. Client calls `Produce(queue, message)`.
2. Message is appended to queue segment (`seg-*.log`) with CRC-protected record framing.
3. Offset is durable according to configured fsync policy.
4. Runtime for each active group treats new message as READY unless overridden by replayed transitions.

### 2) Persist
- Primary payload persistence: segment log (`src/core/storage/segment_log.*`).
- Group transition history persistence: append-only `<group>.events` file.
- Group materialized state persistence: atomic temp-write + rename (`<group>.state`).

### 3) Consume
1. Engine reaps expired leases for target group.
2. READY queue is rebuilt from deterministic runtime map (`offset` order + visibility time).
3. If inflight budget permits, earliest eligible message transitions to `IN_FLIGHT`.
4. Transition event is appended and group state persisted.

### 4) Ack / Retry / Dead
- `Ack`: requires current lease membership in inflight map; otherwise `kNotFound`.
- `Nack(requeue=true)`: increments retry count and schedules visibility delay; over budget → `DEAD`.
- Lease timeout reaper transitions `IN_FLIGHT` to `READY` or `DEAD` with explicit timeout reason.

### 5) Crash/Restart
- Segment recovery truncates torn/corrupt tail records.
- Group snapshot is loaded, then transition log replayed in sequence order.
- Any recovered `IN_FLIGHT` state is rewritten in memory as READY-at-now to preserve prefix safety and bounded liveness.

## Determinism Enforcement Points

1. **Single-writer shard actor** serializes queue mutations.
2. **Monotonic transition sequence** per group (`next_sequence`).
3. **Replay uses persisted transitions only**; no hidden side channel mutates state.
4. **Deadline wheel scheduling** bounds work deterministically by tick bucket.
5. **Retry policy is fixed backoff + max retry count** (no random jitter).
6. **Injected time function (`now_fn`) support** enables deterministic tests/replay.

## Invariant Table

| Invariant | Enforcement File(s) | What Breaks If Violated |
|---|---|---|
| Exactly one state per message (`READY/IN_FLIGHT/ACKED/DEAD`) | `src/core/engine/queue_engine.cc` | Double-delivery ambiguity, impossible explain output, replay divergence |
| Transition order is monotonic by `sequence` | `src/core/engine/queue_engine.cc` and `<group>.events` serialization | Point-in-time replay becomes nondeterministic |
| `IN_FLIGHT` carries lease owner/token and scheduler registration | `src/core/engine/queue_engine.cc` | Ack lease validity collapses; timeout behavior undefined |
| Recovery derives from durable segment + transitions | `src/core/storage/segment_log.cc`, `src/core/engine/queue_engine.cc` | Prefix safety and crash reproducibility break |
| Retry overflow transitions to `DEAD` | `src/core/engine/queue_engine.cc` | Infinite retry loops, boundedness violation |
| Scheduler scans only current wheel bucket | `src/core/engine/queue_engine.cc` | Work becomes unbounded under backlog |

## Explicit “Do Not Change” List

1. **State machine semantics**
   - Ack validity must remain lease-bound to current inflight attempt.
   - Duplicate/stale ack must not mutate state.

2. **Event log ordering semantics**
   - Transition event append order defines replay truth.
   - No out-of-band rewrites of `<group>.events`.

3. **Snapshot atomicity model**
   - Snapshot must remain write-temp + rename atomic replacement.
   - Snapshot may accelerate recovery but must never supersede transition semantics.

4. **Crash recovery prefix safety**
   - Tail truncation of partial/corrupt records is mandatory.
   - Recovery must never resurrect non-durable tail bytes.

5. **No nondeterministic background mutation**
   - Any deletion/compaction/timeout-induced transition must be represented as deterministic events.

