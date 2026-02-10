# Engine Contract

This document operationalizes `docs/QUEUE_CONTRACT.md` for the in-process engine.

## Guarantees
- At-least-once delivery per `queue + group`.
- Deterministic state replay from segment log + append-only transition log (`<group>.events`).
- Durable group snapshots (`<group>.state`) accelerate restart but do not define semantics.
- Bounded scheduling uses a fixed-size deadline wheel, avoiding full inflight scans.

## Explain API
`InspectMessage(queue, group, id)` returns:
- state
- sequence
- enqueue_time_ms
- last_transition_time_ms
- retry_count
- next_visible_at_ms
- last_transition_reason
- lease_owner, lease_token for inflight messages.

## Replay API
`ReplayToSequence(queue, group, until_sequence)` reconstructs point-in-time counts.
