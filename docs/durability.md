# Durability (SOT)

## Implemented

- Append-only segment log with CRC64 validation.
- Recovery scans segments, validates CRC, truncates partial tails.
- Checkpoint store uses atomic rename and fsync.

## TODO

- Fsync policy scheduling (interval).
- Multi-segment retention rules.
- Crash harness with deterministic replay.
- Proof of invariants for at-least-once and ack durability.
