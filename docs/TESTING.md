# Testing Guide

## Determinism rules
- Time-based behavior uses `EngineOptions::now_fn` in tests (FakeClock), never sleeps.
- Crash and randomized tests use fixed seeds.
- Workloads are bounded by fixed iteration counts.

## Run all tests
```bash
cmake -S . -B build
cmake --build build -j
ctest --test-dir build --output-on-failure
```

## Unit tests
- `test_record`: record encoding/CRC/corruption.
- `test_segment_log`: recover/truncation boundaries.
- `test_checkpoint`: checkpoint format/version/CRC.
- `test_queue_engine`: state transitions, idempotency semantics, backpressure limits, fake-clock transitions.

## Integration tests
- `test_engine_integration`: producer/consumer accounting, DLQ counts, restart/replay checks.

## Crash tests
- Fast CI sample:
```bash
./build/crash_runner 2
```
- Larger local run:
```bash
./build/crash_runner 20
```

Failpoints are env-driven via `POMAIQUEUE_FAILPOINT` and include:
`before_log_append`, `after_log_append`, `before_checkpoint_rename`, `before_lease_issue`, `before_ack_transition`, `before_timeout_transition`, `before_dlq_move`.

## Stress test
```bash
./build/pomaiqueue_stress --seconds 10 --producers 4 --consumers 4 --payload_bytes 1024 --queues 2
```
Outputs JSON throughput/latency summary and exits non-zero on invariant violation.
