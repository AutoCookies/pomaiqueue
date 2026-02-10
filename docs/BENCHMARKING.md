# Benchmarking Guide

## Benchmarks
- `bench_enqueue`: payload classes 64B/1KB/16KB, outputs msgs/sec, bytes/sec, p95 enqueue latency.
- `bench_lease_ack`: lease+ack throughput + p95/p99 latency.
- `bench_timeout_retry`: timeout/retry processing cost with FakeClock and drift output.
- `bench_recovery`: startup/recovery time by persisted log size.

All benchmark binaries output JSON to stdout and assert basic invariants.

## Run all
```bash
scripts/bench/run_all.sh build results.json
```

## A/B comparison
```bash
scripts/bench/compare.py baseline.json results.json
```

The comparator prints per-metric percentage deltas.
