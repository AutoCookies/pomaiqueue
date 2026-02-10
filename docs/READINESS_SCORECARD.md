# Readiness Scorecard Update

## Testing rigor: ⭐⭐⭐⭐⭐
Improved from broad smoke checks to deterministic multi-layer proof:
- Expanded unit coverage for record/segment/checkpoint/state-machine/backpressure/idempotency.
- Added deterministic integration coverage for delivery accounting, DLQ counts, and restart replay.
- Added stress binary with bounded workloads and invariant exits.
- Added failpoint-driven crash oracle with scenario matrix and CI sample mode.

## Performance proof: ⭐⭐⭐⭐☆
Improved from a single enqueue throughput sample to a benchmark suite:
- Four benchmark binaries cover enqueue, lease+ack, timeout/retry, and recovery scaling.
- JSON output for all benchmarks + `run_all.sh` aggregator + A/B compare script.
- Benchmark harnesses include invariant guards.

## Remaining limitations (honest)
- No distributed replication benchmarks yet.
- RSS collection in stress output is minimal in this environment.
- Crash oracle models single-node durability semantics, not quorum semantics.
