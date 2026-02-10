# Test + Benchmark Baseline Audit

## Scope reviewed
- Engine/state machine: `src/core/engine/queue_engine.{h,cc}`
- Storage/log/checkpoint: `src/core/storage/{record,segment_log,checkpoint}.{h,cc}`
- API/CLI/server: `src/core/api/pomai_queue.{h,cc}`, `src/cli/pomaiqctl.cc`, `src/server/main.cc`
- Existing tests/bench/CI: `tests/unit/*`, `tests/crash/runner.cc`, `benchmarks/bench_pomaiqueue.cc`, `.github/workflows/ci.yml`
- Contract docs: `docs/QUEUE_CONTRACT.md`

## What is covered today
- Record encode/decode + CRC happy path is covered by one unit test.
- Engine has basic unit coverage for retry/DLQ path, replay to sequence, invalid shard config, and restart from in-flight.
- A basic crash runner exists (randomized single-process restart loop) with limited invariant checking.
- A single benchmark exists (enqueue throughput by payload size), JSON output only.
- CI builds Debug/Release, runs tests, and runs benchmark smoke.

## Gaps and risks

### Critical (P0)
1. **Contract mismatch: ordering fairness not deterministic.**
   - `BuildReadyQueue` iterates `unordered_map` and then sorts by offset, which can duplicate entries across state transitions if transitions are corrupted and does not explicitly enforce one-state constraints during mutation.
2. **Ack semantics not idempotent vs contract expectations for duplicate ACK patterns.**
   - ACK for non-inflight always returns NotFound; no lease-token-aware ack path.
3. **No failpoint-based crash oracle proving specific crash boundaries.**
   - Current crash test does not inject failure at enqueue/lease/ack/timeout/DLQ critical points.
4. **Checkpoint format lacks version/CRC and corruption guards.**
   - `CheckpointStore` stores only raw offset and cannot detect bitrot/version mismatch.

### High (P1)
1. **Unit coverage is thin on boundaries.**
   - Missing segment truncation/rollover boundaries, corrupted tails, and CRC-negative cases.
   - Missing state-machine transition matrix and backpressure boundaries (`max_queue_depth`, `max_inflight`, reject policy).
2. **Integration coverage is thin.**
   - No producer/consumer concurrency test with delivery accounting.
   - No deterministic visibility timeout redelivery, restart persistence bulk test, or ordering test anchored to contract.
3. **Benchmark proof is incomplete.**
   - No lease+ack, timeout/retry cost, or recovery benchmark.
   - No invariant checks in benchmark harness.
   - No A/B comparison tooling.

### Medium (P2)
1. **Observability proof incomplete in tests.**
   - InspectMessage fields and transition reasons not validated deeply.
2. **Crash test scale controls and deterministic seed plumbing are not documented in one place.**

## Priority plan
1. Add deterministic failpoint framework + crash oracle harness with mandatory scenarios.
2. Expand deterministic unit tests across storage/checkpoint/state machine/idempotency/backpressure.
3. Add integration + stress tests with bounded workloads and invariant accounting.
4. Replace single benchmark with multi-binary JSON benchmarks + comparison scripts.
5. Upgrade CI into explicit gates (sanitizers, crash sample, benchmark smoke artifacts).
6. Update docs (testing, benchmarking, readiness scorecard, README links/badges/claims).
