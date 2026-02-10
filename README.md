# PomaiQueue

![Pomai Logo](/assets/logo.png)

Pomai Queue is a deterministic, bounded, crash-safe backend task queue with replay debugging.

## Canonical contract
Behavior source of truth is [`docs/QUEUE_CONTRACT.md`](docs/QUEUE_CONTRACT.md).

## Build and test
```bash
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```

## Server daemon (deterministic runtime wrapper)
```bash
cmake -S . -B build -DPOMAIQUEUE_BUILD_SERVER=ON
cmake --build build --target pomaiqueue_server
./build/pomaiqueue_server --config ./server.cfg
```

Example `server.cfg`:
```ini
data_dir=/tmp/pomaiqueue
shard_count=1
max_inflight=100
max_queue_depth=10000
max_retry_count=5
max_segment_size_bytes=67108864
visibility_timeout=30s
retry_backoff=1s
scheduler_tick=100ms
retention=24h
fsync_policy=always
bootstrap_queues=jobs,dead_jobs
shutdown_grace=5s
```

## How we prove safety
- Contract + invariants: [`docs/QUEUE_CONTRACT.md`](docs/QUEUE_CONTRACT.md), [`docs/INVARIANTS.md`](docs/INVARIANTS.md)
- Deterministic test pyramid: [`docs/TESTING.md`](docs/TESTING.md)
- Crash oracle + failpoints: [`tests/crash/runner.cc`](tests/crash/runner.cc), [`docs/CRASH_TESTING.md`](docs/CRASH_TESTING.md)

## Benchmarks
```bash
scripts/bench/run_all.sh build results.json
scripts/bench/compare.py baseline.json results.json
```
See [`docs/BENCHMARKING.md`](docs/BENCHMARKING.md) and [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md).

## Production-ready claim policy
This project does **not** claim universally production-ready status. Readiness is defined and tracked in [`docs/READINESS_SCORECARD.md`](docs/READINESS_SCORECARD.md) with explicit scope and limitations.
