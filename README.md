# PomaiQueue

![CI](https://github.com/example/pomaiqueue/actions/workflows/ci.yml/badge.svg)

Pomai Queue is a deterministic, bounded, crash-safe backend task queue with replay debugging.

## Canonical contract
Behavior source of truth is [`docs/QUEUE_CONTRACT.md`](docs/QUEUE_CONTRACT.md).

## Build and test
```bash
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
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
