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

## Quickstart (server + admin CLI)
```bash
cmake -S . -B build -DPOMAIQ_BUILD_SERVER=ON
cmake --build build --target pomaiqueue-server pomaiqctl
./build/pomaiqueue-server --config config/pomaiqueue.toml
```

Inspect a message offset:
```bash
./build/pomaiqctl inspect --queue jobs --group default --offset 1
```

Generate a bugreport bundle:
```bash
./build/pomaiqctl bugreport --queue jobs --group default --from-seq 1 --to-seq 500 --out /tmp/pomaiqueue-bugreport.tar.gz
```

## Determinism and crash safety
- Invariants: [`docs/INVARIANTS.md`](docs/INVARIANTS.md)
- Queue contract: [`docs/QUEUE_CONTRACT.md`](docs/QUEUE_CONTRACT.md)
- Runbook: [`docs/runbook.md`](docs/runbook.md)
- Crash testing: [`docs/CRASH_TESTING.md`](docs/CRASH_TESTING.md)

## P0 acceptance checklist (baseline)
- Server wrapper binary (`pomaiqueue-server`) starts from `config/pomaiqueue.toml`.
- Durable rename path uses fsync + rename + fsync(dir) in state persistence.
- Deterministic consumer session primitive and scheduler are unit tested.
- CI contains separate build/unit/integration/crash/lint/artifact stages.

## Production-ready claim policy
This project does **not** claim universally production-ready status. Readiness is defined and tracked in [`docs/READINESS_SCORECARD.md`](docs/READINESS_SCORECARD.md) with explicit scope and limitations.
