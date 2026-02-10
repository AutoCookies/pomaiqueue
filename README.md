# PomaiQueue

Pomai Queue is a deterministic, bounded, crash-safe backend task queue with first-class replay debugging.

## Identity

Pomai focuses on production trust:
- deterministic pull consumption
- append-only transition history per queue/group
- bounded scheduler (time wheel)
- explicit ACK/NACK and DLQ behavior
- crash oracle regression tests

It is intentionally **not** a Kafka/RabbitMQ clone.

## Canonical contract

Behavior source of truth is [`docs/QUEUE_CONTRACT.md`](docs/QUEUE_CONTRACT.md).
Engine operational contract and invariants are in:
- [`docs/ENGINE_CONTRACT.md`](docs/ENGINE_CONTRACT.md)
- [`docs/INVARIANTS.md`](docs/INVARIANTS.md)
- [`docs/FAILURE_MODES.md`](docs/FAILURE_MODES.md)

## Build and test

```bash
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```

## Replay and explain CLI

```bash
./build/pomaiqctl explain --data_dir /tmp/pomaiqueue --queue q --group g --id 42
./build/pomaiqctl replay --data_dir /tmp/pomaiqueue --queue q --group g --until_seq 100
```

## Benchmarks

```bash
./build/bench_pomaiqueue > bench.json
```

Produces stable JSON for enqueue throughput by payload class.

## Crash testing

```bash
./build/crash_runner
```
