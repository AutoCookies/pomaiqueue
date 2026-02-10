# PomaiQueue

PomaiQueue is a **safe task queue engine** for backend workloads in the Pomai ecosystem.

It is intentionally boring and bounded:
- deterministic pull consumption,
- explicit ACK/NACK,
- visibility timeout retries,
- dead-letter behavior,
- crash-safe recovery.

It is **not** a streaming platform.

## Identity and semantics

The canonical source of behavior is [`docs/QUEUE_CONTRACT.md`](docs/QUEUE_CONTRACT.md).

Highlights:
- Delivery: at-least-once.
- States: `READY`, `IN_FLIGHT`, `ACKED`, `DEAD`.
- Retries: fixed deterministic backoff + hard max retry.
- Overflow: producer reject (`kUnavailable`) when queue bounds are reached.

## Build

```bash
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```

## Documentation

- [Queue Contract](docs/QUEUE_CONTRACT.md)
- [Architecture](docs/architecture.md)
- [Durability](docs/durability.md)
- [Protocol](docs/protocol.md)
- [Testing](docs/testing.md)
