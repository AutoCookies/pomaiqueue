# PomaiQueue

PomaiQueue is a local-first durable queue with an embedded C++20 core and an optional gRPC server wrapper. This repository contains the foundational core storage engine and scaffolding for the broader PomaiQueue roadmap.

## Status

This is an early skeleton implementation that provides:

- Core status/error model.
- Record format with CRC64 validation.
- Segment log append/recover/read.
- Checkpoint storage for consumer committed offsets.
- Sharded actor scaffolding for the engine.

**TODO:** implement full queue semantics (redelivery, inflight timeout, retention, DLQ, and gRPC server), CLI tooling, crash harness, integration tests, and benchmarks.

## Quickstart (embedded)

```bash
cmake -S . -B build
cmake --build build
```

## Directory layout

```
CMakeLists.txt
src/
  core/                 # pomai_queue_core (embedded)
    api/                # public headers: stable API surface
    engine/             # shards, routing, actor loops, schedulers
    storage/            # WAL/segments/index/checkpoints/retention
    model/              # message structs, queue/group state
    util/               # Status, errors, crc, fsync helpers, time
  rpc/                  # canonical RPC schema + adapters
    proto/              # .proto files (SOT contract)
    codec/              # mapping core<->proto, validation
  server/               # pomaiqueued (gRPC)
    grpc/               # service impl, stream sessions, auth hooks
    main.cc
  cli/                  # pomaiqctl admin + inspect

```

## Documentation

- [Architecture](docs/architecture.md)
- [Durability](docs/durability.md)
- [Protocol](docs/protocol.md)
- [Testing](docs/testing.md)
