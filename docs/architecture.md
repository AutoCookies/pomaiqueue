# Architecture (SOT)

PomaiQueue is a single-host durable queue with sharded actor threads. The engine lives in `src/core` and the gRPC server is a thin wrapper.

## Current state

Implemented:

- Segment log with append/recover/read.
- Checkpoint store for committed offsets.
- Sharded actor skeleton with synchronous task submission.

TODO:

- Full consume/ack state machine (prefetch, redelivery, inflight timeout enforcement).
- Segment rotation, sparse index, retention and deletion rules.
- gRPC server implementation.
- CLI tools for inspection and admin.

## Actors and shards

```
Producer -> shard hash -> single-writer shard actor -> segment log
Consumer -> shard hash -> group state -> inflight -> checkpoint
```

The shard actor currently executes tasks synchronously using a queue and worker thread. This is a placeholder for more advanced scheduling.
