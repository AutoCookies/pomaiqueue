# Performance Proof

## What is measured
- Enqueue throughput/latency at multiple payload sizes.
- Lease+ack service rate and tail latency.
- Timeout/retry processing overhead under deterministic time progression.
- Recovery latency as persisted data scales.

## How to run
1. Build release:
```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```
2. Run suite:
```bash
scripts/bench/run_all.sh build results.json
```

## How to interpret
- Use msgs/sec and bytes/sec for throughput baselines.
- Use p95/p99 for tail-impact regression detection.
- Recovery ms should scale roughly linearly with persisted entries.
- Compare with prior runs using `scripts/bench/compare.py`.

## Not optimized (honest)
- Multi-node replication/network stack.
- Kernel bypass / io_uring / batching optimizations.
- Memory allocator tuning and full RSS attribution (stress tool reports placeholder RSS in constrained environments).
