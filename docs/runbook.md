# PomaiQueue Runbook (P0)

## Service start
```bash
cmake -S . -B build -DPOMAIQ_BUILD_SERVER=ON
cmake --build build --target pomaiqueue-server pomaiqctl
./build/pomaiqueue-server --config config/pomaiqueue.toml
```

## Core alerts and remediation
- **Produce rejection spike (`RESOURCE_EXHAUSTED`)**
  - Check queue depth and pending bytes.
  - Mitigate by scaling consumers, acking backlog, or temporarily increasing queue limits.
- **Crash recovery loops**
  - Run `pomaiqctl inspect` for suspect offsets.
  - Verify durable files (`group_state.bin`, segment logs) and restart in conservative fsync mode (`always`).
- **Slow fsync latency**
  - Validate disk health and mount options.
  - Keep `fsync_policy=always` for deterministic safety unless a controlled benchmark proves otherwise.

## Rolling upgrade (single-node)
1. Stop new producers.
2. Wait for inflight drains and ack completion.
3. Stop server gracefully (SIGTERM).
4. Deploy new binary.
5. Start server and verify stats/inspect endpoints.

## Incident response
- Capture bug bundle:
```bash
./build/pomaiqctl bugreport --queue jobs --group default --from-seq 1 --to-seq 500 --out /tmp/bugreport.tar.gz
```
- Attach bundle and fill `docs/postmortem_template.md`.
