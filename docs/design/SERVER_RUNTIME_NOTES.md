# Server Runtime Design Notes (Pre-Code)

## Scope
This change introduces a **real daemon runtime wrapper** around the existing deterministic engine. It does not alter queue state machine semantics.

## Goals
- Deterministic startup from explicit config file.
- Crash-safe startup path that only starts serving after engine recovery succeeds.
- Graceful shutdown on SIGINT/SIGTERM.
- Operator-visible startup/shutdown logs.

## Non-Goals in this increment
- Full network RPC transport integration.
- Protocol redesign.

## Determinism / Correctness Constraints
- Daemon startup must be pure config + persisted state; no heuristic defaults that vary by host clock except existing engine time semantics.
- Queue bootstrap creation list must be deterministic and idempotent.
- Shutdown must stop intake before engine stop.

## Planned Structure
1. `ServerConfig` parser (`src/server/config.*`) for strict key=value config.
2. `main.cc` runtime:
   - parse args
   - load config
   - build engine options
   - start engine
   - precreate configured queues
   - block until signal
   - stop engine

## Failure Policy
- Any config parse error: fail-fast with explicit line number.
- Any engine startup error: fail-fast, non-zero exit.
- Queue bootstrap failures for preconfigured queues: fail-fast (except already-exists).

