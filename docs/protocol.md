# Protocol (SOT)

The authoritative API contract is defined in `src/rpc/proto/pomaiqueue.proto`.

## Implemented

- Proto definitions for queue lifecycle, produce/consume, ack/nack.

## TODO

- gRPC server implementation wired to core engine.
- Server-side streaming consume sessions, backpressure, auth hooks.
