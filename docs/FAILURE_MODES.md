# Failure Modes

## Crash during enqueue
Segment recovery truncates partial tail records; no half-record is replayed.

## Crash during consume/ack/nack
Transition log is append-only; after restart the latest durable transition wins.

## Crash with inflight
Inflight entries are recovered as ready-for-redelivery, preserving at-least-once semantics.

## Corrupted state/event files
Engine returns corruption/IO status and avoids silent divergence.
