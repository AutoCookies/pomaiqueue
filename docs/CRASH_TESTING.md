# Crash Testing

`tests/crash/runner.cc` is the crash oracle harness.

It exercises deterministic failpoints for critical crash windows:
- `before_log_append`
- `before_lease_issue`
- `before_ack_transition`
- `before_timeout_transition`
- `before_dlq_move`
- `before_checkpoint_rename`

Flow:
1. Start engine and run scripted operations.
2. Inject failpoint to emulate crash/failure boundary.
3. Restart engine without failpoint.
4. Validate invariants: no invalid state, no stuck inflight after restart, bounded accounting.

CI runs a fast sample (`./build/crash_runner 2`); local runs can use larger iteration counts.
