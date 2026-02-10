# Crash Testing

`tests/crash/runner.cc` is the crash oracle harness sample.

Flow:
1. Start engine
2. Run scripted randomized operations
3. Stop/restart repeatedly to emulate crash boundaries
4. Validate aggregate invariants against an in-test oracle counter model

CI runs this harness as a required gate.
