#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

cmake -S . -B build -DPOMAIQ_BUILD_SERVER=ON
cmake --build build --parallel
ctest --test-dir build --output-on-failure -R "test_"
ctest --test-dir build --output-on-failure -R "integration"
./build/crash_runner 2
