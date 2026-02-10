#!/usr/bin/env bash
set -euo pipefail
BUILD_DIR=${1:-build}
OUT=${2:-results.json}
{
  echo '{"results":['
  ${BUILD_DIR}/bench_enqueue
  echo ','
  ${BUILD_DIR}/bench_lease_ack
  echo ','
  ${BUILD_DIR}/bench_timeout_retry
  echo ','
  ${BUILD_DIR}/bench_recovery
  echo ']}'
} > "$OUT"
echo "wrote $OUT"
