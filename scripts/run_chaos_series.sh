#!/bin/bash
# Run several chaos experiments one after another: scripts/run_chaos_series.sh F2 F3 F4
# Each run prints its verdict into chaos_series.log; a failed run does not stop the series.
cd "$(dirname "$0")/.." || exit 1
for exp in "$@"; do
  echo "=== $exp $(date +%H:%M:%S)" >> chaos_series.log
  PYTHONUNBUFFERED=1 uv run chaos "$exp" >> chaos_series.log 2>&1
  echo "=== $exp exit $?" >> chaos_series.log
done
echo "=== SERIES DONE" >> chaos_series.log
