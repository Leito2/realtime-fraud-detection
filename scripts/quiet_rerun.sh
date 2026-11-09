#!/bin/bash
# Re-measure, on a quiet machine, what changed after the final fixes: option B bridge, E4 end to end, E8.
# Progress in chaos_series_quiet.log (git-ignored). Never uses `down` or `-v`.
cd "$(dirname "$0")/.." || exit 1
LOG=chaos_series_quiet.log
docker compose --profile core --profile ops --profile grpc --profile serving-triton stop \
  grafana prometheus kafka-exporter redis-exporter explainer triton llm-stub auditor scorer-shadow api scoring-grpc > /dev/null 2>&1
echo "=== e4 $(date +%H:%M:%S)" >> $LOG
PYTHONUNBUFFERED=1 uv run python -m fraud_bench.run e4 --rates 200 400 --reps 2 >> $LOG 2>&1
echo "=== e4 exit $?" >> $LOG
bash scripts/final_measurements.sh e8 >> $LOG 2>&1
echo "=== QUIET DONE" >> $LOG
