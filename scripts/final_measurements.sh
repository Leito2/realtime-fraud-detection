#!/bin/bash
# The measurement block that goes last, on a quiet machine (PLAN.md 5): E3, E1, E2, E9, E8, E11.
#   scripts/final_measurements.sh              -> everything, one step after another
#   scripts/final_measurements.sh e3 e8        -> only those steps
# Progress goes to chaos_series_final.log (git-ignored); a failed step does not stop the rest. Nothing here
# uses `down` or `-v`. Run nothing else on the machine meanwhile. It leaves dashboards, exporters and the
# async scorers stopped: bring them back with `docker compose --profile core --profile ops --profile grpc up -d`.
cd "$(dirname "$0")/.." || exit 1
LOG=chaos_series_final.log
steps=("$@")
[ ${#steps[@]} -eq 0 ] && steps=(e3 e1 e2 e9 e8 e11)   # rates are the ones this 8 GB laptop sustains (E1: 200 ev/s)

quiet() {
  # only what the pipeline needs: no dashboards, exporters, explainer, Triton, async scorers
  docker compose --profile core --profile ops --profile grpc --profile serving-triton stop \
    grafana prometheus kafka-exporter redis-exporter explainer triton llm-stub auditor scorer-shadow \
    api scoring-grpc > /dev/null 2>&1
  SERVING_MODE=onnx docker compose --profile core up -d --force-recreate scorer > /dev/null 2>&1
  uv run python -c "from fraud_bench.stack import cancel_flink_jobs, submit_flink; cancel_flink_jobs(); submit_flink()" >> $LOG 2>&1   # drop any backlog
  sleep 20
}

for s in "${steps[@]}"; do
  echo "=== $s $(date +%H:%M:%S)" >> $LOG
  quiet
  case $s in
    e3)  PYTHONUNBUFFERED=1 uv run python scripts/e3_buffer_timeout.py >> $LOG 2>&1 ;;
    e1)  PYTHONUNBUFFERED=1 make bench BENCH_ARGS="--rates 100 200 300 400 500 600" >> $LOG 2>&1 ;;
    e2)  PYTHONUNBUFFERED=1 uv run python -m fraud_bench.run e2 --rate 200 >> $LOG 2>&1 ;;
    e9)  PYTHONUNBUFFERED=1 uv run python scripts/e9_tracing.py --rate 200 >> $LOG 2>&1 ;;
    e8)  PYTHONUNBUFFERED=1 uv run python scripts/e8_shadow.py --rate 200 >> $LOG 2>&1 ;;
    e11) docker compose --profile core --profile grpc up -d scoring-grpc api >> $LOG 2>&1
         sleep 60
         bash scripts/run_e11.sh >> $LOG 2>&1 ;;      # 100 req/s, 200 s cap per run
  esac
  echo "=== $s exit $?" >> $LOG
done
quiet
echo "=== FINAL DONE" >> $LOG
