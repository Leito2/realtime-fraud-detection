#!/bin/bash
# The measurement block that goes last, on a quiet machine (PLAN.md 5): E3, E1, E2, E9, E8, E11.
#   scripts/final_measurements.sh              -> everything, one step after another
#   scripts/final_measurements.sh e3 e8        -> only those steps
# Progress goes to chaos_series_final.log (git-ignored); a failed step does not stop the rest. Nothing here
# uses `down` or `-v`. Run nothing else on the machine meanwhile.
cd "$(dirname "$0")/.." || exit 1
LOG=chaos_series_final.log
steps=("$@")
[ ${#steps[@]} -eq 0 ] && steps=(e3 e1 e2 e9 e8 e11)

quiet() {
  # only what the pipeline needs: no dashboards, exporters, explainer, Triton, async scorers
  docker compose --profile core --profile ops --profile grpc --profile serving-triton stop \
    grafana prometheus kafka-exporter redis-exporter explainer triton llm-stub auditor scorer-shadow \
    api scoring-grpc > /dev/null 2>&1
  SERVING_MODE=onnx docker compose --profile core up -d --force-recreate scorer > /dev/null 2>&1
  sleep 20
}

for s in "${steps[@]}"; do
  echo "=== $s $(date +%H:%M:%S)" >> $LOG
  quiet
  case $s in
    e3)  PYTHONUNBUFFERED=1 uv run python scripts/e3_buffer_timeout.py >> $LOG 2>&1 ;;
    e1)  PYTHONUNBUFFERED=1 make bench BENCH_ARGS="--rates 500 1000 1500 2000 3000" >> $LOG 2>&1 ;;
    e2)  PYTHONUNBUFFERED=1 uv run python -m fraud_bench.run e2 >> $LOG 2>&1 ;;
    e9)  PYTHONUNBUFFERED=1 uv run python scripts/e9_tracing.py >> $LOG 2>&1 ;;
    e8)  PYTHONUNBUFFERED=1 uv run python scripts/e8_shadow.py >> $LOG 2>&1 ;;
    e11)
      docker compose --profile core --profile grpc up -d scoring-grpc api >> $LOG 2>&1
      sleep 60
      rm -f docs/results/e11.jsonl
      for mode in rest grpc grpc-stream; do
        case $mode in rest) target=http://api:8000 ;; *) target=scoring-grpc:50051 ;; esac
        for rep in 1 2 3; do
          docker compose --profile tools run --rm -T e11 $mode --target $target --rate 300 --duration 60 \
            --out /results/e11.jsonl >> $LOG 2>&1
        done
      done ;;
  esac
  echo "=== $s exit $?" >> $LOG
done
quiet
echo "=== FINAL DONE" >> $LOG
