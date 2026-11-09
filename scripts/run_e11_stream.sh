#!/bin/bash
cd "$(dirname "$0")/.." || exit 1
LOG=chaos_series_e11s.log
for rep in 1 2 3; do
  timeout 200 docker compose --profile tools run --rm -T e11 grpc-stream --target scoring-grpc:50051 \
    --rate ${RATE:-100} --duration 60 --warmup 10 >> $LOG 2>&1
  echo "=== e11s rep $rep exit $?" >> $LOG
done
echo "=== e11s DONE" >> $LOG
