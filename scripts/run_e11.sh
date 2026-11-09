#!/bin/bash
# E11 (REST vs gRPC vs gRPC stream) at the rate one client process drives reliably on this laptop.
# 3 repetitions per mode, each capped at 200 s so a stuck client cannot stall the series.
cd "$(dirname "$0")/.." || exit 1
LOG=chaos_series_e11.log
RATE=${RATE:-100}
echo "=== e11 $(date +%T)" >> $LOG
for rep in 1 2 3; do
  for mode in rest grpc grpc-stream; do
    case $mode in rest) target=http://api:8000 ;; *) target=scoring-grpc:50051 ;; esac
    timeout 200 docker compose --profile tools run --rm -T e11 $mode --target $target --rate $RATE \
      --duration 60 --warmup 10 --out /results/e11.jsonl >> $LOG 2>&1
    echo "=== e11 $mode rep $rep exit $?" >> $LOG
  done
done
echo "=== e11 DONE" >> $LOG
