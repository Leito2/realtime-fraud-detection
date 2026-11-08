#!/bin/bash
# Submit the SQL job once: `compose up` re-runs this container, and a second job would duplicate every event.
# Overrides for experiments (they only change the copy of the SQL that is submitted):
#   $1                  SQL file, default m8_validated.sql; e6_event_time.sql runs the window on event time (E6)
#   BUFFER_TIMEOUT      e.g. "10 ms"  -> execution.buffer-timeout (E3)
#   CHECKPOINT_INTERVAL e.g. "1 s"    -> execution.checkpointing.interval (E5, F10)
#   DELIVERY_GUARANTEE  exactly-once  -> transactional Kafka sink for payments-enriched (E5, F10); the DLQ stays
#                                        at-least-once. Consumers of payments-enriched must read_committed.
#   PARALLELISM         e.g. 4        -> parallelism.default (E10)
#   WATERMARK_DELAY     seconds of allowed out-of-orderness for e6_event_time.sql (default 2)
set -e
if curl -sf http://jobmanager:8081/jobs/overview | grep -q "\"state\":\"RUNNING\""; then
  echo "feature job already running - nothing to submit"; exit 0
fi
# m8_validated.sql = the M1 feature job + a raw-source branch that copies invalid payments to the dlq topic (F6)
SQL="${1:-/opt/flink/sql/m8_validated.sql}"
TMP=$(mktemp --suffix=.sql)
{
  [ -n "$BUFFER_TIMEOUT" ] && echo "SET 'execution.buffer-timeout' = '${BUFFER_TIMEOUT}';"
  [ -n "$CHECKPOINT_INTERVAL" ] && echo "SET 'execution.checkpointing.interval' = '${CHECKPOINT_INTERVAL}';"
  [ -n "$PARALLELISM" ] && echo "SET 'parallelism.default' = '${PARALLELISM}';"
  if [ "$DELIVERY_GUARANTEE" = "exactly-once" ]; then
    # the first sink in the file is payments_enriched; the transaction timeout must stay under the broker's 15 min
    sed "0,/'sink.delivery-guarantee' = 'at-least-once'/s//'sink.delivery-guarantee' = 'exactly-once',\n  'sink.transactional-id-prefix' = 'fraud-enriched',\n  'properties.transaction.timeout.ms' = '600000'/" "$SQL"
  else
    cat "$SQL"
  fi
} | sed "s/__WATERMARK_DELAY__/${WATERMARK_DELAY:-2}/g" > "$TMP"
exec /opt/flink/bin/sql-client.sh -f "$TMP"
