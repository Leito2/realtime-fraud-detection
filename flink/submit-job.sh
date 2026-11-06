#!/bin/bash
# Submit the SQL job once: `compose up` re-runs this container, and a second job would duplicate every event.
# BUFFER_TIMEOUT (e.g. "10 ms") overrides execution.buffer-timeout for this job only (experiment E3).
set -e
if curl -sf http://jobmanager:8081/jobs/overview | grep -q "\"state\":\"RUNNING\""; then
  echo "feature job already running - nothing to submit"; exit 0
fi
SQL="${1:-/opt/flink/sql/m1_passthrough.sql}"
if [ -n "$BUFFER_TIMEOUT" ]; then
  TMP=$(mktemp --suffix=.sql)
  { echo "SET 'execution.buffer-timeout' = '${BUFFER_TIMEOUT}';"; cat "$SQL"; } > "$TMP"
  SQL="$TMP"
fi
exec /opt/flink/bin/sql-client.sh -f "$SQL"
