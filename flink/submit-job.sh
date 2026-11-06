#!/bin/bash
# Submit the SQL job once: `compose up` re-runs this container, and a second job would duplicate every event.
set -e
if curl -sf http://jobmanager:8081/jobs/overview | grep -q "\"state\":\"RUNNING\""; then
  echo "feature job already running - nothing to submit"; exit 0
fi
exec /opt/flink/bin/sql-client.sh -f "${1:-/opt/flink/sql/m1_passthrough.sql}"
