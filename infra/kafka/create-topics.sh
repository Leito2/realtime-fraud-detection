#!/bin/sh
# Topics from PLAN.md §2.4. Idempotent.
set -e
BS=${BOOTSTRAP:-kafka:29092}
T=/opt/kafka/bin/kafka-topics.sh
create() { $T --bootstrap-server "$BS" --create --if-not-exists --topic "$1" --partitions "$2" ${3:+--config "$3"} ${4:+--config "$4"}; }
create payments 12 retention.ms=3600000 message.timestamp.type=LogAppendTime
create payments-enriched 12 retention.ms=3600000
create decisions 12 retention.ms=86400000 message.timestamp.type=LogAppendTime
create shadow-scores 6 retention.ms=86400000
create labels 6 retention.ms=86400000
create explanations 3 retention.ms=86400000
create dlq 1 retention.ms=604800000
echo "topics ready"
