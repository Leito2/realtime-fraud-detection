# Flink 2.0 + Kafka SQL connector, config baked in (the entrypoint needs a writable conf).
# The Prometheus reporter plugin already ships in /opt/flink/plugins/metrics-prometheus.
FROM flink:2.0-java17
ARG KAFKA_CONNECTOR=4.0.1-2.0
ADD --chown=flink:flink https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR}/flink-sql-connector-kafka-${KAFKA_CONNECTOR}.jar /opt/flink/lib/
RUN mkdir -p /flink-checkpoints && chown flink:flink /flink-checkpoints
COPY --chown=flink:flink flink/conf/config.yaml /opt/flink/conf/config.yaml
COPY --chown=flink:flink flink/sql /opt/flink/sql
COPY --chown=flink:flink --chmod=755 flink/submit-job.sh /opt/flink/submit-job.sh
