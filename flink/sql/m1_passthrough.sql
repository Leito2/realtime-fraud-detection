-- M1: payments -> payments-enriched with 10-minute velocity features (PLAN.md §3.3, ADR-1/3).
-- OVER window on processing time: one output row per input row, current payment included.
CREATE TABLE payments (
  payment_id STRING, user_id STRING, card_id STRING, amount DOUBLE, currency STRING,
  merchant_id STRING, mcc STRING, country STRING, device_id STRING, ip_country STRING,
  channel STRING, event_time STRING, t_scheduled_ns BIGINT, t_sent_ns BIGINT,
  headers MAP<STRING, BYTES> METADATA VIRTUAL,          -- carries the W3C traceparent through Flink (PLAN.md 6.6)
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'kafka', 'topic' = 'payments',
  'properties.bootstrap.servers' = 'kafka:29092', 'properties.group.id' = 'flink-features',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json', 'json.ignore-parse-errors' = 'true'
);

CREATE TABLE payments_enriched (
  payment_id STRING, user_id STRING, card_id STRING, amount DOUBLE, currency STRING,
  merchant_id STRING, mcc STRING, country STRING, device_id STRING, ip_country STRING,
  channel STRING, event_time STRING, t_scheduled_ns BIGINT, t_sent_ns BIGINT,
  f_cnt_10m BIGINT, f_sum_10m DOUBLE, f_max_10m DOUBLE, f_small_tx_cnt_10m BIGINT, t_flink_out_ns BIGINT,
  headers MAP<STRING, BYTES> METADATA
) WITH (
  'connector' = 'kafka', 'topic' = 'payments-enriched',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'raw', 'key.fields' = 'user_id',
  'value.format' = 'json', 'value.fields-include' = 'ALL',
  'sink.delivery-guarantee' = 'at-least-once',
  'properties.linger.ms' = '1'
);

INSERT INTO payments_enriched
SELECT
  payment_id, user_id, card_id, amount, currency, merchant_id, mcc, country, device_id, ip_country,
  channel, event_time, t_scheduled_ns, t_sent_ns,
  COUNT(*) OVER w,
  SUM(amount) OVER w,
  MAX(amount) OVER w,
  CAST(SUM(CASE WHEN amount < 10 THEN 1 ELSE 0 END) OVER w AS BIGINT),
  CAST(NULL AS BIGINT),
  headers
FROM payments
WINDOW w AS (
  PARTITION BY user_id ORDER BY proc_time
  RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW
);
