-- E6 (PLAN.md 5.6): the M8 feature job with the OVER window on EVENT time instead of processing time.
--
-- Processing time stamps a payment when Flink reads it, so a late or reordered payment is counted as if it had
-- arrived on time. Event time uses the payment's own `event_time` and a watermark: the window is exact even when
-- records arrive out of order, but a row can only be emitted once the watermark has passed it, so the
-- out-of-orderness bound is paid in latency. The bound is __WATERMARK_DELAY__ seconds here (submit-job.sh fills it
-- in from WATERMARK_DELAY); payments later than that are dropped by the window, and the experiment counts them.
--
-- Partitions that go quiet hold the watermark back, so idle sources are released after 2 s.
SET 'table.exec.source.idle-timeout' = '2 s';

CREATE TABLE payments_raw (
  payload STRING,
  headers MAP<STRING, BYTES> METADATA VIRTUAL,
  event_ts AS COALESCE(
    TO_TIMESTAMP(REPLACE(REPLACE(JSON_VALUE(payload, '$.event_time'), 'T', ' '), 'Z', '')),
    TIMESTAMP '1970-01-01 00:00:00'),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '__WATERMARK_DELAY__' SECOND
) WITH (
  'connector' = 'kafka', 'topic' = 'payments',
  'properties.bootstrap.servers' = 'kafka:29092', 'properties.group.id' = 'flink-features',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'raw'
);

CREATE TEMPORARY VIEW payments_parsed AS
SELECT
  payload,
  headers,
  event_ts,
  JSON_VALUE(payload, '$.payment_id') AS payment_id,
  JSON_VALUE(payload, '$.user_id') AS user_id,
  JSON_VALUE(payload, '$.card_id') AS card_id,
  TRY_CAST(JSON_VALUE(payload, '$.amount') AS DOUBLE) AS amount,
  JSON_VALUE(payload, '$.currency') AS currency,
  JSON_VALUE(payload, '$.merchant_id') AS merchant_id,
  JSON_VALUE(payload, '$.mcc') AS mcc,
  JSON_VALUE(payload, '$.country') AS country,
  JSON_VALUE(payload, '$.device_id') AS device_id,
  JSON_VALUE(payload, '$.ip_country') AS ip_country,
  JSON_VALUE(payload, '$.channel') AS channel,
  JSON_VALUE(payload, '$.event_time') AS event_time,
  TRY_CAST(JSON_VALUE(payload, '$.t_scheduled_ns') AS BIGINT) AS t_scheduled_ns,
  TRY_CAST(JSON_VALUE(payload, '$.t_sent_ns') AS BIGINT) AS t_sent_ns
FROM payments_raw
WHERE payload IS JSON OBJECT;

CREATE TEMPORARY VIEW payments AS
SELECT * FROM payments_parsed
WHERE payment_id IS NOT NULL AND user_id IS NOT NULL AND amount IS NOT NULL;

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

CREATE TABLE dlq (
  payload STRING,
  headers MAP<STRING, BYTES> METADATA
) WITH (
  'connector' = 'kafka', 'topic' = 'dlq',
  'properties.bootstrap.servers' = 'kafka:29092',
  'format' = 'raw',
  'sink.delivery-guarantee' = 'at-least-once'
);

EXECUTE STATEMENT SET
BEGIN
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
  PARTITION BY user_id ORDER BY event_ts
  RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW
);

INSERT INTO dlq
SELECT payload, MAP['error', CAST('invalid payment at the edge' AS BYTES)]
FROM payments_raw
WHERE payload IS NOT JSON OBJECT
   OR JSON_VALUE(payload, '$.payment_id') IS NULL
   OR JSON_VALUE(payload, '$.user_id') IS NULL
   OR TRY_CAST(JSON_VALUE(payload, '$.amount') AS DOUBLE) IS NULL;
END;
