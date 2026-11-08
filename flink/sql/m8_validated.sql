-- M8: the M1 feature job with validation at the edge (PLAN.md 7.3, experiment F6).
--
-- Reading `payments` through the json format cannot be made safe: `json.ignore-parse-errors` drops malformed
-- records silently (invisible loss), and a record that is valid JSON of the wrong shape (a bare number, an array,
-- a string where an object is expected) makes the format throw a ClassCastException. The job then restarts from
-- its last checkpoint, reads the same record again, and loops for ever: one poison pill stops the pipeline (F6).
--
-- So the topic is read as raw text and parsed in SQL, where a bad value is a NULL and not an exception. Records
-- that are not a well-formed payment go to the `dlq` topic, untouched, and everything else follows the M1 path.
CREATE TABLE payments_raw (
  payload STRING,
  headers MAP<STRING, BYTES> METADATA VIRTUAL,        -- carries the W3C traceparent through Flink (PLAN.md 6.6)
  proc_time AS PROCTIME()
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
  proc_time,
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
  PARTITION BY user_id ORDER BY proc_time
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
