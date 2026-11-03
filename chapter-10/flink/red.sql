-- Chapter 10: the Flink job. The source and sink tables are declared here;
-- the INSERT is Listing 10.2.

-- Single-phase window aggregation. The default splits the window operator into
-- a local and a global half, and a late span is discarded by the local half,
-- which reports no late-record count. One phase keeps the count on one operator.
SET 'table.optimizer.agg-phase-strategy' = 'ONE_PHASE';

CREATE TABLE spans (
  service_name STRING,
  span_name STRING,
  span_kind STRING,
  status_code STRING,
  duration_ms DOUBLE,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'spans.flat',
  'properties.bootstrap.servers' = 'kafka:9093',
  'properties.group.id' = 'red-stream',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE red_metrics (
  service_name STRING,
  span_name STRING,
  window_start TIMESTAMP(3),
  calls BIGINT,
  errors BIGINT,
  p99_ms DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'red_metrics',
  'properties.bootstrap.servers' = 'kafka:9093',
  'format' = 'json'
);

-- ---- Listing 10.2: A Flink tumbling-window RED query ----

INSERT INTO red_metrics
SELECT
  service_name,
  span_name,
  window_start,
  COUNT(*) AS calls,
  SUM(CASE WHEN status_code = 'STATUS_CODE_ERROR'
      THEN 1 ELSE 0 END) AS errors,
  PERCENTILE(duration_ms, 0.99) AS p99_ms
FROM TABLE(
  TUMBLE(TABLE spans, DESCRIPTOR(event_time),
         INTERVAL '60' SECONDS))
WHERE span_kind IN ('SERVER', 'CONSUMER')
GROUP BY window_start, window_end, service_name, span_name;
-- ---- end listing 10.2 ----
