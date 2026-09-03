-- Chapter 8, listing 8.3: a materialized view that pre-aggregates request and
-- error rates.
-- Run: docker compose exec -T clickhouse clickhouse-client --multiquery < clickhouse/rollup.sql
--
-- Three things in here are easy to get wrong and all three are silent:
--
--   POPULATE. Without it the view only ever sees inserts that arrive after it
--   was created. Build it on a store that is already full and the dashboard
--   reads zero while looking perfectly healthy.
--
--   The root-span filter. The rollup counts requests, so it has to filter to
--   roots exactly as listing 8.1 does. Leave it out and every number is seven
--   times too big, which is a difference small enough to look plausible.
--
--   The re-sum on read. SummingMergeTree collapses duplicate keys on a
--   background merge that has not necessarily run. Read the table without
--   summing again and you get one row per insert batch, not one per minute.

DROP VIEW IF EXISTS tracing.red_by_service;

-- ---- Listing 8.3: A materialized view that pre-aggregates request and error rates ----
CREATE MATERIALIZED VIEW tracing.red_by_service
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (minute, service_name, status_code)
TTL minute + INTERVAL 90 DAY
POPULATE
AS SELECT
  service_name,
  status_code,
  toStartOfMinute(timestamp) AS minute,
  sum(adjusted_count) AS requests
FROM tracing.otel_traces
WHERE parent_span_id = ''
GROUP BY service_name, status_code, minute;

SELECT service_name,
       sum(requests) AS requests
FROM tracing.red_by_service
WHERE minute >= toStartOfMinute(
        now() - INTERVAL 1 HOUR)
GROUP BY service_name;
-- ---- end listing 8.3 ----
