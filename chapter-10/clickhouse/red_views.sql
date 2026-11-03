-- Chapter 10: the RED target table and Listing 10.3's materialized view.
-- Applied automatically on first boot, after init.sql has created
-- tracing.otel_traces.

-- The target must exist before the view that writes to it.
CREATE TABLE IF NOT EXISTS red_by_service
(
    service        LowCardinality(String),
    operation      LowCardinality(String),
    status         LowCardinality(String),
    minute         DateTime,
    calls          AggregateFunction(count),
    weighted_calls AggregateFunction(sum, Float64),
    p99_state      AggregateFunction(quantileTDigestWeighted(0.99), Float64, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMMDD(minute)
ORDER BY (service, operation, status, minute);

-- ---- Listing 10.3: A ClickHouse insert-time materialized view for RED ----

CREATE MATERIALIZED VIEW red_by_service_mv
TO red_by_service AS
SELECT
  service_name AS service,
  span_name AS operation,
  status_code AS status,
  toStartOfMinute(timestamp) AS minute,
  countState() AS calls,
  sumState(adjusted_count) AS weighted_calls,
  quantileTDigestWeightedState(0.99)(duration_ns / 1e6,
      toUInt64(round(adjusted_count))) AS p99_state
FROM tracing.otel_traces
GROUP BY service, operation, status, minute;
-- ---- end listing 10.3 ----
