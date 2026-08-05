-- Chapter 8, listing 8.1: biased and unbiased aggregates over sampled data.
-- Run: docker compose exec -T clickhouse clickhouse-client --multiquery < clickhouse/unbiased.sql
--
-- Four queries, two pairs. Each pair asks the same question twice, once ignoring
-- the sampling weight and once respecting it. Run generate/generate.py first,
-- then compare both answers against tracing.ground_truth, which holds what the
-- generator actually produced before it sampled anything.
--
-- The root-span filter is not decoration. This table stores one row per span and
-- a request is seven of them, so a count without it answers a question about
-- spans while calling the answer "requests".

-- ---- Listing 8.1: Biased and unbiased aggregates over sampled data ----
SELECT service_name, count() AS requests
FROM tracing.otel_traces
WHERE timestamp >= toStartOfMinute(
        now() - INTERVAL 1 HOUR)
  AND parent_span_id = ''
GROUP BY service_name;

SELECT service_name,
       sum(adjusted_count) AS requests
FROM tracing.otel_traces
WHERE timestamp >= toStartOfMinute(
        now() - INTERVAL 1 HOUR)
  AND parent_span_id = ''
GROUP BY service_name;

SELECT service_name,
       round(quantile(0.99)(duration_ns)
             / 1e6, 1) AS p99_ms
FROM tracing.otel_traces
WHERE timestamp >= toStartOfMinute(
        now() - INTERVAL 1 HOUR)
  AND parent_span_id = ''
GROUP BY service_name;

SELECT service_name,
       round(quantileExactWeighted(0.99)(
             duration_ns,
             toUInt64(round(adjusted_count)))
             / 1e6, 1) AS p99_ms
FROM tracing.otel_traces
WHERE timestamp >= toStartOfMinute(
        now() - INTERVAL 1 HOUR)
  AND parent_span_id = ''
GROUP BY service_name;
-- ---- end listing 8.1 ----

-- What the generator produced, for grading the four answers above.
SELECT requests AS true_requests, p99_ms AS true_p99_ms
FROM tracing.ground_truth;
