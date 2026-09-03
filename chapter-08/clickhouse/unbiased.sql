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
--
-- The weight in the fourth query is rounded before the cast, not after, and the
-- order is the whole point. quantileExactWeighted() takes an integer weight and
-- a bare toUInt64() truncates rather than rounds. Every weight this generator
-- produces is a whole number, 100, 2 or 1, so nothing here moves either way and
-- the hazard is invisible in the output. It shows up the moment a keep rate does
-- not divide: a limiter admitting 37 of 500 requests gives a weight of 13.5135,
-- a bare cast makes that 13, and the row stands for 3.8 percent fewer requests
-- than it should. The loss only runs one way, because truncation only ever goes
-- down, so a query applying the sampling rule correctly still reads low. Change
-- the rates in generate/generate.py to something that does not divide and the
-- gap between round() and no round() is measurable in the weighted p99.

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
