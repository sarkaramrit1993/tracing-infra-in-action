-- Chapter 9: Trace-Driven Insights, listing 9.2 (the error-issue index).
--
-- Apply this by hand against the running store, BEFORE driving the traffic you
-- want indexed:
--
--   docker compose exec -T clickhouse clickhouse-client --database tracing \
--     --multiquery < clickhouse/error_index.sql
--
-- Standard materialized-view caveat: the view fires on INSERT and never
-- backfills. Error spans already in otel_traces before this runs are not
-- indexed; only error spans inserted after it are. On a fresh stack that is
-- every error, so arm it right after `docker compose up`.
--
-- The three fingerprint inputs are read out of the span's attributes Map under
-- their OpenTelemetry semantic-convention names: exception.type,
-- exception.message and exception.stacktrace. The application never sets those
-- by hand. It calls record_exception(), which writes them onto a span EVENT,
-- and the Collector's transform processor copies them onto the span itself
-- (collector/gateway-config.yaml). That matters for reading the chapter: a
-- store that keeps exception detail in flat columns reads those columns
-- directly, one that keeps it in a Map reads attributes['...'], and the
-- fingerprint arithmetic is identical either way.

-- ---- Target: one row per distinct issue (fingerprint), folded at merge time -----
-- AggregatingMergeTree keyed by fingerprint. SimpleAggregateFunction columns fold
-- many raw error spans that share a fingerprint into one issue row carrying its
-- running count, first_seen / last_seen window, and one sample trace_id for the
-- drill-down back to the full trace. error_count sums adjusted_count so the issue
-- volume stays sample-weighted (the chapter 8 / section 7.4.4 rule); on live
-- ingest adjusted_count defaults to 1.0, so it is a plain span count there.
CREATE TABLE IF NOT EXISTS tracing.exceptions
(
    fingerprint     UInt64,
    error_type      SimpleAggregateFunction(any, LowCardinality(String)),
    msg_template    SimpleAggregateFunction(any, String),
    top_frame       SimpleAggregateFunction(any, LowCardinality(String)),
    service_name    SimpleAggregateFunction(any, LowCardinality(String)),
    error_count     SimpleAggregateFunction(sum, UInt64),
    first_seen      SimpleAggregateFunction(min, DateTime64(9)),
    last_seen       SimpleAggregateFunction(max, DateTime64(9)),
    sample_trace_id SimpleAggregateFunction(anyLast, String)
)
ENGINE = AggregatingMergeTree
ORDER BY fingerprint;

-- ---- Listing 9.2: An error-issue index as a materialized view -------------------
-- #A The hash of type, normalized template, and top frame is the issue identity
-- #B replaceRegexpAll collapses ids and numbers to one template
-- #C The innermost frame is where it was raised; the line number is dropped so
--    an edit above it does not fork the issue in two
-- #D trace_id preserves the drill-down join back to the full trace
CREATE MATERIALIZED VIEW IF NOT EXISTS tracing.exc_mv TO tracing.exceptions AS
SELECT
    cityHash64(error_type, msg_template, top_frame) AS fingerprint,
    error_type,
    msg_template,
    top_frame,
    service_name,
    toUInt64(adjusted_count) AS error_count,
    timestamp AS first_seen,
    timestamp AS last_seen,
    trace_id  AS sample_trace_id
FROM (
    SELECT
        timestamp,
        trace_id,
        service_name,
        adjusted_count,
        attributes['exception.type'] AS error_type,
        replaceRegexpAll(attributes['exception.message'],
                         '[0-9a-f]{8,}|[0-9]+', '?') AS msg_template,
        replaceRegexpAll(
            arrayElement(
                extractAll(attributes['exception.stacktrace'],
                           'File "[^"]*", line [0-9]+, in [A-Za-z_0-9<>.]+'),
                -1),
            ', line [0-9]+', '') AS top_frame
    FROM tracing.otel_traces
    WHERE status_code = 'STATUS_CODE_ERROR'
);
-- ---- end listing 9.2

-- Read the ranked issue list (what a human triages instead of the error firehose).
-- GROUP BY folds the SimpleAggregateFunction state deterministically whether or not
-- a background merge has run yet, so the counts are correct immediately.
--
--   SELECT fingerprint,
--          any(error_type)      AS error_type,
--          any(msg_template)    AS msg_template,
--          any(top_frame)       AS top_frame,
--          sum(error_count)     AS error_count,
--          min(first_seen)      AS first_seen,
--          max(last_seen)       AS last_seen,
--          any(sample_trace_id) AS sample_trace_id
--   FROM tracing.exceptions
--   GROUP BY fingerprint
--   ORDER BY error_count DESC;
--
-- Expected on this stack: the injected fraud-scoring timeouts all carry the same
-- exception type, the same innermost frame, and a message that differs only in a
-- latency number and a request id, so N raw error spans collapse to ONE
-- fingerprint whose error_count is N. That collapse (millions of spans to tens or
-- hundreds of issues) is the whole point of section 9.2.3.
