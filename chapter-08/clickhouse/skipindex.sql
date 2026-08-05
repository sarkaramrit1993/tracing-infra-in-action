-- Chapter 8, listing 8.2: add a bloom-filter skip index and verify the pruning.
-- Run: docker compose exec -T clickhouse clickhouse-client --multiquery < clickhouse/skipindex.sql
--
-- The table ships without this index on purpose (see clickhouse/init.sql), so
-- the first EXPLAIN below is a real "before" reading and not a formality.
--
-- Read the output as a chain, not as one ratio. EXPLAIN prints a section per
-- step: the primary key first, reporting how many of the table's granules
-- survived the sort-key comparison, and then each skip index below it,
-- reporting how many of THOSE survivors it removed. The bloom filter's
-- denominator is whatever the primary key already left, never the table total.
-- Credit it with the fall between its own two numbers and nothing more.

-- ---- Listing 8.2: Add a bloom-filter skip index and verify the pruning ----
EXPLAIN indexes = 1
SELECT * FROM tracing.otel_traces
WHERE trace_id = '4bf92f3577b34da6a3ce929d0e0e4736';

ALTER TABLE tracing.otel_traces
  ADD INDEX idx_trace_id trace_id
  TYPE bloom_filter(0.01) GRANULARITY 1;

ALTER TABLE tracing.otel_traces
  MATERIALIZE INDEX idx_trace_id
  SETTINGS mutations_sync = 2;

EXPLAIN indexes = 1
SELECT * FROM tracing.otel_traces
WHERE trace_id = '4bf92f3577b34da6a3ce929d0e0e4736';
-- ---- end listing 8.2 ----
