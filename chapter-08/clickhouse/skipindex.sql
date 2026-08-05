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
--
-- The trace ID below is the W3C specification's example, and it is deliberately
-- NOT in the generated data. That is what makes the reading clean: a bloom
-- filter's guarantee is that it never misses a block that could match, so an ID
-- the table has never seen prunes every surviving granule and the second EXPLAIN
-- reports 0. Looking up an ID that IS present prunes less, because a bloom can
-- only narrow to the blocks that might hold it. Trace IDs here are MD5 of the
-- trace's index, so lower(hex(MD5('0'))) is a real one if you want that reading
-- too; it returns seven spans.
--
-- The DROP below is outside the listing. ADD INDEX fails if the index already
-- exists, and TRUNCATE does not remove index definitions, so without this a
-- second run of the file would report a "before" that already has the index
-- pruning, which is the one thing this listing exists to show.
ALTER TABLE tracing.otel_traces DROP INDEX IF EXISTS idx_trace_id;

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
