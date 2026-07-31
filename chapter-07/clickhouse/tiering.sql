-- Chapter 7: hot-to-cold tiering, listing 7.2.
--
-- Run this against tracing.otel_traces AFTER init.sql has created the table.
-- It moves parts older than two days to the 'cold' volume and deletes them past
-- the 15-day window. The 'cold' volume is defined in config.d/storage.xml.
--
--   docker compose exec -T clickhouse clickhouse-client --multiquery < clickhouse/tiering.sql
--
-- The 'cold' volume is backed by a real S3 disk (the s3_cold disk in
-- config.d/storage.xml) that points at the MinIO service in docker-compose.yml.
-- When a part moves to cold, its system.parts.disk_name flips from 'default' to
-- 's3_cold' and the data is written as S3 objects into the 'traces-cold' bucket.
-- Swapping MinIO for AWS S3, GCS, or Azure Blob is an endpoint and credential
-- change in storage.xml; the ALTER statements below stay identical.

-- ---- Listing 7.2: A ClickHouse hot-to-cold tiering policy
ALTER TABLE tracing.otel_traces
  MODIFY TTL
    toDateTime(timestamp) + INTERVAL 2 DAY TO VOLUME 'cold',
    toDateTime(timestamp) + INTERVAL 15 DAY DELETE;

ALTER TABLE tracing.otel_traces
  DROP PARTITION '20260601';
-- ---- end listing 7.2

-- The DROP PARTITION above targets the literal date 20260601 from listing 7.2;
-- on a fresh demo no such partition exists, so ClickHouse no-ops it
-- (DROP PARTITION of an absent partition is a silent success, which is itself
-- the metadata-time-retention point listing 7.2 makes). The README walkthrough
-- shows how to drop a partition that actually holds data, so you can watch it
-- vanish instantly from system.parts.
