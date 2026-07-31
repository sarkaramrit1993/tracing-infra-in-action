-- Chapter 7, listing 7.3: verify per-column compression on tracing.otel_traces. Run: docker compose exec -T clickhouse clickhouse-client --multiquery < clickhouse/compression.sql

SELECT
    name AS column,
    formatReadableSize(sum(data_compressed_bytes))   AS stored,
    formatReadableSize(sum(data_uncompressed_bytes)) AS raw,
    round(sum(data_uncompressed_bytes)
          / sum(data_compressed_bytes), 1)           AS ratio
FROM system.columns
WHERE table = 'otel_traces' AND database = 'tracing'
GROUP BY name
ORDER BY ratio DESC;
