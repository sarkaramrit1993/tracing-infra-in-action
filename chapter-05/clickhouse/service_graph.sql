-- Chapter 5: the service graph derived from the spans table by a self-join.
-- Each child span is joined to its parent on (trace_id, parent_span_id), and
-- every cross-service pair counts as one call. Pairs inside one service are
-- internal work and are filtered out. Run by scripts/show-service-graph.sh.

-- ---- Listing 5.6: Service graph derivation from the spans table
SELECT
    parent_service,
    child_service,
    count() AS call_count,
    quantileTDigest(0.99)(duration) AS p99_duration_ns,
    countIf(status_code = 'STATUS_CODE_ERROR') AS error_count
FROM (
    SELECT
        s.service_name AS child_service,
        p.service_name AS parent_service,
        s.duration,
        s.status_code
    FROM tracing.otel_traces AS s
    INNER JOIN tracing.otel_traces AS p
        ON s.trace_id = p.trace_id
       AND s.parent_span_id = p.span_id
    WHERE s.timestamp >= now() - INTERVAL 1 HOUR
      AND p.timestamp >= now() - INTERVAL 2 HOUR
)
WHERE parent_service != child_service
GROUP BY parent_service, child_service
ORDER BY call_count DESC
-- ---- end listing 5.6
