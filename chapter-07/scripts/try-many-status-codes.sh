#!/usr/bin/env bash
# Loads the same 200,000 spans into a copy of compress_listing, with one change:
# status_code takes 50,000 distinct values instead of two. Then compares that
# one column across the two tables.
#
# Usage: ./scripts/try-many-status-codes.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_compression_loaded
ch --query "DROP TABLE IF EXISTS tracing.compress_many_status"
ch --query "CREATE TABLE tracing.compress_many_status AS tracing.compress_listing"

ch --query "
INSERT INTO tracing.compress_many_status
  (timestamp, trace_id, span_id, service_name, span_name,
   status_code, duration_ns, adjusted_count, attributes)
SELECT
  toDateTime64('2026-01-01 00:00:00', 9) + toIntervalMillisecond(number),
  lower(hex(MD5(toString(intDiv(number, 6))))),
  lower(hex(reinterpretAsFixedString(toUInt64(number)))),
  ['checkout-service', 'inventory-service', 'payment-service',
   'fraud-service', 'notification-service'][(number % 5) + 1],
  ['validate_cart', 'inventory.reserve', 'payment.charge', 'fraud.score',
   'order.create', 'notification.send', 'db.query', 'cache.get',
   'http.request', 'grpc.call'][(number % 10) + 1],
  concat('STATUS_', toString(number % 50000)),
  toUInt64(1000000 + (number * 2654435761) % 200000000),
  multiIf(intDiv(number, 6) % 100 < 80, 1.0,
          intDiv(number, 6) % 100 < 98, 10.0, 100.0),
  map('http.method', ['GET', 'POST', 'PUT'][(number % 3) + 1],
      'k8s.pod.name', concat('pod-', toString(number % 32)))
FROM numbers(200000)"
ch --query "OPTIMIZE TABLE tracing.compress_many_status FINAL"

ch_table "
SELECT table, uniqExact(status_code) AS distinct_values,
       formatReadableSize(any(bytes)) AS status_code_on_disk
FROM (
  SELECT 'compress_listing' AS table, status_code FROM tracing.compress_listing
  UNION ALL
  SELECT 'compress_many_status', status_code FROM tracing.compress_many_status
) AS spans
JOIN (
  SELECT table, data_compressed_bytes AS bytes FROM system.columns
  WHERE database = 'tracing' AND name = 'status_code'
) AS cols USING (table)
GROUP BY table ORDER BY table"
