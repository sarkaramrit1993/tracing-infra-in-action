#!/usr/bin/env bash
# Generates 200,000 spans into compress_listing, copies the same rows into
# compress_plain, merges each table to one part, and checks the rows match.
#
# Usage: ./scripts/load-compression-tables.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_clickhouse
[ "$(ch --query "EXISTS TABLE tracing.compress_plain")" = 1 ] \
  || die "no compression tables yet: run ./scripts/build-compression-tables.sh first"

ch --query "TRUNCATE TABLE tracing.compress_listing"
ch --query "TRUNCATE TABLE tracing.compress_plain"

ch --query "
INSERT INTO tracing.compress_listing
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
  if(number % 20 = 0, 'STATUS_CODE_ERROR', 'STATUS_CODE_OK'),
  toUInt64(1000000 + (number * 2654435761) % 200000000),
  multiIf(intDiv(number, 6) % 100 < 80, 1.0,
          intDiv(number, 6) % 100 < 98, 10.0, 100.0),
  map('http.method', ['GET', 'POST', 'PUT'][(number % 3) + 1],
      'k8s.pod.name', concat('pod-', toString(number % 32)))
FROM numbers(200000)"

ch --query "INSERT INTO tracing.compress_plain SELECT * FROM tracing.compress_listing"
ch --query "OPTIMIZE TABLE tracing.compress_listing FINAL"
ch --query "OPTIMIZE TABLE tracing.compress_plain FINAL"

ch --format Vertical --query "
SELECT
  (SELECT count() FROM tracing.compress_listing) AS listing_rows,
  (SELECT count() FROM tracing.compress_plain)   AS plain_rows,
  (SELECT sum(cityHash64(timestamp, trace_id, span_id, service_name, span_name,
      status_code, duration_ns, adjusted_count, toString(attributes)))
   FROM tracing.compress_listing) AS listing_hash,
  (SELECT sum(cityHash64(timestamp, trace_id, span_id, service_name, span_name,
      status_code, duration_ns, adjusted_count, toString(attributes)))
   FROM tracing.compress_plain)   AS plain_hash"
