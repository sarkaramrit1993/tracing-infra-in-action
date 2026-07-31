-- Chapter 7: row-level tenant isolation, listing 7.4.
--
-- Run AFTER init.sql. This makes the shared-table / mandatory-tenant-filter
-- archetype from section 7.5.2 real: a tenant_id column and a row policy that
-- rewrites `tenant_id = currentUser()` onto every SELECT.
--
--   docker compose exec -T clickhouse clickhouse-client --multiquery < clickhouse/tenancy.sql
--
-- DEMO NOTE on the sort key. Section 7.5.2 argues for leading the sort key with
-- tenant_id so one tenant's rows sit together on disk. ClickHouse cannot move a
-- column into an EXISTING table's sort key with ALTER ... MODIFY ORDER BY: the
-- primary key must stay a prefix of the sorting key, and MODIFY ORDER BY only
-- accepts columns introduced in the same statement, not a pre-existing column.
-- A tenant-leading layout is therefore set at CREATE time. For a fresh store you
-- would create the listing 7.1 table with:
--
--     ORDER BY (tenant_id, service_name, span_name,
--               toStartOfHour(timestamp), trace_id)
--
-- The row policy below is what enforces isolation, and it works the same whether
-- tenant_id leads the sort key or not. The sort-key prefix is a read-locality
-- optimization, not the security boundary.
--
-- currentUser() returns the connected username, so the demo creates two users
-- named after their tenants (tenant_a, tenant_b). A real deployment maps an
-- authenticated principal to a tenant claim instead of naming the SQL user after
-- the tenant, but the row-policy mechanics are identical.

-- The row policy predicate needs tenant_id to exist as a column first.
ALTER TABLE tracing.otel_traces
  ADD COLUMN IF NOT EXISTS tenant_id LowCardinality(String) DEFAULT 'tenant_a'
  CODEC(ZSTD(1)) AFTER trace_id;

-- ---- Listing 7.4: Row-level tenant isolation in ClickHouse
CREATE ROW POLICY IF NOT EXISTS tenant_filter ON tracing.otel_traces
  USING tenant_id = currentUser()
  TO ALL;
-- ---- end listing 7.4

-- Two demo tenants. Each user can only ever see rows whose tenant_id equals its
-- own username, because the row policy above is TO ALL (applies to every user,
-- including these and the default admin). With TO ALL, even the admin connection
-- sees only rows tagged with its own username unless a broader policy is added.
CREATE USER IF NOT EXISTS tenant_a IDENTIFIED WITH no_password;
CREATE USER IF NOT EXISTS tenant_b IDENTIFIED WITH no_password;
GRANT SELECT ON tracing.* TO tenant_a;
GRANT SELECT ON tracing.* TO tenant_b;

-- Seed one obvious row per tenant so the README can prove cross-tenant blocking.
INSERT INTO tracing.otel_traces
  (timestamp, trace_id, tenant_id, span_id, service_name, span_name,
   status_code, duration_ns, attributes)
VALUES
  (now64(9), 'aaaa0000aaaa0000aaaa0000aaaa0000', 'tenant_a', 'aaaa1111',
   'checkout-service', 'validate_cart', 'STATUS_CODE_OK', 21000000, {'tenant':'a'}),
  (now64(9), 'bbbb0000bbbb0000bbbb0000bbbb0000', 'tenant_b', 'bbbb1111',
   'checkout-service', 'validate_cart', 'STATUS_CODE_OK', 19000000, {'tenant':'b'});
