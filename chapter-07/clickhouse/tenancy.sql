-- Chapter 7: row-level tenant isolation, listing 7.4.
--
-- Run AFTER init.sql. This makes the shared-table / mandatory-tenant-filter
-- archetype from section 7.5.2 real: a tenant_id column, a table mapping each
-- login to its tenant, and a row policy that rewrites that map's answer onto
-- every SELECT.
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
-- DEMO NOTE on the exempt list. Listing 7.4 writes TO ALL EXCEPT admin, ingest.
-- Those two names stand for your operator and your writer, the accounts that
-- have to read the whole table rather than one tenant's slice. This stack has
-- one privileged login and it is both: `default`. Roles called admin and ingest
-- cannot stand in for it either, because `default` is declared in users.xml and
-- ClickHouse refuses to grant a role to a user held in read-only storage
-- (ACCESS_STORAGE_READONLY, verified on 25.8). So the exempt list below names
-- `default`, and the mapping from the printed line to this one is:
--
--     admin, ingest   ->   default
--
-- Substitute your own operator and writer logins and the line is the book's.

-- The row policy predicate needs tenant_id to exist as a column first.
ALTER TABLE tracing.otel_traces
  ADD COLUMN IF NOT EXISTS tenant_id LowCardinality(String) DEFAULT 'tenant_a'
  CODEC(ZSTD(1)) AFTER trace_id;

-- The login-to-tenant map listing 7.4's policy reads. Logins are named after
-- the people who hold them and tenant ids after the customers they belong to,
-- which is annotation #C's point: a database user is not a tenant id, and this
-- table is the only thing that joins the two. Admitting a new login to a tenant
-- is an INSERT here, not a change to the policy.
CREATE TABLE IF NOT EXISTS tracing.tenant_users
(
    user_name   LowCardinality(String),
    tenant_id   LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY user_name;

TRUNCATE TABLE tracing.tenant_users;

INSERT INTO tracing.tenant_users (user_name, tenant_id) VALUES
  ('acme_reader',   'tenant_a'),
  ('globex_reader', 'tenant_b');

CREATE USER IF NOT EXISTS acme_reader IDENTIFIED WITH no_password;
CREATE USER IF NOT EXISTS globex_reader IDENTIFIED WITH no_password;
GRANT SELECT ON tracing.otel_traces TO acme_reader, globex_reader;

-- ClickHouse evaluates the policy predicate with the querying user's own
-- grants, so a login that cannot read tenant_users cannot run any SELECT
-- against otel_traces at all. The map is therefore readable by the tenants
-- here. A production deployment keeps it out of their reach, behind a
-- dictionary or in a database they hold no grant on.
GRANT SELECT ON tracing.tenant_users TO acme_reader, globex_reader;

-- ---- Listing 7.4: Row-level tenant isolation in ClickHouse ----
CREATE ROW POLICY OR REPLACE tenant_filter ON tracing.otel_traces
  USING tenant_id IN (SELECT tenant_id FROM tracing.tenant_users
                      WHERE user_name = currentUser())
  TO ALL EXCEPT default;
-- ---- end listing 7.4 ----
--
-- OR REPLACE rather than the book's bare CREATE so re-applying this file
-- converges on the definition above instead of leaving an older policy in
-- place. An unmapped login is still filtered, and a filter that matches no
-- tenant returns nothing, so a login nobody remembered to add to tenant_users
-- reads an empty table rather than the whole one.

-- Seed one obvious row per tenant so the README can prove cross-tenant blocking.
INSERT INTO tracing.otel_traces
  (timestamp, trace_id, tenant_id, span_id, service_name, span_name,
   status_code, duration_ns, attributes)
VALUES
  (now64(9), 'aaaa0000aaaa0000aaaa0000aaaa0000', 'tenant_a', 'aaaa1111',
   'checkout-service', 'validate_cart', 'STATUS_CODE_OK', 21000000, {'tenant':'a'}),
  (now64(9), 'bbbb0000bbbb0000bbbb0000bbbb0000', 'tenant_b', 'bbbb1111',
   'checkout-service', 'validate_cart', 'STATUS_CODE_OK', 19000000, {'tenant':'b'});
