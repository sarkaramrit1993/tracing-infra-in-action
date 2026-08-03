# Tenancy: what a row policy stops, and what it does not

Run this from `chapter-07/`. It does not depend on the other two exercises. It
does leave one thing behind if you skip the cleanup, and that one thing will
break everything you do next, so read the cleanup section before you start.

## The question

One table, several customers, and the rule that customer A must never see
customer B's spans. Listing 7.4 enforces that with a ClickHouse row policy: a
predicate the server welds onto every SELECT against the table.

The interesting question is not whether it works. It does. The question is what
"every SELECT" leaves out.

## The starting state

Reset first. The policy and the demo rows may already be there from an earlier
run, from `tests/test_stack.sh`, or from a run of this file that stopped part
way. Dropping the policy has to come first, because while it exists the admin
login is filtered too.

```bash
ch()      { docker compose exec -T clickhouse clickhouse-client "$@" < /dev/null; }
ch_file() { docker compose exec -T clickhouse clickhouse-client --multiquery < "$1"; }
```

```bash
ch --query "DROP ROW POLICY IF EXISTS tenant_filter ON tracing.otel_traces"
ch --query "DROP ROW POLICY IF EXISTS audit_read ON tracing.otel_traces"
ch --query "
ALTER TABLE tracing.otel_traces
DELETE WHERE trace_id IN ('aaaa0000aaaa0000aaaa0000aaaa0000',
                          'bbbb0000bbbb0000bbbb0000bbbb0000',
                          'deadbeefdeadbeefdeadbeefdeadbeef',
                          'cafe0000cafe0000cafe0000cafe0000')
SETTINGS mutations_sync = 2"
```

`audit_read` is a second policy one of the variations below creates, dropped
here in case a run stopped part way. Those four trace ids are the rows this
exercise creates. Now apply listing 7.4:

```bash
ch_file clickhouse/tenancy.sql
```

That does four things: adds a `tenant_id` column, creates the `tenant_filter`
policy, creates two users named `tenant_a` and `tenant_b`, and seeds one obvious
row for each.

```bash
ch --query "
SELECT short_name, database, table, select_filter, apply_to_all
FROM system.row_policies FORMAT Vertical"
```

The filter reads `tenant_id = currentUser()`. `currentUser()` is the connected
SQL username, which is why the demo names its users after its tenants. A real
deployment maps an authenticated principal to a tenant claim instead. The
mechanics are identical.

## Read as a tenant

```bash
ch --user tenant_a --query "
SELECT tenant_id, count() FROM tracing.otel_traces GROUP BY tenant_id"
ch --user tenant_a --query "
SELECT count() FROM tracing.otel_traces WHERE tenant_id = 'tenant_b'"
```

The first returns one group, `tenant_a`. The second returns `0`. Asking directly
for another tenant's rows does not get you an error, a permission denial, or an
empty-set warning. It gets you a truthful answer to a question the server
rewrote before it ran. `tenant_a` cannot tell the difference between "tenant_b
has no rows" and "tenant_b is none of your business", and that is the point.

`tenant_a` probably shows a large count, far more than the one row
`tenancy.sql` seeded. That is worth a look. The `tenant_id` column was added to a
table that already held data, with `DEFAULT 'tenant_a'`, so every span the
collector had already written is now tagged `tenant_a`. Adding a tenant column to
a live table silently assigns all of history to whichever tenant the default
names. Backfilling the real value is a migration, not a DDL statement.

The symmetric check:

```bash
ch --user tenant_b --query "
SELECT tenant_id, count() FROM tracing.otel_traces GROUP BY tenant_id"
```

One row, tagged `tenant_b`.

## Now look at the admin

```bash
ch --query "SELECT count() FROM tracing.otel_traces"
```

`0`.

Listing 7.4 says `TO ALL`, and `default` is in ALL. The username `default` is not
a tenant_id, so the predicate `tenant_id = currentUser()` matches nothing and the
admin login sees an empty table. Everything else you might do against this stack
right now, every count, every benchmark, every other exercise, reads zero rows
and looks broken.

This is not a quirk of the demo, it is the safe default. A policy that exempts
some accounts is only as strong as the list of accounts nobody exempted by
mistake. `TO ALL` has no such list. The price is that operators need a way back
in, and here that way back in is the cleanup at the bottom of this file.

## The gap: the policy gates reads

Section 7.5.2 warns about this and it is easy to miss, because the read side
works so convincingly.

A writer holding INSERT rights tags a row with somebody else's tenant_id:

```bash
ch --query "
INSERT INTO tracing.otel_traces
  (timestamp, trace_id, tenant_id, span_id, service_name, span_name,
   status_code, duration_ns, attributes)
VALUES (now64(9), 'deadbeefdeadbeefdeadbeefdeadbeef', 'tenant_b', 'deadbeef',
        'checkout-service', 'validate_cart', 'STATUS_CODE_OK', 1000000,
        {'injected':'true'})"
```

```bash
ch --user tenant_b --query "
SELECT tenant_id, span_id, attributes['injected'] AS injected
FROM tracing.otel_traces WHERE trace_id = 'deadbeefdeadbeefdeadbeefdeadbeef'"
```

```
tenant_b   deadbeef   true
```

The write went through and `tenant_b` is now reading a span it never produced.
The policy rewrites SELECT. It does not rewrite INSERT, and it does not gate
`ALTER ... DELETE` or `DROP PARTITION` either.

So the isolation boundary is not where it looks. A shared-table deployment has to
bind `tenant_id` to the authenticated principal at the ingest boundary, before
the row is written, and never trust the value that arrived on the wire. The row
policy is the second lock, not the first. `tests/test_tenancy.sh` asserts exactly
this, so a regression fails a test instead of quietly leaking.

## Try this

**Take `default` out of the policy.** Drop `tenant_filter` and recreate it aimed
at the two tenants only:

```bash
ch --query "DROP ROW POLICY IF EXISTS tenant_filter ON tracing.otel_traces"
ch --query "
CREATE ROW POLICY tenant_filter ON tracing.otel_traces
USING tenant_id = currentUser() TO tenant_a, tenant_b"
ch --query "SELECT count() FROM tracing.otel_traces"
ch --user tenant_a --query "SELECT count() FROM tracing.otel_traces WHERE tenant_id = 'tenant_b'"
ch --query "DROP ROW POLICY tenant_filter ON tracing.otel_traces"
ch --query "
CREATE ROW POLICY tenant_filter ON tracing.otel_traces
USING tenant_id = currentUser() TO ALL"
```

A non-zero count for the admin and `0` for the cross-tenant read: the admin can
see the whole table again and tenant isolation still holds. That is a real
deployment shape and it is more comfortable to operate. It is also the version
where anyone who talks their way into an unnamed account reads everything.
`TO ALL` fails closed for identities nobody thought about. This one fails open.
The last two commands put listing 7.4's own policy back.

**Add a second policy and watch it widen, not narrow.**

```bash
ch --query "
CREATE ROW POLICY audit_read ON tracing.otel_traces
USING tenant_id = 'tenant_b' TO tenant_a"
ch --user tenant_a --query "SELECT count() FROM tracing.otel_traces WHERE tenant_id = 'tenant_b'"
ch --query "DROP ROW POLICY audit_read ON tracing.otel_traces"
ch --user tenant_a --query "SELECT count() FROM tracing.otel_traces WHERE tenant_id = 'tenant_b'"
```

A count above zero, then `0` once the second policy is gone. `tenant_a` read
`tenant_b` because permissive policies on the same table are combined with OR, so
adding one can only ever grant more. If your mental model was "another rule,
another restriction", this is the sort of thing that ships a data leak.

**Do the mislabeled insert as a tenant instead of as the admin.** The gap above
used the `default` login, which invites the excuse that the admin can do anything
anyway. Give `tenant_a` write rights and let it try:

```bash
ch --query "GRANT INSERT ON tracing.* TO tenant_a"
ch --user tenant_a --query "
INSERT INTO tracing.otel_traces
  (timestamp, trace_id, tenant_id, span_id, service_name, span_name,
   status_code, duration_ns, attributes)
VALUES (now64(9), 'cafe0000cafe0000cafe0000cafe0000', 'tenant_b', 'cafe1111',
        'checkout-service', 'validate_cart', 'STATUS_CODE_OK', 1000000,
        {'from':'tenant_a'})"
ch --user tenant_b --query "SELECT count() FROM tracing.otel_traces WHERE trace_id = 'cafe0000cafe0000cafe0000cafe0000'"
ch --user tenant_a --query "SELECT count() FROM tracing.otel_traces WHERE trace_id = 'cafe0000cafe0000cafe0000cafe0000'"
```

`tenant_b` reads `1`. `tenant_a` reads `0`. A tenant wrote a row into another
tenant's view and then could not see what it had done, because the read side of
the policy applies to the writer too. That asymmetry is the whole lesson in two
queries.

## Clean up

Do this. The `tenant_filter` policy is `TO ALL`, so leaving it behind means the
`default` admin sees an empty table for everything you run next, and nothing
tells you why.

```bash
ch --query "DROP ROW POLICY IF EXISTS tenant_filter ON tracing.otel_traces"
ch --query "DROP ROW POLICY IF EXISTS audit_read ON tracing.otel_traces"
ch --query "
ALTER TABLE tracing.otel_traces
DELETE WHERE trace_id IN ('aaaa0000aaaa0000aaaa0000aaaa0000',
                          'bbbb0000bbbb0000bbbb0000bbbb0000',
                          'deadbeefdeadbeefdeadbeefdeadbeef',
                          'cafe0000cafe0000cafe0000cafe0000')
SETTINGS mutations_sync = 2"
ch --query "DROP USER IF EXISTS tenant_a, tenant_b"
ch --query "ALTER TABLE tracing.otel_traces DROP COLUMN IF EXISTS tenant_id"
```

The users go too. Without the policy they are passwordless logins with SELECT on
the whole `tracing` database, which is a worse thing to forget about than the
policy. Confirm you are back to listing 7.1:

```bash
ch --query "SHOW ROW POLICIES"
ch --query "SELECT count() FROM tracing.otel_traces"
ch --query "
SELECT name FROM system.columns
WHERE database = 'tracing' AND table = 'otel_traces' ORDER BY position"
```

No policies, a non-zero count, and nine columns with no `tenant_id` among them.

## Going deeper

[NOTES.md](../NOTES.md) explains why the policy has to be dropped again, why
`MODIFY ORDER BY` cannot move `tenant_id` into an existing table's sort key, and
what `currentUser()` maps to in a real deployment.

`tests/test_tenancy.sh` runs the read isolation and the ingest gap as assertions
and cleans up after itself the same way this file does.
`benchmarks/tenant_cardinality_blowup.py` takes the other half of section 7.5.2,
where one tenant's unique-per-span attribute wrecks the shared column's
compression for everybody.
