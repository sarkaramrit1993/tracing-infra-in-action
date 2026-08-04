# Tenancy: what a row policy stops, and what it does not

Run this from `chapter-07/`. It does not depend on the other two exercises, and
nothing it creates gets in the way of them. Do the cleanup at the end anyway: it
leaves two passwordless logins behind otherwise.

## The question

One table, several customers, and the rule that customer A must never see
customer B's spans. Listing 7.4 enforces that with a ClickHouse row policy: a
predicate the server welds onto every SELECT against the table.

The interesting question is not whether it works. It does. The questions are what
"every SELECT" leaves out, and who "every" means.

## The starting state

Reset first. The demo rows may already be there from an earlier run, from
`tests/test_stack.sh`, or from a run of this file that stopped part way, and one
of the variations below creates a second policy that would widen what you are
about to measure.

```bash
ch()      { docker compose exec -T clickhouse clickhouse-client "$@" < /dev/null; }
ch_file() { docker compose exec -T clickhouse clickhouse-client --multiquery < "$1"; }
```

```bash
ch --query "DROP ROW POLICY IF EXISTS audit_read ON tracing.otel_traces"
ch --query "
ALTER TABLE tracing.otel_traces
DELETE WHERE trace_id IN ('aaaa0000aaaa0000aaaa0000aaaa0000',
                          'bbbb0000bbbb0000bbbb0000bbbb0000',
                          'deadbeefdeadbeefdeadbeefdeadbeef',
                          'cafe0000cafe0000cafe0000cafe0000')
SETTINGS mutations_sync = 2"
```

Those four trace ids are the rows this exercise creates. Now apply listing 7.4:

```bash
ch_file clickhouse/tenancy.sql
```

That does five things: adds a `tenant_id` column, creates and seeds the
`tenant_users` map, creates the `tenant_filter` policy, creates two logins, and
seeds one obvious row for each tenant.

```bash
ch --query "
SELECT short_name, database, table, select_filter, apply_to_all, apply_to_except
FROM system.row_policies FORMAT Vertical"
ch --query "SELECT * FROM tracing.tenant_users ORDER BY user_name"
```

The filter is a subquery, not a comparison:

```
tenant_id IN (SELECT tenant_id FROM tracing.tenant_users
              WHERE user_name = currentUser())
```

`currentUser()` is the connected SQL login. `acme_reader` and `globex_reader`
are logins. `tenant_a` and `tenant_b` are customers. The map is the only thing
that joins the two, and that is annotation #C's point. Name the login after the
tenant and the map collapses into an identity function you could delete without
anyone noticing, right up until one customer needs two people.

## Read as a tenant

```bash
ch --user acme_reader --query "
SELECT tenant_id, count() FROM tracing.otel_traces GROUP BY tenant_id"
ch --user acme_reader --query "
SELECT count() FROM tracing.otel_traces WHERE tenant_id = 'tenant_b'"
```

The first returns one group, `tenant_a`. The second returns `0`. Asking directly
for another tenant's rows does not get you an error, a permission denial, or an
empty-set warning. It gets you a truthful answer to a question the server
rewrote before it ran. `acme_reader` cannot tell the difference between "tenant_b
has no rows" and "tenant_b is none of your business", and that is the point.

`acme_reader` probably shows a large count, far more than the one row
`tenancy.sql` seeded. That is worth a look. The `tenant_id` column was added to a
table that already held data, with `DEFAULT 'tenant_a'`, so every span the
collector had already written is now tagged `tenant_a`. Adding a tenant column to
a live table silently assigns all of history to whichever tenant the default
names. Backfilling the real value is a migration, not a DDL statement.

The symmetric check:

```bash
ch --user globex_reader --query "
SELECT tenant_id, count() FROM tracing.otel_traces GROUP BY tenant_id"
```

One row, tagged `tenant_b`.

## Now look at the operator

```bash
ch --query "SELECT count() FROM tracing.otel_traces"
```

Every row, both tenants included. `default` is in the policy's exempt list, so
the server does not rewrite its SELECTs at all.

That is annotation #D. `default` is not a tenant id, so a policy that filtered it
would match nothing and hand the operator an empty table for every count, every
benchmark and both other exercises, with nothing in the output to say why. The
book writes `TO ALL EXCEPT admin, ingest`: an operator and a writer. This stack
has one login that is both.

## Who a policy applies to

The `TO` clause does not say who is allowed in. It says who gets filtered. Three
shapes, and they are not variations on one idea.

`TO ALL EXCEPT default` is listing 7.4's. Everyone is filtered except the logins
you name. A login nobody thought about is filtered, its lookup in `tenant_users`
finds nothing, and it reads an empty table. The exemptions are a short list you
can read in one line and grep for in review.

`TO acme_reader, globex_reader` reads like the same exemption written the other
way round. It is not. Only the logins you name are filtered. A login nobody
thought about is not filtered at all, so it reads every tenant's rows.

`TO ALL` filters everyone, operators included. Nothing leaks, and nothing works
either, which is how a policy ends up dropped "temporarily" one afternoon.

The two exempting forms both leave your operator unfiltered. Only one of them
leaves the login you forgot about unfiltered too. `TO ALL EXCEPT <list>` fails
closed on an unknown identity. `TO <list>` fails open on one. The first variation
below runs both and shows the difference in two counts.

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
ch --user globex_reader --query "
SELECT tenant_id, span_id, attributes['injected'] AS injected
FROM tracing.otel_traces WHERE trace_id = 'deadbeefdeadbeefdeadbeefdeadbeef'"
```

```
tenant_b   deadbeef   true
```

The write went through and `globex_reader` is now reading a span its customer
never produced. The policy rewrites SELECT. It does not rewrite INSERT, and it
does not gate `ALTER ... DELETE` or `DROP PARTITION` either.

So the isolation boundary is not where it looks. A shared-table deployment has to
bind `tenant_id` to the authenticated principal at the ingest boundary, before
the row is written, and never trust the value that arrived on the wire. The row
policy is the second lock, not the first. `tests/test_tenancy.sh` asserts exactly
this, so a regression fails a test instead of quietly leaking.

## Try this

**Add a login the map has never heard of.** This is the difference between the
two exempting forms, in two counts.

```bash
ch --query "CREATE USER IF NOT EXISTS newhire IDENTIFIED WITH no_password"
ch --query "GRANT SELECT ON tracing.otel_traces TO newhire"
ch --query "GRANT SELECT ON tracing.tenant_users TO newhire"
ch --user newhire --query "SELECT count() FROM tracing.otel_traces"
```

`0`. `newhire` has SELECT rights and no row in `tenant_users`, so listing 7.4
filters it and the filter matches nothing. Now write the policy the other way
round, naming the tenants instead of exempting the operator:

```bash
ch --query "
CREATE ROW POLICY OR REPLACE tenant_filter ON tracing.otel_traces
USING tenant_id IN (SELECT tenant_id FROM tracing.tenant_users
                    WHERE user_name = currentUser())
TO acme_reader, globex_reader"
ch --user newhire --query "SELECT count() FROM tracing.otel_traces"
ch --user acme_reader --query "SELECT count() FROM tracing.otel_traces WHERE tenant_id = 'tenant_b'"
```

The whole table, then `0`. Tenant isolation still holds for the two logins the
policy names. `newhire` reads everything, because a policy that names its targets
has nothing to say about anyone else. Nobody edited the policy to allow this. The
account simply appeared after the policy was written, which is how accounts
usually appear.

Put listing 7.4's policy back, then admit `newhire` the way the map intends:

```bash
ch --query "
CREATE ROW POLICY OR REPLACE tenant_filter ON tracing.otel_traces
USING tenant_id IN (SELECT tenant_id FROM tracing.tenant_users
                    WHERE user_name = currentUser())
TO ALL EXCEPT default"
ch --user newhire --query "SELECT count() FROM tracing.otel_traces"
ch --query "INSERT INTO tracing.tenant_users (user_name, tenant_id) VALUES ('newhire', 'tenant_a')"
ch --user newhire --query "SELECT tenant_id, count() FROM tracing.otel_traces GROUP BY tenant_id"
```

`0`, then Acme's rows and only Acme's. Two logins now hold one tenant, and the
policy never changed. That is the indirection earning its keep: access is data,
reviewed and revoked like data, not DDL.

**Add a second policy and watch it widen, not narrow.**

```bash
ch --query "
CREATE ROW POLICY audit_read ON tracing.otel_traces
USING tenant_id = 'tenant_b' TO acme_reader"
ch --user acme_reader --query "SELECT count() FROM tracing.otel_traces WHERE tenant_id = 'tenant_b'"
ch --query "DROP ROW POLICY audit_read ON tracing.otel_traces"
ch --user acme_reader --query "SELECT count() FROM tracing.otel_traces WHERE tenant_id = 'tenant_b'"
```

A count above zero, then `0` once the second policy is gone. `acme_reader` read
Globex's rows because permissive policies on the same table are combined with OR,
so adding one can only ever grant more. If your mental model was "another rule,
another restriction", this is the sort of thing that ships a data leak.

**Do the mislabeled insert as a tenant instead of as the operator.** The gap
above used the `default` login, which invites the excuse that the operator can do
anything anyway. Give `acme_reader` write rights and let it try:

```bash
ch --query "GRANT INSERT ON tracing.* TO acme_reader"
ch --user acme_reader --query "
INSERT INTO tracing.otel_traces
  (timestamp, trace_id, tenant_id, span_id, service_name, span_name,
   status_code, duration_ns, attributes)
VALUES (now64(9), 'cafe0000cafe0000cafe0000cafe0000', 'tenant_b', 'cafe1111',
        'checkout-service', 'validate_cart', 'STATUS_CODE_OK', 1000000,
        {'from':'acme_reader'})"
ch --user globex_reader --query "SELECT count() FROM tracing.otel_traces WHERE trace_id = 'cafe0000cafe0000cafe0000cafe0000'"
ch --user acme_reader --query "SELECT count() FROM tracing.otel_traces WHERE trace_id = 'cafe0000cafe0000cafe0000cafe0000'"
```

`globex_reader` reads `1`. `acme_reader` reads `0`. One tenant wrote a row into
another tenant's view and then could not see what it had done, because the read
side of the policy applies to the writer too. That asymmetry is the whole lesson
in two queries.

## Clean up

Nothing here breaks the rest of the stack if you skip it. The `default` login is
exempt from `tenant_filter`, so the walkthrough, the benchmarks and the other two
exercises read the full table whether or not the policy is still in place. What
you would be leaving behind is three passwordless logins holding SELECT, one of
them with INSERT, which is worth more care than a policy.

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
ch --query "DROP USER IF EXISTS acme_reader, globex_reader, newhire"
ch --query "DROP TABLE IF EXISTS tracing.tenant_users"
ch --query "ALTER TABLE tracing.otel_traces DROP COLUMN IF EXISTS tenant_id"
```

Confirm you are back to listing 7.1:

```bash
ch --query "SHOW ROW POLICIES"
ch --query "SELECT count() FROM tracing.otel_traces"
ch --query "
SELECT name FROM system.columns
WHERE database = 'tracing' AND table = 'otel_traces' ORDER BY position"
```

No policies, a non-zero count, and nine columns with no `tenant_id` among them.

## Going deeper

[NOTES.md](../NOTES.md) explains why the exempt list names `default` rather than
the book's `admin, ingest`, why the tenant logins need SELECT on the map for the
policy to work at all, and why `MODIFY ORDER BY` cannot move `tenant_id` into an
existing table's sort key.

`tests/test_tenancy.sh` runs the read isolation and the ingest gap as assertions
and cleans up after itself the same way this file does.
`benchmarks/tenant_cardinality_blowup.py` takes the other half of section 7.5.2,
where one tenant's unique-per-span attribute wrecks the shared column's
compression for everybody.
