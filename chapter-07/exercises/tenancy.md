# Tenancy: what a row policy stops, and what it does not

Run this from `chapter-07/`. It does not depend on the other two exercises, and
nothing it creates gets in the way of them. Do the cleanup at the end anyway:
otherwise it leaves passwordless logins behind.

## The question

One table, several customers, and the rule that customer A must never see
customer B's spans. Listing 7.4 enforces that with a ClickHouse row policy: a
predicate the server welds onto every SELECT against the table.

The interesting question is not whether it works. It does. The questions are what
"every SELECT" leaves out, and who "every" means.

## The starting state

Every step below runs a script from `scripts/`, and the SQL it runs is shown
under it. The stack has to be up:

```bash
docker compose up -d --build --wait
```

Now apply listing 7.4. The script first clears whatever an earlier run of this
file or `tests/test_stack.sh` left behind: both policies, the demo rows and the
tenant logins. Then it applies `clickhouse/tenancy.sql` and prints what that
created:

```bash
./scripts/apply-tenancy.sh
```

```
Row 1:
──────
short_name:      tenant_filter
database:        tracing
table:           otel_traces
select_filter:   tenant_id IN (SELECT tenant_id FROM tracing.tenant_users WHERE user_name = currentUser())
apply_to_all:    1
apply_to_except: ['default']

user_name      tenant_id
acme_reader    tenant_a
globex_reader  tenant_b
```

`tenancy.sql` does five things: adds a `tenant_id` column, creates and seeds
the `tenant_users` map, creates the `tenant_filter` policy, creates two logins,
and seeds one obvious row for each tenant. The policy is listing 7.4:

```sql
CREATE ROW POLICY OR REPLACE tenant_filter ON tracing.otel_traces
USING tenant_id IN (SELECT tenant_id FROM tracing.tenant_users
                    WHERE user_name = currentUser())
TO ALL EXCEPT default;
```

The filter is a subquery, not a comparison. `currentUser()` is the connected
SQL login. `acme_reader` and `globex_reader` are logins. `tenant_a` and
`tenant_b` are customers. The map is the only thing that joins the two, and that
is annotation #C's point. Name the login after the tenant and the map collapses
into an identity function you could delete without anyone noticing, right up
until one customer needs two people.

## Read as a tenant

```bash
./scripts/read-as-acme.sh
```

```
tenant_id  rows
tenant_a   2116

tenant_b_rows
            0
```

The first query returns one group, `tenant_a`. The second asks directly for the
other tenant's rows and gets `0`. Not an error, not a permission denial, not an
empty-set warning: a truthful answer to a question the server rewrote before it
ran. `acme_reader` cannot tell "tenant_b has no rows" from "tenant_b is none of
your business", and that is the point. What it runs, as `acme_reader`:

```sql
SELECT tenant_id, count() AS rows FROM tracing.otel_traces GROUP BY tenant_id
```

```sql
SELECT count() AS tenant_b_rows FROM tracing.otel_traces WHERE tenant_id = 'tenant_b'
```

`tenant_a` probably shows far more than the one row `tenancy.sql` seeded. The
`tenant_id` column was added to a table that already held data, with
`DEFAULT 'tenant_a'`, so every span the Collector had already written is now
tagged `tenant_a`. Adding a tenant column to a live table silently assigns all of
history to whichever tenant the default names. Backfilling the real value is a
migration, not a DDL statement.

The symmetric check:

```bash
./scripts/read-as-globex.sh
```

```
tenant_id  rows
tenant_b      1
```

One row, tagged `tenant_b`. Same query, as `globex_reader`.

## Now look at the operator

```bash
./scripts/read-as-operator.sh
```

```
tenant_id  rows
tenant_a   2116
tenant_b      1
```

Every row, both tenants. `default` is in the policy's exempt list, so the server
does not rewrite its SELECTs at all.

That is annotation #D. `default` is not a tenant id, so a policy that filtered it
would match nothing and hand the operator an empty table for every count, every
benchmark and both other exercises, with nothing in the output to say why. The
book writes `TO ALL EXCEPT admin, ingest`: an operator and a writer. This stack
has one login that is both. What it runs, as `default`:

```sql
SELECT tenant_id, count() AS rows FROM tracing.otel_traces GROUP BY tenant_id ORDER BY tenant_id
```

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

`TO ALL EXCEPT <list>` fails closed on an unknown identity. `TO <list>` fails
open on one. The first variation below shows the difference in two counts.

## The gap: the policy gates reads

Section 7.5.2 warns about this, and it is easy to miss because the read side
works so convincingly. A writer holding INSERT rights tags a row with somebody
else's `tenant_id`, then `globex_reader` reads it back:

```bash
./scripts/insert-mislabeled-row.sh
```

```
tenant_id  span_id   injected
tenant_b   deadbeef  true
```

The write went through and `globex_reader` is now reading a span its customer
never produced. The policy rewrites SELECT. It does not rewrite INSERT, and it
does not gate `ALTER ... DELETE` or `DROP PARTITION` either. What it runs:

```sql
INSERT INTO tracing.otel_traces
  (timestamp, trace_id, tenant_id, span_id, service_name, span_name,
   status_code, duration_ns, attributes)
VALUES (now64(9), 'deadbeefdeadbeefdeadbeefdeadbeef', 'tenant_b', 'deadbeef',
        'checkout-service', 'validate_cart', 'STATUS_CODE_OK', 1000000,
        {'injected':'true'})
```

```sql
SELECT tenant_id, span_id, attributes['injected'] AS injected
FROM tracing.otel_traces WHERE trace_id = 'deadbeefdeadbeefdeadbeefdeadbeef'
```

So the isolation boundary is not where it looks. A shared-table deployment has to
bind `tenant_id` to the authenticated principal at the ingest boundary, before
the row is written, and never trust the value that arrived on the wire. The row
policy is the second lock, not the first. `tests/test_tenancy.sh` asserts exactly
this, so a regression fails a test instead of quietly leaking.

## Try this

**Add a login the map has never heard of.** This is the difference between the
two exempting forms, in two counts.

```bash
./scripts/add-unmapped-login.sh
```

```
newhire_reads
            0
```

`newhire` has SELECT rights and no row in `tenant_users`, so listing 7.4 filters
it and the filter matches nothing. If you run it again later, it first puts
listing 7.4's policy back and takes `newhire` out of the map, so it always
measures this same starting point. What it runs:

```sql
CREATE USER IF NOT EXISTS newhire IDENTIFIED WITH no_password
```

```sql
GRANT SELECT ON tracing.otel_traces TO newhire
```

Now write the policy the other way round, naming the tenants instead of
exempting the operator:

```bash
./scripts/name-the-tenants-instead.sh
```

```
newhire_reads
         2119

acme_reads_of_tenant_b
                     0
```

The whole table, then `0`. Tenant isolation still holds for the two logins the
policy names. `newhire` reads everything, because a policy that names its targets
has nothing to say about anyone else. Nobody edited the policy to allow this. The
account simply appeared after the policy was written, which is how accounts
usually appear. What it runs:

```sql
CREATE ROW POLICY OR REPLACE tenant_filter ON tracing.otel_traces
USING tenant_id IN (SELECT tenant_id FROM tracing.tenant_users
                    WHERE user_name = currentUser())
TO acme_reader, globex_reader
```

Put listing 7.4's policy back:

```bash
./scripts/restore-listing-policy.sh
```

```
newhire_reads
            0
```

Then admit `newhire` the way the map intends, with one row in `tenant_users`:

```bash
./scripts/map-newhire-to-acme.sh
```

```
tenant_id  rows
tenant_a   2117
```

Acme's rows and only Acme's. Two logins now hold one tenant, and the policy
never changed. That is the indirection earning its keep: access is data,
reviewed and revoked like data, not DDL. What it runs:

```sql
INSERT INTO tracing.tenant_users (user_name, tenant_id) VALUES ('newhire', 'tenant_a')
```

**Add a second policy and watch it widen, not narrow.**

```bash
./scripts/add-second-policy.sh
```

```
with_audit_read
              2

without_it
         0
```

`acme_reader` read Globex's rows while the second policy was there, and `0`
once it was gone. Permissive policies on the same table are combined with OR, so
adding one can only ever grant more. If your mental model was "another rule,
another restriction", this is the sort of thing that ships a data leak. What it
runs, before dropping it again:

```sql
CREATE ROW POLICY audit_read ON tracing.otel_traces
USING tenant_id = 'tenant_b' TO acme_reader
```

**Do the mislabeled insert as a tenant instead of as the operator.** The gap
above used the `default` login, which invites the excuse that the operator can
do anything anyway. This gives `acme_reader` write rights and lets it try:

```bash
./scripts/insert-as-tenant.sh
```

```
globex_sees
          1

acme_sees
        0
```

`globex_reader` reads `1`. `acme_reader` reads `0`. One tenant wrote a row into
another tenant's view and then could not see what it had done, because the read
side of the policy applies to the writer too. That asymmetry is the whole lesson
in two queries. What it runs:

```sql
GRANT INSERT ON tracing.* TO acme_reader
```

Then, as `acme_reader`:

```sql
INSERT INTO tracing.otel_traces
  (timestamp, trace_id, tenant_id, span_id, service_name, span_name,
   status_code, duration_ns, attributes)
VALUES (now64(9), 'cafe0000cafe0000cafe0000cafe0000', 'tenant_b', 'cafe1111',
        'checkout-service', 'validate_cart', 'STATUS_CODE_OK', 1000000,
        {'from':'acme_reader'})
```

## Clean up

Nothing here breaks the rest of the stack if you skip it. The `default` login is
exempt from `tenant_filter`, so the walkthrough, the benchmarks and the other two
exercises read the full table whether or not the policy is still in place. What
you would be leaving behind is three passwordless logins holding SELECT, one of
them with INSERT, which deserves more care than a policy.

```bash
./scripts/clean-up-tenancy.sh
```

```
row policies left: 0
rows in otel_traces: 2116
columns: timestamp, trace_id, span_id, service_name, span_name, status_code, duration_ns, adjusted_count, attributes
```

No policies, a non-zero count, and listing 7.1's columns with no `tenant_id`
among them. What it runs:

```sql
DROP ROW POLICY IF EXISTS tenant_filter ON tracing.otel_traces
```

```sql
DROP ROW POLICY IF EXISTS audit_read ON tracing.otel_traces
```

```sql
DROP USER IF EXISTS acme_reader, globex_reader, newhire
```

```sql
DROP TABLE IF EXISTS tracing.tenant_users
```

```sql
ALTER TABLE tracing.otel_traces DROP COLUMN IF EXISTS tenant_id
```

It also deletes the four demo rows by trace id. The two insert scripts delete
their own row before writing it, so running either twice still shows `1`.

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
