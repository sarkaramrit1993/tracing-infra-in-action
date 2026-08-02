# Notes

Why the walkthrough in [README.md](README.md) is built the way it is. None of
this is needed to run the stack. Read a section when a step surprises you, or
when you want to know what the demo is doing underneath.

## Why there are two helpers, `ch` and `ch_file`

Both are defined at the top of "Verify it works".

`clickhouse-client` reads stdin for INSERT data even when the row is already
inline in `VALUES`, and `docker compose exec -T` hands it whatever stdin you
have. If that stdin stays open and never reaches EOF, the INSERT sits there
forever with nothing printed. That is why `ch` always redirects `/dev/null`: a
query it runs can never end up waiting on your keyboard.

But steps 4 and 6 need the opposite. They feed a whole `.sql` file to
`--multiquery`, which means stdin has to carry the file. One helper cannot do
both, and guessing from the shape of stdin gets it wrong somewhere. An earlier
version guessed with `[ -t 0 ]`, which worked at a terminal and hung anywhere
stdin was an open pipe. So the file case gets its own name, `ch_file`, and it is
the only thing in the walkthrough that puts anything on stdin.

The same split is in `tests/test_stack.sh` and `tests/test_tenancy.sh` as `CH`
and `CH_FILE`, for the same reason.

## Why step 2 filters on `GET /checkout`

Step 2 is "A trace round-trips (point lookup by trace_id)".

The container healthcheck writes a one-span `GET /health` trace every 10 seconds,
and those outnumber the checkout traces once the traffic loop finishes. Without
the filter you would usually pick up a one-span healthcheck trace instead of the
seven-span checkout trace.

The lookup itself is answered by the bloom-filter skip index, which resolves the
membership question without scanning the random `trace_id` column.

## Why step 3 shows `0.00 B` on a fresh demo

Step 3 is "Compression: compressed vs uncompressed bytes".

Per-column byte accounting only exists for parts in Wide format.
`min_bytes_for_wide_part` defaults to 10 MiB and the demo writes well under one,
so every part is Compact and `system.columns` has nothing to report. The real
per-column numbers come from `benchmarks/compression_ratio.py`, which loads
enough rows to cross that boundary.

`system.parts` still reports whole-part totals, which is why step 3 ends with a
second query against it.

## How `TO VOLUME 'cold'` resolves

Step 4 is "Tiering: hot-to-cold (listing 7.2)".

`TO VOLUME 'cold'` resolves against the `cold` volume defined in
`config.d/storage.xml`, whose disk is the S3-backed `s3_cold` disk pointing at
the MinIO object store. That is the same API AWS S3, GCS, and Azure Blob expose.
Only the endpoint and the credentials change between them.

The rule fires on parts older than two days, and on a fresh demo nothing is two
days old yet, so parts stay on `default` until you lower the boundary.

Step 4 selects a real partition id into `$PART` first and then moves it
explicitly, because `MOVE PARTITION` takes a literal id and not a subquery.

## Why `DROP PARTITION` is instant

Step 5 is "DROP PARTITION is instant (metadata-time retention)".

Dropping a partition is a metadata operation, not a row-by-row tombstone delete.
That is why the time it takes does not depend on how many rows the day held, and
it is the Cassandra-tombstone contrast from the chapter opener.

## Why the row policy comes last

Step 6 is "Row policy blocks cross-tenant reads (listing 7.4)".

The listing 7.4 policy is `TO ALL`, so it applies to the `default` admin login
too. That username is not a tenant_id, so once the policy exists the admin sees
zero rows and every earlier query looks broken. That is why steps 1 to 5 run
first, and that ordering is the point of section 7.5.2's trap: the policy gates
every read.

It works by rewriting `tenant_id = currentUser()` onto every SELECT, so a tenant
cannot read another tenant's spans even by asking for them directly.

Reads are all it gates. The policy does **not** gate `INSERT` or `DROP PARTITION`,
so a real ingest path has to validate `tenant_id` against the authenticated
principal itself. `tests/test_tenancy.sh` proves that gap.

## Tempo's cold boundary

Step 7 is "(Optional) The block archetype: Grafana Tempo".

Tempo's object store is the local filesystem in this stack and S3 or GCS in
production. It expresses the cold boundary as `compactor.block_retention` rather
than `TTL ... TO VOLUME`, which is the difference section 7.3 draws against the
ClickHouse row store.

## Running the book's listings verbatim

The book's listings are kept terse. A few need a server-side prerequisite or a
specific run order that the code supplies. If you run a listing verbatim and it
behaves unexpectedly, these are why:

1. **Listing 7.1 needs `SETTINGS storage_policy = 'tiered'`** for listing 7.2's
   `TO VOLUME 'cold'` to resolve. A `MergeTree` table on the default policy has
   no volume named `cold`, so the tiering ALTER fails without it. `init.sql`
   supplies the SETTINGS line for you.
2. **Listing 7.2's `DROP PARTITION '20260601'`** targets a literal past date. On
   a fresh demo no such partition exists, so it no-ops (a silent success that is
   itself the metadata-retention point). The walkthrough shows how to drop a
   partition that actually holds data.
3. **Listing 7.4's `MODIFY ORDER BY (tenant_id, ...)` cannot run on an existing
   table.** ClickHouse requires the primary key to stay a prefix of the sorting
   key, and `MODIFY ORDER BY` only accepts columns introduced in the same
   statement, so a pre-existing `tenant_id` column cannot be moved into the sort
   key by ALTER (verified on 25.8, `BAD_ARGUMENTS`, both prepend and append). A
   tenant-leading layout is therefore a `CREATE TABLE` property: a fresh store
   creates the table with `ORDER BY (tenant_id, service_name, span_name,
   toStartOfHour(timestamp), trace_id)`. `tenancy.sql` adds the `tenant_id`
   column and applies the row policy (the isolation boundary) without running the
   ALTER ClickHouse rejects. The sort-key prefix is a read-locality optimization,
   not the security mechanism.
4. **Listing 7.4's `currentUser()`** returns the connected SQL username, which is
   why the demo names its two users `tenant_a` / `tenant_b`. A real deployment
   maps an authenticated principal to a tenant claim rather than naming the SQL
   user after the tenant; the row-policy mechanics are identical.
