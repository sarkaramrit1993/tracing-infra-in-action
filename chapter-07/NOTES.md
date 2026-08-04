# Notes

Why [README.md](README.md) and the three files in `exercises/` are built the way
they are. None of this is needed to run the stack. Read a section when something
surprises you, or when you want to know what the demo is doing underneath.

## Where a finding goes

Every paragraph in this file started life next to a command. The rule that
decides where the next one lands:

- A finding that changes what a reader **does** stays in the walkthrough, beside
  the command it governs. Preconditions, pass conditions, run order, version
  constraints and the sample output people compare their own against are all
  this kind. Move one and you break the reader, even though every command still
  runs.
- A finding that explains what a reader **saw** belongs here. The walkthrough
  keeps one sentence and a link that names what this file explains.

When a paragraph is both, split it. The operative sentence stays put and the
explanation moves.

The test is that any one exercise can be finished start to finish without
opening this file. If a section here is load-bearing for a step, it is in the
wrong document.

## Why there are two helpers, `ch` and `ch_file`

Both are defined at the top of README's "Look at your trace data", and again at
the top of each exercise so that each one stands alone.

`clickhouse-client` reads stdin for INSERT data even when the row is already
inline in `VALUES`, and `docker compose exec -T` hands it whatever stdin you
have. If that stdin stays open and never reaches EOF, the INSERT sits there
forever with nothing printed. That is why `ch` always redirects `/dev/null`: a
query it runs can never end up waiting on your keyboard.

But applying `tiering.sql` and `tenancy.sql` needs the opposite. They feed a
whole `.sql` file to `--multiquery`, which means stdin has to carry the file. One
helper cannot do both, and guessing from the shape of stdin gets it wrong
somewhere. An earlier version guessed with `[ -t 0 ]`, which worked at a terminal
and hung anywhere stdin was an open pipe. So the file case gets its own name,
`ch_file`, and it is the only thing here that puts anything on stdin.

The same split is in `tests/test_stack.sh` and `tests/test_tenancy.sh` as `CH`
and `CH_FILE`, for the same reason.

## Why the point lookup filters on `GET /checkout`

README's "Look at your trace data" picks a `trace_id` before it looks one up, and
it filters that pick on `GET /checkout`.

The container healthcheck writes a one-span `GET /health` trace every 10 seconds,
and those outnumber the checkout traces once the traffic loop finishes. Without
the filter you would usually pick up a one-span healthcheck trace instead of the
seven-span checkout trace.

The lookup itself is answered by the bloom-filter skip index, which resolves the
membership question without scanning the random `trace_id` column.

## Why the compression exercise builds its own tables

[exercises/compression.md](exercises/compression.md) does not measure
`otel_traces`. Two reasons, and both would bite anyone who pointed
`clickhouse/compression.sql` at the live table instead.

The first is Compact parts. Per-column byte accounting only exists for parts in
Wide format. `min_bytes_for_wide_part` defaults to 10 MiB and a demo that has
been up for an hour holds a few hundred rows, well under one MiB, so every part
is Compact, a Compact part accounts for all its columns together,
`system.columns` has nothing to report, and listing 7.3 comes back `0.00 B` and
`nan` down every row. The exercise loads 200,000 rows, which crosses that
boundary. `benchmarks/compression_ratio.py` loads more and fails loudly rather
than publishing a table of `nan`.

The second is the clock. Byte counts only repeat between runs if the generated
timestamps are fixed: a moving anchor shifts the Delta codec's base value, a run
that straddles an hour boundary reorders rows inside every sort-key group, and a
run that straddles UTC midnight splits the load across two partitions
`OPTIMIZE FINAL` cannot merge. But a fixed past anchor cannot live in
`otel_traces` once listing 7.2 is on it, because ClickHouse reserves space by the
TTL rules at insert time. Rows past the two-day boundary would be written
straight to the S3 cold disk and never land hot, and rows past fifteen days would
be dropped on the first merge. The measurement would then describe S3-resident
parts, or nothing.

So the scratch tables carry listing 7.1's columns, codecs, sort key, partitioning
and skip index with no TTL and no storage policy, and they are dropped at the
end. `bloom_index_pruning.py` and `tenant_cardinality_blowup.py` do the same
thing for the same reasons.

`system.parts` still reports whole-part totals whatever the part format, which is
why the exercise ends with a query against it.

## How `TO VOLUME 'cold'` resolves

This is what [exercises/tiering.md](exercises/tiering.md) is built on.

`TO VOLUME 'cold'` resolves against the `cold` volume defined in
`config.d/storage.xml`, whose disk is the S3-backed `s3_cold` disk pointing at
the MinIO object store. That is the same API AWS S3, GCS, and Azure Blob expose.
Only the endpoint and the credentials change between them.

The rule fires on parts older than two days. The exercise stages rows dated
yesterday, which is inside that boundary on purpose, so they land hot and there
is a move to watch. It then moves the partition by hand.

It selects a real partition id into `$PART` first and then moves it explicitly,
because `MOVE PARTITION` takes a literal id and not a subquery. Deriving that id
from the exercise's own rows, rather than from `ORDER BY partition LIMIT 1`, is
what keeps the move and the later drop off the live traffic in today's partition.

An earlier version lowered the move boundary to five seconds first, to
make the part eligible. That made every part eligible at once, which wakes
ClickHouse's background mover, and the manual `MOVE PARTITION` then raced it and
failed intermittently with `PART_IS_TEMPORARILY_LOCKED`. `MOVE PARTITION` is an
explicit move that does not need the part to be TTL-eligible, so the boundary
change was never doing any work. Dropping it removes the race and leaves listing
7.2's real rule on the table, which is one less thing to put back.

When the rule does the moving, rather than a hand-written `MOVE PARTITION`, the
wait before `disk_name` changes is the scheduler and not the storage. The
move-selecting task sleeps five seconds while it has work and backs off toward
sixty once the server has been quiet, so the same ALTER can land in eleven
seconds or take the better part of a minute. `tiering_automation.py` waits up to
three minutes for that reason, and publishes no number measured from it.

An insert picks its destination from the move TTL at write time. Rows already
past the boundary are written straight to the cold disk and no part is ever
relocated afterwards, because nothing has to be. So backfilling a month of
history into a tiered table writes a month of history to object storage, at
object-storage write rates, whether or not you expected that. It is also why the
compression exercise keeps its fixed past timestamps out of `otel_traces`.

Counting the objects behind the move is scoped to the parts that are live right
now for a reason. ClickHouse deletes a replaced part's blobs lazily,
`old_parts_lifetime` after the replacement, which is eight minutes by default. A
bucket-wide count therefore picks up garbage from earlier work and is not a fact
about the move you just made. That is why `mc` can report more objects than
ClickHouse does on a second run through the exercise.

## Why `DROP PARTITION` is instant

The last thing [exercises/tiering.md](exercises/tiering.md) does.

Dropping a partition is a metadata operation, not a row-by-row tombstone delete.
That is why the time it takes does not depend on how many rows the day held, and
it is the Cassandra-tombstone contrast from the chapter opener. It also does not
depend on which disk held the part, so dropping a partition that has already
moved to S3 costs the same as dropping one that never left.

## Why the exempt list names `default`

Listing 7.4 ends `TO ALL EXCEPT admin, ingest`, and annotation #D says why: a
bare `TO ALL` would include your operators and your ingest user, who would then
read an empty table. Their logins are not tenant ids, so the predicate matches
nothing for them.

`clickhouse/tenancy.sql` writes `TO ALL EXCEPT default` instead. This stack has
exactly one privileged login and it is both the operator and the writer. Roles
named `admin` and `ingest` cannot stand in for the book's names either: the
`default` user is declared in `users.d/z-allow-network.xml`, and granting a role
to a user held in XML storage fails with `ACCESS_STORAGE_READONLY` (verified on
25.8). Swap `default` for your own operator and writer logins and the line is
the book's.

The exemption is the reason nothing else here has to work around the policy. The
walkthrough, the benchmarks and the other two exercises all connect as `default`
and read the whole table whether or not the policy is in place. Both live test
scripts still drop what they created at the end, but that is hygiene now: the
tenant logins are passwordless and hold SELECT, and dropping the `tenant_id`
column puts the table back to listing 7.1's nine.

## Why tenants can read the tenant map

The listing 7.4 predicate is a subquery: `tenant_id IN (SELECT tenant_id FROM
tenant_users WHERE user_name = currentUser())`. ClickHouse evaluates a row
policy expression with the querying user's own grants, so a login that cannot
read `tenant_users` cannot run any SELECT against `otel_traces` at all. It gets
`Not enough privileges ... SELECT(tenant_id, user_name) ON tracing.tenant_users`
rather than an empty result.

`tenancy.sql` therefore grants the two tenant logins SELECT on the map, which
means each of them can read the whole map and learn the other's name. That is a
demo compromise, not the shape to copy. Production keeps the map out of the
tenants' reach: a dictionary they query through `dictGet`, or a separate
database they hold no grant on.

The indirection is still the point. The map is what makes annotation #C true:
`acme_reader` is a login and `tenant_a` is a customer, and admitting a second
person at Acme is an INSERT into `tenant_users` rather than a new tenant id or
an edit to the policy.

## What the row policy does not gate

Reads are all of it. The policy rewrites SELECT. It does **not** gate `INSERT`,
`ALTER ... DELETE` or `DROP PARTITION`, so a real ingest path has to validate
`tenant_id` against the authenticated principal itself and never trust the value
that arrived on the wire. `tests/test_tenancy.sh` proves that gap by writing a
mislabeled row as a trusted writer and reading it back as the wrong tenant.

## Tempo's cold boundary, and why the config looks different from the book's

Tempo writes its blocks to the same MinIO that backs ClickHouse's `s3_cold`
disk, in the `tempo-blocks` bucket. Both archetypes therefore sit on one object
store, and the contrast section 7.3 draws is the unit of storage rather than the
medium: a part of a column against an opaque Parquet block.

It expresses the cold boundary as a block retention period rather than
`TTL ... TO VOLUME`. Whole blocks expire; ClickHouse evaluates a TTL expression
per part and moves before it deletes. Same two days, different unit of work.

The config sits at a different path than a 2.x example would. Tempo 3.0's
Project Rhythm re-architecture (section 7.4.4) removed the `ingester` and
`compactor` sections outright: feeding 3.0 a 2.x config fails at startup with
`field ingester not found in type app.Config`. Block building moved to
`live_store` and retention to `backend_scheduler.provider.compaction.compaction`.

Monolithic mode still needs no Kafka. The Kafka-backed ingest path 7.4.4
describes is what microservices mode does, and this stack runs the single
binary, so Tempo receives OTLP straight from the Collector's second exporter.

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
   itself the metadata-retention point). `exercises/tiering.md` stages a
   partition and drops one that actually holds data.
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
4. **Listing 7.4's `TO ALL EXCEPT admin, ingest`** names an operator and a
   writer this stack does not have separately. `tenancy.sql` exempts `default`,
   which is both, because ClickHouse will not grant a role to a user declared in
   XML. See "Why the exempt list names `default`" above.
5. **Listing 7.4's `tenant_users`** has to exist and has to be readable by the
   tenant logins, because the policy predicate runs with the querying user's
   grants. `tenancy.sql` creates it, seeds it and grants it. See "Why tenants
   can read the tenant map" above.
