# Fingerprints: normalization is what turns a firehose into a triage list

Run this from `chapter-09/`. It does not depend on the other two exercises, and
the cleanup at the end drops every table it created and puts the listing back the
way it shipped.

## The question

Section 9.2.3 says the compression is dramatic and reliable: millions of raw
error spans collapse to tens or low thousands of distinct fingerprints, because a
handful of code paths throw the overwhelming majority of the errors.

That is a claim about a normalization step, and the awkward thing about it is
that in production you cannot check it. You can count issues, but there is
nothing to grade the count against, because "how many distinct bugs does this
service actually have" is not a question the store can answer. A fingerprint
scheme that quietly merges two different bugs and one that quietly splits one bug
into forty both produce a plausible number and neither produces a complaint.

So this exercise does what chapter 8 did to the unbiased estimate: it seeds a
known population, writes down what it seeded **before** anything measures it, and
then grades the answer. The number that matters is `F == P`, and it fails in both
directions.

## The starting state

Three helpers. `ch` runs a query, `ch_file` applies a `.sql` file, and
`await_rows` blocks until a query comes back with a number rather than sleeping
and hoping:

```bash
ch()      { docker compose exec -T clickhouse clickhouse-client "$@" < /dev/null; }
ch_file() { docker compose exec -T clickhouse clickhouse-client --multiquery < "$1"; }
await_rows() { for _ in $(seq 1 180); do
                 [ "$(ch --query "$1" 2>/dev/null)" -ge "$2" ] 2>/dev/null && return 0
                 sleep 2
               done
               echo "timed out: $1 never reached $2" >&2; return 1; }
```

The `< /dev/null` is not decoration. Without it the client waits on a stdin that
never reaches EOF. NOTES has the detail.

```bash
docker compose up -d --build
docker compose ps
```

Wait for the health column to settle. This exercise needs ClickHouse and nothing
else, so you can start reading as soon as that one is `healthy`.

Arm the listing 9.2 index against live traffic first, because a materialized view
fires on insert and never backfills. Order matters: the view has to exist before
the spans do.

```bash
ch --query "DROP VIEW IF EXISTS tracing.exc_mv"
ch --query "DROP TABLE IF EXISTS tracing.exceptions"
ch_file clickhouse/error_index.sql
for _ in $(seq 1 150); do curl -s -o /dev/null http://localhost:8080/checkout; done
for _ in $(seq 1 6); do curl -s -o /dev/null "http://localhost:8080/checkout?fail=1"; done
await_rows "SELECT sum(error_count) FROM tracing.exceptions" 7
```

Seven, not six: the 1-in-100 cadence throws one of its own inside 150 requests.
The poll is on the index rather than on a clock, because a span has to clear the
exporter's batch, the tail sampler, Kafka and the storage consumer before the
materialized view has anything to fire on.

## What the index holds

```bash
ch --query "
SELECT any(error_type)      AS error_type,
       any(msg_template)    AS message,
       sum(error_count)     AS errors,
       any(sample_trace_id) AS trace
FROM tracing.exceptions GROUP BY fingerprint ORDER BY errors DESC"
```

```
TimeoutError  fraud scoring backend timed out after ?ms (req ?)  14  7961af7916503ef5573f44d400ae0e97
```

One row. Fourteen error spans from seven failed checkouts, one issue, and a
`trace` column that takes you from the issue back to a whole trace. Fourteen and
not seven because a failed checkout records the exception twice, on the span that
threw and on the server span that reports the failure to the caller, and both
land under the same fingerprint. The raw messages behind them differ per failure,
because each carried its own deadline in milliseconds and its own request id:

```bash
ch --query "
SELECT attributes['exception.message'] AS raw
FROM tracing.otel_traces WHERE status_code = 'STATUS_CODE_ERROR'
ORDER BY timestamp DESC LIMIT 3"
```

```
fraud scoring backend timed out after 30462ms (req db673223)
fraud scoring backend timed out after 30461ms (req 9871f9c4)
fraud scoring backend timed out after 30461ms (req 9871f9c4)
```

Three rows, two distinct strings: the last two are the two spans of one failed
checkout carrying the same text. Across failures they differ, and one template
covers all of them. That is the whole mechanism, and at fourteen spans it is also
unimpressive. The interesting question is what happens at two
million, and whether the answer is right.

## Grading it

`benchmarks/fingerprint_compression.py` generates an error population
server-side over a known number of code paths, records that number into a truth
table before running a single measuring query, applies the real
`clickhouse/error_index.sql` against it, and compares.

```bash
python3 benchmarks/fingerprint_compression.py
```

```
[fingerprint] paths=1,200 spans=2,000,000 zipf_alpha=0.5
[fingerprint] seeded path count recorded before any measuring query ran
[fingerprint] listing 9.2 view armed over an empty scratch table
[fingerprint] generating error spans server-side...
[fingerprint] N raw error spans        : 2,000,000
[fingerprint] D distinct un-normalized : 2,000,000  (100.0000% of N)
[fingerprint] F distinct fingerprints  : 1,200
[fingerprint] P code paths seeded      : 1,200
[fingerprint] compression N/F          : 1,667x
[fingerprint] top-10 share of volume   : 71.9%  (busiest alone 30.2%)
[fingerprint] busiest issue            : ConnectionResetError  |  payment.lookup failed for cart ?: deadline exceeded after ?ms (req ?)
[fingerprint] PASS: F == P == 1,200; D is 100.0% of N; top ten carry 71.9%
[fingerprint] wrote .../results/fingerprint-compression-2026-08-26T015357.json
[fingerprint] scratch tables dropped; the live store was never touched
```

About ten seconds, and those numbers reproduce on your machine. Read them in
order, because each one exists to close off a way the result could be
uninteresting.

`D` is 2,000,000, which is 100 percent of `N`. Every raw message was unique
before normalization ran, so the collapse that follows is the regex's doing and
not the generator quietly emitting the same string two million times.

`F` is 1,200 and `P` is 1,200. `P` came out of a table written before the first
measuring query existed. The normalization found the seeded code-path count on
the nose, which means it neither merged two of them nor split one.

The top ten issues carry 71.9 percent of the volume and the busiest alone carries
30.2. Section 9.2.3's premise, that a handful of paths throw most of the errors,
is modelled here rather than assumed: volume is distributed Zipf across the paths.

`1,667x` is the only number on that list worth nothing to you. It is `N` divided
by the number of paths this generator seeded, so quoting it for a real service
would be the same unsupported claim in a new font.

Read the truth table yourself, which is the point of it being a table. The run
drops its scratch tables on the way out, so ask it not to:

```bash
KEEP_SCRATCH=1 python3 benchmarks/fingerprint_compression.py
ch --query "SELECT * FROM tracing.fp_bench_truth FORMAT Vertical"
```

That row is written before any measuring query runs, which is what makes it
truth rather than a second opinion. Drop the scratch tables when you are done:

```bash
ch --query "DROP TABLE tracing.fp_bench_truth"
```

The two expressions it measures are the two the book prints, read straight out
of the listing:

```bash
grep -n 'replaceRegexpAll(attributes\|extractAll(attributes' clickhouse/error_index.sql
```

```
70:        replaceRegexpAll(attributes['exception.message'],
74:                extractAll(attributes['exception.stacktrace'],
```

The benchmark reads that file and substitutes three table names. Nothing in it
reimplements the normalization, so if the listing changes, the measurement
changes with it, which is what the next section leans on.

## Try this

Two edits, each one line, each backing up the listing first and restoring it in
the same section.

**Widen the token class from hex to alphanumeric.** One character, `f` to `z`,
which is the kind of "let me also catch the base-36 ids" change that looks
harmless:

```bash
cp clickhouse/error_index.sql clickhouse/error_index.sql.bak
sed -i.tmp "s/'\[0-9a-f\]{8,}|\[0-9\]+', '?'/'[0-9a-z]{8,}|[0-9]+', '?'/" clickhouse/error_index.sql
rm -f clickhouse/error_index.sql.tmp
python3 benchmarks/fingerprint_compression.py
```

```
[fingerprint] F distinct fingerprints  : 1,200
[fingerprint] P code paths seeded      : 1,200
[fingerprint] busiest issue            : ConnectionResetError  |  payment.lookup failed for cart ?: ? ? after ?ms (req ?)
[fingerprint] PASS: F == P == 1,200; D is 100.0% of N; top ten carry 71.9%
```

Look at the busiest issue rather than at `F`. `deadline exceeded` has become
`? ?`: the template lost two real words, because `deadline` and `exceeded` are
both eight letters and both now look like ids. A human triaging that list has
lost the cause of the error from the issue title.

And `F` did not move. That is the lesson, and it is not the one the edit was
supposed to teach. The fingerprint has three inputs, and the top frame is still
unique per code path, so it carries the whole identity on its own while the
template degrades quietly underneath. A fingerprint scheme can be visibly
losing information and still return the right issue count. Restore:

```bash
mv clickhouse/error_index.sql.bak clickhouse/error_index.sql
```

**Drop `top_frame` from the hash.** Now the identity rests on type and template
alone. Run it with more code paths than there are distinct message templates, so
that two genuinely different bugs are forced to share a message shape:

```bash
cp clickhouse/error_index.sql clickhouse/error_index.sql.bak
sed -i.tmp 's/cityHash64(error_type, msg_template, top_frame)/cityHash64(error_type, msg_template)/' clickhouse/error_index.sql
rm -f clickhouse/error_index.sql.tmp
PATHS=2400 python3 benchmarks/fingerprint_compression.py
```

```
[fingerprint] F distinct fingerprints  : 1,200
[fingerprint] P code paths seeded      : 2,400
[fingerprint] F=1,200 against a recorded P=2,400. The normalization is over-merging distinct bugs.
```

Exactly half. 2,400 code paths, 1,200 issues, and every issue on that list is two
different bugs wearing one title, raised from two different modules. On a triage
board it reads as one problem with twice the volume, so it gets one owner and one
fix, and half of it stays broken.

The script exits non-zero and says which direction it went. That is what the
truth table bought: without a recorded `P` the run above produces 1,200 issues at
an 833x compression ratio and looks entirely healthy. Restore:

```bash
mv clickhouse/error_index.sql.bak clickhouse/error_index.sql
PATHS=2400 python3 benchmarks/fingerprint_compression.py
```

```
[fingerprint] F distinct fingerprints  : 2,400
[fingerprint] P code paths seeded      : 2,400
[fingerprint] PASS: F == P == 2,400; D is 100.0% of N; top ten carry 71.2%
```

`F == P` at 2,400 as well as at 1,200, which is worth confirming once: it tells
you the identity is about the normalization and not about the size of the run.

## Clean up

Drop the live index, and check the listing is the one that shipped:

```bash
ch --query "DROP VIEW IF EXISTS tracing.exc_mv"
ch --query "DROP TABLE IF EXISTS tracing.exceptions"
ch --query "SELECT name FROM system.tables WHERE database = 'tracing' ORDER BY name"
grep -c 'cityHash64(error_type, msg_template, top_frame)' clickhouse/error_index.sql
grep -o "'\[0-9a-f\]{8,}|\[0-9\]+'" clickhouse/error_index.sql
ls clickhouse/*.bak clickhouse/*.tmp 2>/dev/null | wc -l
```

```
otel_traces
1
'[0-9a-f]{8,}|[0-9]+'
       0
```

One table, which is what a fresh stack has. The three-input hash is back, the
token class is back to hex, and nothing with a `.bak` or `.tmp` suffix is left in
`clickhouse/`. Nothing with an `fp_bench_` prefix should appear in that table
list either; the benchmark drops its own scratch tables at the end of every run
and again at the start of the next, so an interrupted run costs nothing.

Drop the view before the target table, in that order. While the view exists it is
watching inserts, and a table that vanishes underneath a live view leaves the
next insert into `otel_traces` failing rather than silently unindexed.

## Going deeper

`clickhouse/error_index.sql` is listing 9.2 with its annotations, including why
the exception detail is read out of an attribute map rather than out of columns
of its own, and why the target table is an `AggregatingMergeTree` keyed on the
fingerprint.

**Cause the other failure on purpose.** The listing strips the line number out of
the top frame before hashing it. Take that out and watch one bug become three:

```bash
cp clickhouse/error_index.sql clickhouse/error_index.sql.bak
python3 - <<'PY'
from pathlib import Path
p = Path("clickhouse/error_index.sql")
old = """        replaceRegexpAll(
            arrayElement(
                extractAll(attributes['exception.stacktrace'],
                           'File "[^"]*", line [0-9]+, in [A-Za-z_0-9<>.]+'),
                -1),
            ', line [0-9]+', '') AS top_frame"""
new = """        arrayElement(
                extractAll(attributes['exception.stacktrace'],
                           'File "[^"]*", line [0-9]+, in [A-Za-z_0-9<>.]+'),
                -1) AS top_frame"""
p.write_text(p.read_text().replace(old, new))
PY
python3 benchmarks/fingerprint_compression.py
```

```
[fingerprint] F distinct fingerprints  : 3,598
[fingerprint] P code paths seeded      : 1,200
[fingerprint] F=3,598 against a recorded P=1,200. The normalization is leaving a variable token in the template.
```

Three times 1,200, less two. The generator raises each code path at one of three
line numbers, standing in for three deploys of the same file, and without the
strip each deploy forks the issue again. The two missing are line variants of the
rarest paths that the Zipf draw never produced.

Three thousand issues where there are twelve hundred bugs is the under-merge
direction, and it is worse than it sounds on a triage board. Every deploy resets
the volume on every issue it touches, and every one of those forks arrives marked
"first seen in this deploy", which is exactly the alert section 9.2.3 calls the
highest-signal one that falls out of fingerprinting. Restore:

```bash
mv clickhouse/error_index.sql.bak clickhouse/error_index.sql
```

Two more if the storage engine interests you rather than the argument.

The target table is an `AggregatingMergeTree` with `SimpleAggregateFunction`
columns, and the read query in the listing's trailing comment does a `GROUP BY
fingerprint` even though the table is already keyed on it. Take the `GROUP BY`
out and read the raw rows: before a background merge runs you get one row per
insert batch rather than one per issue, and the counts read low. `OPTIMIZE TABLE
tracing.exceptions FINAL` collapses them, but a merge that has not happened yet
is not a bug to wait out, it is a reason to always re-aggregate on read.

And point the view at `first_seen` as `min` and `last_seen` as `max` over spans
that arrive out of order, which is the normal case with a batch processor in the
path. The window is correct in either order because both are aggregates over the
whole fingerprint rather than over the arrival sequence. Replace either with
`anyLast` and it starts reporting whichever span the merge happened to see last.
