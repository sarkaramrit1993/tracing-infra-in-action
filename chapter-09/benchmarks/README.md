# Chapter 9 benchmarks

Two measurements, both of which exist because the chapter makes a claim that
would otherwise be an assertion.

| Script | Replaces | Needs |
|---|---|---|
| `sampler_divergence.py` | "the post error rate reads inflated against the pre rate" | the stack up, traffic driven, Prometheus |
| `fingerprint_compression.py` | "millions of error spans collapse to tens or low thousands of fingerprints" | the stack up, ClickHouse |

The rendered record of the last committed run is in [../RESULTS.md](../RESULTS.md),
generated from the JSON in `results/` by `scripts/render_results.py` at the
repository root. That directory is gitignored so your own runs stay out of the
history; the committed reference runs were added with `git add -f`. Every run
stamps its filename to the second, so a run of yours lands on a new path and can
never overwrite one of those: a gitignore entry has no power over a file that is
already tracked, and the guarantee has to come from the name.

## sampler_divergence.py

Reads four series out of Prometheus at one (service, span) grain, computes both
error rates and reports the ratio between them.

```bash
python3 benchmarks/sampler_divergence.py
```

```
[divergence] Prometheus=http://localhost:9090 grain=checkout-service/fraud.score
[divergence] pre : total=306 errors=9 rate=2.941%
[divergence] post: total=14 errors=9 rate=64.286%
[divergence] PASS: post error rate > pre error rate (inflation x21.9); pre total > post total
[divergence] wrote .../results/sampler-divergence-2026-08-26T013924.json
```

Your numbers will differ, and they should. The script asserts **direction only**:
that the pre total exceeds the post total, because the sampler drops spans, and
that the post error rate exceeds the pre one, because the keep-errors policy
protects the numerator while the denominator is cut. The magnitude is a function
of the injected error ratio and the sample rate. Reported, never asserted.

Drive traffic first and wait 15 seconds, or the series do not exist yet and the
script exits telling you so. `SERVICE` and `SPAN` are environment variables;
`SPAN=""` measures the whole service instead of one operation.

## fingerprint_compression.py

Section 9.2.3 says the compression from fingerprinting is "dramatic and
reliable", with millions of error spans collapsing to tens or low thousands of
distinct fingerprints. That is the sentence this script exists to replace.

The problem with measuring it in production is that nobody knows the right
answer. You can count issues, but there is nothing to grade the count against,
because "how many distinct bugs does this service actually have" is not a
question the store can answer. So this script supplies the answer: seed a known
number of code paths, write that number into a truth table **before any
measuring query runs**, and then ask the listing 9.2 view how many it found.

The truth table buys ordering, not independence. `P` is the constant that drove
the generator, written down and read back, so the round trip proves the store
kept the number rather than that the generator produced that many paths. `F` is
the measurement. This is one measured number graded against one declared one,
which is what the claim needs; it is not chapter 8's ground-truth discipline,
where the recorded population is what an estimator has to recover.

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

About ten seconds. Rows are built server-side with `INSERT ... SELECT FROM
numbers()`, so nothing large crosses the wire, and everything is a function of
the row index, so two runs at the same `PATHS` and `SPANS` return the same
numbers.

### What it asserts, and why those three

**`F == P`, exactly.** This is the whole test, and it fails in both directions.
`P` is declared and `F` is measured, so what is being graded is the
normalization and nothing else.
A regex that misses a variable token leaves that token in the template and one
bug forks into many, pushing `F` up toward `N`. A regex that strips too much
merges two genuinely different bugs into one issue, pulling `F` below `P`. Only a
normalization that gets both edges right returns the seeded count on the nose.

**`D > 0.9 * N`.** `D` is how many distinct fingerprints the same three inputs
produce with **no** normalization at all. If the raw messages were already
near-duplicates, the compression below would be the generator's doing rather than
the regex's, and the measurement would be worthless. Every generated message
carries three variable tokens: a seven-digit cart id, a duration in
milliseconds, and a sixteen-character lowercase hex request id. The hex is what
makes each message effectively unique, and lowercase is load-bearing, because
the listing 9.2 regex reads `[0-9a-f]` and an uppercase `hex()` would sail
straight through normalization.

**Top ten over 50 percent.** Section 9.2.3's premise is that a handful of code
paths throw the overwhelming majority of the errors. Volume is distributed Zipf
across the paths, truncated Pareto with `alpha = 0.5`, so that premise is
modelled rather than asserted. The busiest path takes about 30 percent and the
top ten about 72.

The `N/F` ratio is reported and never asserted. It is `N` divided by the number
of code paths seeded here, so it is a property of this generator. Quoting 1,667x
as a figure for a real service would be exactly the kind of unsupported number
this script exists to remove.

### What it does not touch

It never writes to `tracing.otel_traces`. It creates a scratch table with
`CREATE TABLE ... AS tracing.otel_traces` so the schema cannot drift, applies
`clickhouse/error_index.sql` against it with three table names redirected, and
drops everything it made on the way out. The live store is untouched, so this
can be run against a stack you are in the middle of using.

The three substitutions are the only change made to the listing. Nothing here
reimplements the normalization, so if listing 9.2 changes, this benchmark
changes with it.

### Environment variables

```bash
PATHS=300 SPANS=200000 python3 benchmarks/fingerprint_compression.py
```

`PATHS` is `P`, the number of distinct code paths seeded. `SPANS` is `N`, the
number of raw error spans. The identity `F == P` holds at any pair, which is
worth confirming once: it is what tells you the result is about the
normalization rather than about the size of the run.

The first `P` rows are seeded one per path, so every path is present by
construction. Without that seeding, `F == P` would be a statement about how a
random draw happened to land rather than about the regex.

## Cleaning up

Neither script leaves anything behind. `sampler_divergence.py` only reads.
`fingerprint_compression.py` drops its scratch tables at the end and again at the
start of the next run, so an interrupted run costs nothing:

```bash
docker compose exec -T clickhouse clickhouse-client \
  --query "SELECT name FROM system.tables WHERE database = 'tracing' ORDER BY name" < /dev/null
```

```
exc_mv
exceptions
otel_traces
```

The span table plus the listing 9.2 index, if you have applied it. Nothing with
an `fp_bench_` prefix. If one of those does appear, a run was killed partway;
rerunning the script drops them before it starts.
