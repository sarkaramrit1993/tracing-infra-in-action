# Notes

Why this directory is built the way it is. Nothing here is needed to run
anything in the README. It exists because several of the decisions below look
arbitrary until they bite, and every one of them cost real time to find.

The short list, if you are here because something surprised you:

- A stack that looks healthy but produces no logs: [the disk trap](#the-disk-trap-loki-reports-up-and-503s-every-write).
- A Loki query that returns nothing and no error: [trace_id is not a label](#trace_id-is-not-a-label).
- An exemplar that points at nothing: [why exemplars come off the post connector](#why-exemplars-come-off-the-post-connector).
- `histogram_quantile` returning `+Inf`: [why the buckets are explicit](#why-the-histogram-buckets-are-explicit).

## Why span metrics run twice

Chapter 9's central claim is that a metric derived from sampled traces describes
the sample, not the service. Making that checkable needs both numbers at once,
over the same workload, at the same grain. Two Collectors would introduce a
second variable. One Collector with two connectors on opposite sides of the
sampler does not.

`traces/in` receives OTLP, hands the full stream to `spanmetrics/pre` and
`servicegraph`, and forwards the same spans on. `traces/sampled` receives the
forward, runs `tail_sampling`, and exports to Kafka and to `spanmetrics/post`.
The `forward` connector is what makes that split possible: it is the only way to
have one receiver feed two pipelines with different processor chains.

The tail sampler is plain probabilistic on purpose. It does **not** stamp a W3C
`tracestate` `ot` threshold on the spans it keeps, so the post connector has no
way to reconstruct the population it came from. A sampler that did stamp one
would let the downstream weight its counts back up, the correction section 9.2.4
describes, and there would be no divergence left to look at. The divergence here
is the uncorrected case, which is also the common one.

## Why two helpers, `ch` and `ch_file`

```bash
ch()      { docker compose exec -T clickhouse clickhouse-client "$@" < /dev/null; }
ch_file() { docker compose exec -T clickhouse clickhouse-client --multiquery < "$1"; }
```

`clickhouse-client` reads standard input when it is given one. Inside a shell
loop that also owns stdin, a client without `< /dev/null` sits there forever
waiting on an EOF that never comes, producing no output and no error. That is
the trap that hung both of chapter 7's scripts for a reviewer.

They cannot be one helper. `ch_file` needs its stdin for the file it is piping
in, so it cannot redirect from `/dev/null`; `ch` must redirect or it hangs.
`tests/test_static.py` has a check over this directory's markdown and shell
scripts. Be exact about its reach: it fails the build on a `clickhouse-client`
command that carries `--query` and no redirect, after joining backslash
continuations so a command wrapped across two lines is read as one. It says
nothing about a client invoked through a variable, through a wrapper this
directory does not define, or from a file type it does not glob.

## The disk trap: Loki reports `Up` and 503s every write

This one cost the most time to find, so it is first among the traps.

If the Docker VM's disk fills, Loki keeps running. `docker compose ps` shows
`Up`. The `/ready` endpoint returns 200. The Collector's `otlphttp/loki`
exporter retries, gives up quietly, and drops the batch. Every log query returns
an empty result with `"status": "success"`. The only evidence anywhere is one
line at `warn` level in Loki's own log.

```bash
docker run --rm alpine df -h /
docker compose logs loki | grep -i "no space\|disk"
```

Loki's `log_level` is `warn` in `loki/loki.yaml` for exactly this reason: at
`error` the line does not appear at all. Allow about 4 GB free inside the VM
before starting, and reclaim with `docker system prune --volumes` when it is
tight. A macOS or Windows Docker Desktop VM is a fixed-size disk, so "free space
on my laptop" is not the number that matters.

The general shape is worth keeping. A component that degrades to silence rather
than to an error is the hardest kind to operate, and this stack contains three of
them: Loki out of disk, a Prometheus alert over an empty vector, and a
materialized view that never backfills.

## trace_id is not a label

Loki stores OTLP `TraceId` as **structured metadata**, not as a stream label.
So this returns nothing:

```bash
curl -s -G http://localhost:3100/loki/api/v1/query_range \
  --data-urlencode 'query={trace_id="cb51fea83a577a1f9b15d7f850667212"}' \
  --data-urlencode "start=$(python3 -c 'import time;print(int((time.time()-900)*1e9))')" \
  --data-urlencode "end=$(python3 -c 'import time;print(int(time.time()*1e9))')"
```

```
{"status":"success","data":{"resultType":"streams","result":[]}}
```

`"status":"success"`, zero rows, no error, no warning. A stream selector that
names a field which is not a label matches no streams, and matching no streams is
not an error condition in LogQL. The selector has to lead with a real label and
filter on the metadata afterwards:

```bash
curl -s -G http://localhost:3100/loki/api/v1/query_range \
  --data-urlencode 'query={service_name="checkout-service"} | trace_id="cb51fea83a577a1f9b15d7f850667212"' \
  --data-urlencode "start=$(python3 -c 'import time;print(int((time.time()-900)*1e9))')" \
  --data-urlencode "end=$(python3 -c 'import time;print(int(time.time()*1e9))')"
```

`allow_structured_metadata: true` in `loki/loki.yaml` is what makes the field
survive ingestion at all. Turn it off and Loki accepts the write and discards
the field, which produces the same empty result from the correct selector.

Promoting `trace_id` to a real label would make the first selector work and would
also be the exact mistake section 9.3.3 forbids: a label per trace id is a
cardinality explosion, one series per trace. Structured metadata is the bounded
place for it, the log-side equivalent of the exemplar buffer.

## Why exemplars come off the post connector

Both connectors have `exemplars.enabled: true`, but `tests/test_correlation.sh`
reads `post_duration_milliseconds_bucket` and not the pre one. Measured on this
stack, resolving every exemplar trace id against the span store:

```
post: 17 of 17 resolve
pre:  10 of 35 resolve
```

A pre-sampler exemplar is minted before the sampler has decided anything. The
sampler then keeps one successful trace in a hundred, and the pointer to each of
the other ninety-nine is left aiming at a trace that was never stored. Nothing
errors. `query_exemplars` returns a trace id, the drill-down runs, and the trace
viewer says the trace does not exist. That is contrib issue #38878, and it is the
dangling pointer section 9.3.2 names.

The post exemplars all resolve because they are minted from spans that already
survived the decision. The cost is that they only describe the sample, which is
the same trade the whole chapter is about: the pre series is the honest
aggregate, the post exemplar is the honest pointer, and you want both.

## Why the histogram buckets are explicit

```yaml
histogram:
  explicit:
    buckets: [2ms, 5ms, 10ms, 20ms, 50ms, 100ms, 200ms, 500ms, 1s, 2s, 5s, 10s]
```

The obvious modern choice is an exponential histogram: better resolution, fewer
configuration decisions, no bucket boundaries to guess. It does not work here,
and the failure is silent in both directions.

The Collector's `prometheus` exporter renders classic Prometheus exposition. An
exponential histogram has no classic rendering, so it comes out as a single
`le="+Inf"` bucket. Two things then break at once. `histogram_quantile` over one
bucket returns `+Inf` for every quantile, and an exemplar has no bucket to attach
to, so `/api/v1/query_exemplars` comes back empty. Section 9.3's jump from a
latency bucket to a trace depends on both halves, and the config that breaks it
is valid, boots clean, and reports no error.

This is a property of the exposition format, not of exponential histograms. Over
remote-write or OTLP-native ingest they carry fine.

## Why the exception detail is copied onto the span

The OpenTelemetry SDK records an exception as a span **event** named `exception`
carrying `exception.type`, `exception.message` and `exception.stacktrace`. The
storage-time consumer writes span **attributes**. Left alone, those three fields
never reach ClickHouse and the listing 9.2 index fingerprints over three empty
strings, producing exactly one issue for the entire service.

`transform/exception_to_span` in the Collector config copies them across. It runs
in `spanevent` context, where bare `attributes` means the event's map and
`span.attributes` means the span's, and it is guarded on the event name because a
span may carry other events. `error_mode: ignore` means a span event missing one
of the three fields is skipped rather than failing the whole batch.

What matters for reading the chapter is that the application knows nothing about
any of this. `app/checkout.py` calls `record_exception()`, the way an instrumented
service does. The fingerprints in listing 9.2 are ones any instrumented service
already emits, not ones this repository rigged the producer to emit.

## Why the error index is applied by hand

`clickhouse/init.sql` is mounted into ClickHouse's entrypoint directory and runs
on first boot. `clickhouse/error_index.sql` is not, and has to be applied
deliberately:

```bash
docker compose exec -T clickhouse clickhouse-client \
  --multiquery < clickhouse/error_index.sql
```

A materialized view fires on insert and never backfills. Error spans already in
`otel_traces` before the view exists are not indexed, ever. Putting it in the
boot path would hide that: on a fresh stack the view would always exist before
the first span, the reader would never see the ordering matter, and the first
time they applied the pattern to a store with history they would get an index
that silently covers only the last few minutes.

The same asymmetry runs the other way. A view is a trigger on insert, and a
`DELETE` or a `TRUNCATE` is not an insert, so rows removed from the source stay
folded into the index forever. `tests/test_stack.sh` drops the view and the
target table at the end for that reason, so the suite re-runs from any state.

## Why the ingest gap needs two unrelated counters

`spans:received:rate5m` reads `otelcol_receiver_accepted_spans_total`, the
Collector's own internal telemetry. `spans:expected:rate5m` reads
`checkout_spans_emitted_total`, a counter the producer increments from a
`SpanProcessor` in its own process.

Prometheus scrapes the producer **directly**, at `checkout-service:8080`. That is
the whole design. Two counts that travelled the same path cannot detect that path
losing spans: when the Collector drops half its input, both numbers halve and the
ratio holds at 1.0. The gap only opens when one of the two numbers never went
through the thing being measured.

`or vector(0)` on each side is not cosmetic either. The failure this rule exists
for is the Collector going away entirely, which takes its series stale; an empty
vector propagates through the arithmetic, and an alert over an empty vector never
fires. Without the fallback the alert works for every partial failure and misses
the total one.

Neither side uses `rate()`, and that is the second thing worth knowing about this
rule. `rate()` measures from a series' first sample INSIDE the window, and these
two counters do not come into existence the same way. The producer's is published
at zero from its first scrape, before a span exists. The Collector's does not
exist until it accepts a batch, so its first sample is already in the hundreds,
and under `rate()` those hundreds enter the expected side and can never enter the
received one. A stack whose two counters both read 2,142 reported every span in
the system lost. The rule takes the increase against the value five minutes ago
instead, with `or vector(0)` standing in for a series that did not exist then, so
both sides count from zero.

The expected side is then taken at its low point over the last ninety seconds. A
span counted at `on_end` has not reached the Collector yet: the batch processor
holds it for up to five seconds, and Prometheus scrapes the two targets on
schedules of their own, up to a scrape apart. Compared instant against instant,
every span still in flight is charged to the gap and the first minute of a burst
reads as total loss.

Offsetting one window a minute earlier than the other was the obvious fix and it
is wrong, which is worth knowing because it looks right for as long as you watch
a burst start. It fixes the ramp and breaks the drain: the expected window still
holds traffic the received window has already dropped, and on a burst-then-idle
cycle the gap climbed to 1.0 for a minute with both counters sitting at 4,998.
`min_over_time` has no edge. Both windows cover the same five minutes, so a
steady rate gives the same increase on each wherever it is sampled, and taking
the lower recent reading of the expected side only ever gives ground while the
rate is climbing.

The Collector's internal telemetry reader is declared explicitly in
`gateway-config.yaml` rather than left at its default, because recent releases
bind it to `localhost` inside the container. From Prometheus in another container
the scrape is refused, `received` reads zero, and the rule reports a 100 percent
ingest gap that is really an unreachable endpoint.

## Why the producer counts spans instead of requests

`SpanEmitCounter` is a `SpanProcessor` whose `on_end` increments a Prometheus
counter. It sits **before** the `BatchSpanProcessor` in the provider's processor
list, so it counts spans the SDK finished, whether or not the export that follows
succeeds.

Counting requests instead would compare a request count against a span count and
need a spans-per-request constant to bridge them, which is a number that changes
the moment anyone adds instrumentation. Counting successful exports would make
the counter agree with the Collector by construction, which is precisely the
agreement listing 9.5 exists to test.

## Why three of the images carry no healthcheck

`otel-collector`, `prometheus`, `loki` and `consumer-clickhouse` show a blank
health column in `docker compose ps`. For the first three the reason is that
those images are distroless: no shell, no `curl`, no `wget`, so there is nothing
for a `test:` line to invoke. `consumer-clickhouse` is a plain Python process
with no HTTP surface to check.

Anything that needs to know those services are ready has to ask from outside the
container, which is what the test scripts do: they poll `http://localhost:3100/ready`,
`http://localhost:9090/-/ready` and a real ClickHouse query, on a budget, rather
than sleeping a fixed number of seconds and hoping. A fixed sleep is a guess
about a machine you are not sitting at.

## Why the numbers in the README are not exact

Chapter 8's README prints numbers that reproduce on your machine, because its
population is generated deterministically and sampled with a fixed stride. This
chapter's are not, and cannot be.

The traffic here goes through a live sampler making a probabilistic decision per
trace, over however many requests you happened to drive, flushed on a 15-second
timer and scraped on another. 306 and 10 will be different numbers for you, and
the post total especially: at one in a hundred, three hundred successful requests
leave about three survivors, and three is a number with a lot of luck in it. What
reproduces is the relationship: pre above post, error counts equal on both sides
because the sampler keeps every error, and a post error rate many times the pre
one.

The one place this chapter does have an exact number is
`benchmarks/fingerprint_compression.py`, which builds its own population
server-side over a known number of code paths and then asks the listing 9.2 view
how many issues it found. `F == P` is a real result: `F` comes out of the view,
`P` is the number the generator was told to seed, and a normalization that
merges two paths or splits one misses it in a direction the script names.

What that is not is chapter 8's ground-truth discipline. Chapter 8 records a
population it then has to estimate back, so the recorded number is evidence.
Here the same constant that generates the rows is written into the truth table,
so reading `P` back out of the store returns the constant it was given: the
round trip proves the store held it, not that the generator produced it. `F` is
measured, `P` is declared, and the assertion is worth exactly the first of
those. It is still the only claim in this directory that is graded against a
number rather than compared against a direction.

## Running the book's listings verbatim

The book prints a readable excerpt and this repository ships a runnable file, so
they differ in small ways throughout: qualified table names, callout markers,
YAML that has to satisfy a real schema. Three of the differences are worth
knowing before you paste a printed listing into your own stack.

### Listing 9.2's top frame

The book prints:

```
splitByChar('\n', attributes['exception.stacktrace'])[1] AS top_frame
```

Element 1 of a Python traceback split on newlines is the literal string
`Traceback (most recent call last):`. It is identical for every exception the
process will ever raise, so the fingerprint degenerates to a hash of type and
message template alone and the top frame contributes nothing. In a service where
two different call sites raise the same exception type with the same message
shape, those two bugs become one issue and stay one issue.

The file parses the frames out and takes the innermost:

```
replaceRegexpAll(
    arrayElement(
        extractAll(attributes['exception.stacktrace'],
                   'File "[^"]*", line [0-9]+, in [A-Za-z_0-9<>.]+'),
        -1),
    ', line [0-9]+', '') AS top_frame
```

The line number is stripped on the way through. Without that, an edit anywhere
above the raise site shifts the line and forks one ongoing issue into two, one
of which is marked "first seen in this deploy". `benchmarks/fingerprint_compression.py`
varies the raise line across three values per code path to stand in for three
deploys, so dropping the strip triples the issue count, measurably.

### Listing 9.4's series name

The book reads `traces_span_metrics_calls_total`, which is what the connector
emits with no `namespace` set. This stack sets `namespace: pre` and
`namespace: post` so that the two connectors produce distinguishable series, and
that setting is what renames the counter. The rules therefore read
`pre_calls_total`.

If you copy the printed rule into a stack whose connector does set a namespace,
the expression matches nothing. It will not error. It will evaluate to an empty
vector, record an empty series, and the alert built on it will never fire.

### Listing 9.5's expected side

The book baselines the expected rate against the same hour one week earlier:

```
avg_over_time(spans:received:rate5m[1h] offset 1w)
```

That is the better choice for a system that has been running a week, and section
9.4.4 names it first. It is unusable on a stack you brought up ten minutes ago:
the offset window is empty, the recording rule produces nothing, and the ratio is
undefined. So the file uses the other input the same paragraph names, an upstream
emit counter, which is available immediately and has the additional property of
not having travelled through the Collector.

The alert arithmetic, thresholds and `for:` duration are unchanged. Swapping one
`expr` swaps the file back to the printed version once you have a week of data.

### Listing 9.5's received side

The book prints `sum(rate(otelcol_receiver_accepted_spans_total[5m]))`. The file
subtracts the value five minutes ago and divides by 300, which is the same
quantity in the same unit and is not the same measurement when the two counters
were born at different times. The ratio rule then reads the expected side through
`min_over_time`. "Why the ingest gap needs two unrelated counters"
above has the arithmetic. Copy the printed line into a stack where both counters
start at zero together and it behaves; copy it into this one and a healthy stack
reads a permanent gap.
