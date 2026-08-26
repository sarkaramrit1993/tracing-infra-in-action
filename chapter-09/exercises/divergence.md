# Divergence: the error rate the survivors report is not the service's

Run this from `chapter-09/`. It does not depend on the other two exercises, and
every edit below restores the file it touched, so the directory ends where it
started.

## The question

Section 9.2.4 calls it the first of the two ways sampled errors lie, and it is
the one an on-call engineer is most likely to already be looking at. A tail
sampler keeps every trace that carries an error and one in a hundred of
everything else.
Count errors over what it kept and the numerator survived whole while the
denominator was cut by a hundred, so the error rate reads many times too high.

The arithmetic is not subtle. What makes it dangerous is that nothing about the
result looks wrong. The panel has an axis, a line and a number in the right kind
of range, and the query behind it is correct in the sense that it computes
exactly what it says it computes.

This stack puts both numbers side by side. Listing 9.1's Collector config runs
`spanmetrics` twice, once on the pre-sampling fork and once after the sampler, so
the honest rate and the survivors' rate come off one workload and one process
with nothing else different between them.

## The starting state

Five helpers. `ch` runs a query, `ch_file` applies a `.sql` file, `promq` reads
one scalar out of Prometheus, `await` blocks until a series reaches a number, and
`await_collector` blocks until the Collector is answering again after a restart:

```bash
ch()      { docker compose exec -T clickhouse clickhouse-client "$@" < /dev/null; }
ch_file() { docker compose exec -T clickhouse clickhouse-client --multiquery < "$1"; }
promq()   { curl -s -G http://localhost:9090/api/v1/query --data-urlencode "query=$1" \
              | python3 -c "import sys,json;r=json.load(sys.stdin)['data']['result'];print(r[0]['value'][1] if r else 'no data')"; }
await()   { for _ in $(seq 1 180); do
              awk -v v="$(promq "$1")" -v t="$2" 'BEGIN { exit !(v + 0 >= t) }' && return 0
              sleep 2
            done
            echo "timed out: $1 never reached $2" >&2; return 1; }
await_collector() { for _ in $(seq 1 90); do
                      curl -sf -o /dev/null http://localhost:8888/metrics && return 0
                      sleep 1
                    done
                    echo "the collector did not come back" >&2; return 1; }
```

The `< /dev/null` is not decoration. Without it the client waits on a stdin that
never reaches EOF. NOTES has the detail.

Every measurement below polls. Nothing here sleeps a fixed number of seconds and
hopes, because the chain in front of a span metric is the tail sampler's
`decision_wait`, then a 15-second connector flush, then a 15-second Prometheus
scrape, and a wait tuned to one machine is a guess on any other. A poll that
times out tells you which series never arrived; a sleep that was too short tells
you the opposite of what the exercise is about.

```bash
docker compose up -d --build
docker compose ps
```

Wait for the health column to settle, then drive a workload. Ordinary traffic
fails one checkout in a hundred; the `?fail=1` requests make the error path
deterministic without changing its shape:

```bash
for _ in $(seq 1 300); do curl -s -o /dev/null http://localhost:8080/checkout; done
for _ in $(seq 1 6); do curl -s -o /dev/null "http://localhost:8080/checkout?fail=1"; done
await 'sum(post_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"})' 9
```

Nine errors: the six forced ones plus the three the 1-in-100 cadence produces
over 300 requests. The poll waits on the error series rather than on the totals,
because the totals reach Prometheus a scrape earlier and a poll that stops there
reads the errors as zero.

## Two series, one workload

Start with the totals, at the deepest span in the trace:

```bash
promq 'sum(pre_calls_total{service_name="checkout-service",span_name="fraud.score"})'
promq 'sum(post_calls_total{service_name="checkout-service",span_name="fraud.score"})'
```

```
306
14
```

306 calls in, 14 kept. The sampler dropped the other 292, which is what a sampler
is for. Now the same two series filtered to errors:

```bash
promq 'sum(pre_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"})'
promq 'sum(post_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"})'
```

```
9
9
```

Identical. Not close, equal. The `keep-errors` policy in `tail_sampling` keeps
every trace carrying an error, so the sampler dropped nothing at all from that
class. The denominator fell by a factor of twenty-two and the numerator did not
move.

Your four numbers will differ from these, and the post total most of all. At one
in a hundred, 297 successful requests leave about three survivors, and three is a
number with a lot of luck in it: this run kept five. The stack also samples
probabilistically over however many requests you drove, and the counters reset
whenever the Collector restarts. What reproduces is the relationship: pre above
post, and the two error counts equal.

## The two rates

```bash
promq 'sum(pre_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"}) / sum(pre_calls_total{service_name="checkout-service",span_name="fraud.score"})'
promq 'sum(post_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"}) / sum(post_calls_total{service_name="checkout-service",span_name="fraud.score"})'
```

```
0.029411764705882353
0.6428571428571429
```

2.9 percent against 64.3. One of those is this service's error rate. The other is
a property of how its traces were selected for storage, and there is nothing in
the query, the series, or the dashboard that says which is which.

The ratio between them has a closed form, which is worth having because it lets
you check your own numbers rather than compare them to these. With `E` errors,
`S` successes and a keep rate `s` on the successes:

```
    pre  rate = E / (E + S)
    post rate = E / (E + sS)
    inflation = (E + S) / (E + sS)
```

At `s = 0.01`, with `E = 9` and `S = 297`, the formula gives 25.6. The measured
64.3 over 2.9 is 21.9. The gap between the two is the five successful traces the
sampler happened to keep where the expected number was three: at this sample rate
the survivors' denominator is a handful of traces, so it moves, and the inflation
moves with it.

Run the packaged version of the same measurement:

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

It asserts direction and reports magnitude, for the reason the formula above
makes obvious: the magnitude is a function of two things the operator chose, so
there is no universal number to assert.

## Try this

Two edits, each changing exactly one variable. Both back the config up first and
restore it in the same section, so neither leaves anything behind.

**Raise the sample rate from 1 percent to 50.** The only thing that moves is
`s`, so watch the inflation ratio fall while the pre rate stays where it was:

```bash
cp collector/gateway-config.yaml collector/gateway-config.yaml.bak
sed -i.tmp 's/sampling_percentage: 1/sampling_percentage: 50/' collector/gateway-config.yaml
rm -f collector/gateway-config.yaml.tmp
docker compose restart otel-collector
await_collector
for _ in $(seq 1 300); do curl -s -o /dev/null http://localhost:8080/checkout; done
for _ in $(seq 1 6); do curl -s -o /dev/null "http://localhost:8080/checkout?fail=1"; done
await 'sum(post_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"})' 9
promq 'sum(pre_calls_total{service_name="checkout-service",span_name="fraud.score"})'
promq 'sum(post_calls_total{service_name="checkout-service",span_name="fraud.score"})'
promq 'sum(pre_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"}) / sum(pre_calls_total{service_name="checkout-service",span_name="fraud.score"})'
promq 'sum(post_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"}) / sum(post_calls_total{service_name="checkout-service",span_name="fraud.score"})'
```

```
306
161
0.029411764705882353
0.055900621118012424
```

2.9 percent against 5.6, an inflation of 1.90 where it was 21.9. The formula
predicts 1.94 at these counts. The lie did not go away, it got quieter, and
quieter is the more dangerous direction. At 64 percent nobody believes the panel.
At 5.6 percent against a true 2.9 the panel is wrong by a factor you would take
for noise, or for a bad afternoon, and act on. Restore:

```bash
mv collector/gateway-config.yaml.bak collector/gateway-config.yaml
docker compose restart otel-collector
```

**Delete the `keep-errors` policy.** Now the sampler treats errors like
everything else, so `s` applies to both classes:

```bash
cp collector/gateway-config.yaml collector/gateway-config.yaml.bak
python3 - <<'PY'
from pathlib import Path
p = Path("collector/gateway-config.yaml")
block = """      - name: keep-errors
        type: status_code
        status_code:
          status_codes: [ERROR]
"""
p.write_text(p.read_text().replace(block, ""))
PY
docker compose restart otel-collector
await_collector
for _ in $(seq 1 600); do curl -s -o /dev/null http://localhost:8080/checkout; done
for _ in $(seq 1 600); do curl -s -o /dev/null "http://localhost:8080/checkout?fail=1"; done
await 'sum(post_calls_total{service_name="checkout-service",span_name="fraud.score"})' 8
promq 'sum(pre_calls_total{service_name="checkout-service",span_name="fraud.score"})'
promq 'sum(post_calls_total{service_name="checkout-service",span_name="fraud.score"})'
promq 'sum(pre_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"})'
promq 'sum(post_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"})'
promq 'sum(pre_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"}) / sum(pre_calls_total{service_name="checkout-service",span_name="fraud.score"})'
promq 'sum(post_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"}) / sum(post_calls_total{service_name="checkout-service",span_name="fraud.score"})'
```

```
1173
9
579
4
0.4936061381074169
0.4444444444444444
```

49.4 percent against 44.4, an inflation of 0.90. The error count fell from 579 to
4 along with everything else, and the ratio survived. This is the case section
9.2.4 says does not break: a uniform sample scales numerator and denominator
alike and cancels in the ratio, even though the absolute counts read low.

Two things about the numbers in that block are worth saying plainly. The traffic
is half forced failures, which is not a service anyone would ship, and it is
there because one in a hundred of a realistic error count is zero: with no
`keep-errors` policy the survivors carry errors only if there were a great many
errors to begin with. And nine surviving traces is a small sample, so 44.4
against 49.4 is 0.90 rather than 1.00 for the same reason a coin lands heads five
times in nine. What is being shown is the difference between an inflation near
one and the 21.9 above, not a third decimal place.

Which is the useful way to see what the first number was really measuring. The
divergence was never caused by sampling. It was caused by sampling the two
classes at **different** rates, and a policy that protects errors is exactly such
a rate. Every error-tracking setup worth having does this, so every one of them
carries this bias. Restore:

```bash
mv collector/gateway-config.yaml.bak collector/gateway-config.yaml
docker compose restart otel-collector
```

## Clean up

Both edits above restore in place, so this is a confirmation rather than a step:

```bash
grep -c 'keep-errors' collector/gateway-config.yaml
grep -o 'sampling_percentage: [0-9]*' collector/gateway-config.yaml
ls collector/*.bak collector/*.tmp 2>/dev/null | wc -l
```

```
1
sampling_percentage: 1
       0
```

One `keep-errors` policy, the sampler back at 1 percent, and no backup or temp
file left in `collector/`.

If the last number is not zero, some edit was interrupted between its `cp` and
its `mv`. It does not have to have been one of yours: `exercises/correlation.md`
backs up the same file, so an abandoned run of either exercise leaves the same
`.bak` behind, and the remedy is the same either way.

```bash
mv collector/gateway-config.yaml.bak collector/gateway-config.yaml
```

Then confirm the Collector is running the file that shipped:

```bash
docker compose restart otel-collector
await_collector
for _ in $(seq 1 200); do curl -s -o /dev/null http://localhost:8080/checkout; done
await 'sum(post_calls_total{service_name="checkout-service"})' 7
promq 'sum(pre_calls_total{service_name="checkout-service"}) > sum(post_calls_total{service_name="checkout-service"})'
```

```
891
```

Any number rather than `no data` means the comparison held: the pre series is
above the post series again, which is only true when the sampler sits on the far
side of the pre connector. The number itself is whatever the pre total has
reached, so it depends on how much traffic this Collector process has seen since
it last restarted.

This exercise never wrote to ClickHouse, so there is nothing to delete there.

## Going deeper

`collector/gateway-config.yaml` is listings 9.1 and 9.3 with their annotations,
including why the tail sampler is plain probabilistic and does not stamp a
`tracestate` threshold on what it keeps. NOTES covers what would change if it
did: a downstream that can read the threshold can weight its counts back up, and
the divergence disappears.

**Cause the failure on purpose.** Move the `spanmetrics/pre` connector to the
sampled fork. This is the mistake the whole listing exists to prevent, and it is
one line in each of two pipelines:

```bash
cp collector/gateway-config.yaml collector/gateway-config.yaml.bak
python3 - <<'PY'
from pathlib import Path
p = Path("collector/gateway-config.yaml")
t = p.read_text()
t = t.replace("exporters: [spanmetrics/pre, servicegraph, forward]",
              "exporters: [servicegraph, forward]")
t = t.replace("exporters: [kafka, spanmetrics/post]",
              "exporters: [kafka, spanmetrics/post, spanmetrics/pre]")
p.write_text(t)
PY
docker compose restart otel-collector
await_collector
for _ in $(seq 1 300); do curl -s -o /dev/null http://localhost:8080/checkout; done
for _ in $(seq 1 6); do curl -s -o /dev/null "http://localhost:8080/checkout?fail=1"; done
await 'sum(post_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"})' 9
promq 'sum(pre_calls_total{service_name="checkout-service",span_name="fraud.score"})'
promq 'sum(post_calls_total{service_name="checkout-service",span_name="fraud.score"})'
promq 'sum(pre_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"}) / sum(pre_calls_total{service_name="checkout-service",span_name="fraud.score"})'
promq 'sum(post_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"}) / sum(post_calls_total{service_name="checkout-service",span_name="fraud.score"})'
```

```
14
14
0.6428571428571429
0.6428571428571429
```

Both series identical, both at 64 percent, and the Collector booted clean. The
config is valid YAML, every component name resolves, no log line complains, and
`benchmarks/sampler_divergence.py` is the only thing anywhere that notices: it
exits non-zero with "expected pre total above post total".

That is the shape worth carrying away. A connector on the wrong side of a
processor is not a syntax error and not a runtime error. It produces a dashboard
that agrees with itself, and two series matching looks like corroboration when it
is the strongest available evidence that both are measuring the sample. Restore:

```bash
mv collector/gateway-config.yaml.bak collector/gateway-config.yaml
docker compose restart otel-collector
```

Two more if the sampler itself interests you.

Set `decision_wait` below the p99 of a checkout, around 150ms, and the sampler
starts deciding on traces before their last span arrives. Late spans arrive after
the verdict and are dropped whatever the verdict was, so the post series loses
spans from traces it decided to keep, and the two error counts stop matching for
a reason that has nothing to do with the error rate.

And add a second `probabilistic` policy alongside the first. Tail-sampling
policies are ORed rather than ANDed, so two 1-percent policies keep roughly 1.99
percent and not 0.01 percent. It is the most common way a sampling config ends up
keeping far more than its author intended, and the only visible symptom is a
storage bill.
