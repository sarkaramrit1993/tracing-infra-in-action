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

Every step below is a script in `scripts/`. Each one prints what it found, and
each wait polls the data itself instead of sleeping a fixed number of seconds,
because the chain in front of a span metric is the tail sampler's
`decision_wait`, then a 15-second connector flush, then a 15-second Prometheus
scrape, and a wait tuned to one machine is a guess on any other.

A run of any of the three exercises that was stopped between a backup and its
restore leaves a `.bak` beside the file it edited, and a container still running
the edited copy. Put every such file back before anything else, whichever
exercise left it:

```bash
./scripts/restore-edited-files.sh
```

```
nothing to restore: every file is the one that shipped
```

Then bring the stack up, and restart the two services that read a config file
mounted from here, so neither keeps running a copy that was just put back:

```bash
docker compose up -d --build
docker compose restart otel-collector loki
docker compose ps
```

Drive a workload. Ordinary traffic fails one checkout in a hundred; the
`?fail=1` requests make the error path deterministic without changing its shape:

```bash
docker compose restart otel-collector
./scripts/send-traffic.sh
./scripts/wait-until-ready.sh
```

```
sending 300 ordinary checkouts and 6 forced failures...
sent 306 checkouts, 9 of them failed
waiting for the Collector to receive all 2142 spans the app sent... ok
waiting for the tail sampler to decide all 306 traces... ok
waiting for span metrics to reach Prometheus... ok
waiting for the 98 kept spans to reach ClickHouse... ok
ready
```

The restart zeroes the connector counters, so every number below counts this
workload and nothing before it. `send-traffic.sh` waits for the Collector to come
back before it sends anything.

Nine errors: the six forced ones plus the three the 1-in-100 cadence produces
over 300 requests.

## Two series, one workload

Count calls and errors at the deepest span in the trace, `fraud.score`, on both
sides of the sampler:

```bash
./scripts/compare-error-rates.sh
```

```
                  calls  errors  error rate
before sampler      306       9        2.9%
after sampler        14       9       64.3%
```

It runs four PromQL sums, the `pre_calls_total` and `post_calls_total` series
for `service_name="checkout-service",span_name="fraud.score"`, each once as is
and once filtered to `status_code="STATUS_CODE_ERROR"`, and divides errors by
calls.

Start with the calls column. 306 calls in, 14 kept. The sampler dropped the
other 292, which is what a sampler is for. Now the errors column: 9 before, 9
after.

Identical. Not close, equal. The `keep-errors` policy in `tail_sampling` keeps
every trace carrying an error, so the sampler dropped nothing at all from that
class. The denominator fell by a factor of twenty-two and the numerator did not
move.

Your four numbers will differ from these, and the post total most of all. The
table is the committed reference run in `RESULTS.md`, the same one the README
prints. At one in a hundred, 297 successful requests leave about three
survivors, and three is a number with a lot of luck in it: that run kept five.
The post total is nine plus that draw, so it lands between 9 and 16 on all but
about one run in a hundred. What reproduces is the relationship: pre above post,
and the two error counts equal.

## The two rates

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

At `s = 0.01`, with `E = 9` and `S = 297`, the formula gives 25.6. The reference
run's 64.3 over 2.9 is 21.9. The gap between the two is the five successful traces the
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
./scripts/send-traffic.sh
./scripts/wait-until-ready.sh
./scripts/compare-error-rates.sh
```

```
                  calls  errors  error rate
before sampler      306       9        2.9%
after sampler       161       9        5.6%
```

2.9 percent against 5.6, an inflation of 1.90 where it was 21.9. The formula
predicts 1.94 at these counts. At 50 percent every successful trace is a coin
toss, so the block is one draw and the post total moves between runs: nine plus
half of 297 on average, landing between 141 and 175 on about 98 runs in a hundred,
which puts the post rate between 5.1 and 6.4 percent. The lie did not go away,
it got quieter, and quieter is the more dangerous direction. At 64 percent nobody believes the panel.
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
./scripts/send-traffic.sh 600 600
./scripts/wait-until-ready.sh
./scripts/compare-error-rates.sh
```

```
                  calls  errors  error rate
before sampler     1200     606       50.5%
after sampler        12       7       58.3%
```

Twelve hundred requests with 606 of them failing are both deterministic: 600 are
forced, and the 1-in-100 cadence adds exactly six more inside the 600 plain ones.
`wait-until-ready.sh` does not need to know which policy is in force: it waits
for the sampler to decide every trace and for the counts to be scraped after
that, so it works the same with `keep-errors` gone.

50.5 percent against 58.3, an inflation of 1.15. The error count fell from 606 to
7 along with everything else, and the ratio came back near where it started. This
is the case section 9.2.4 says does not break: a uniform sample scales numerator
and denominator alike and cancels in the ratio, even though the absolute counts
read low.

Two things about the numbers in that block are worth saying plainly. The traffic
is half forced failures, which is not a service anyone would ship, and it is
there because one in a hundred of a realistic error count is zero: with no
`keep-errors` policy the survivors carry errors only if there were a great many
errors to begin with. And twelve surviving traces is a small sample, so 58.3
against 50.5 is 1.15 rather than 1.00 for the same reason a coin lands heads
seven times in twelve. The block is one draw. The kept count is one percent of
1,200, twelve on average and between 5 and 20 on about 98 runs in a hundred, and
about half of what is kept is errors, so your inflation will land somewhere either
side of one, between 0.5 and 1.5 on about nine runs in ten. What is being shown is
the difference between an inflation near one and the 21.9 above, not a third
decimal place.

Which is the useful way to see what the first number was really measuring. The
divergence was never caused by sampling. It was caused by sampling the two
classes at **different** rates, and a policy that protects errors is exactly such
a rate. Every error-tracking setup worth having does this, so every one of them
carries this bias. Restore:

```bash
mv collector/gateway-config.yaml.bak collector/gateway-config.yaml
docker compose restart otel-collector
```

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
./scripts/send-traffic.sh
./scripts/wait-until-ready.sh
./scripts/compare-error-rates.sh
python3 benchmarks/sampler_divergence.py
echo "exit $?"
```

```
                  calls  errors  error rate
before sampler       14       9       64.3%
after sampler        14       9       64.3%
[divergence] Prometheus=http://localhost:9090 grain=checkout-service/fraud.score
[divergence] pre : total=14 errors=9 rate=64.286%
[divergence] post: total=14 errors=9 rate=64.286%
[divergence] expected pre total 14 > post total 14 (the sampler drops spans)
exit 1
```

Both series identical, and the Collector booted clean. The block is one draw and
the shared rate is whatever the survivors happen to hold: nine errors over nine
plus the kept successes, anywhere from about 56 percent to 100, because both
series now count the same handful of kept traces. The config is valid YAML, every component name resolves, and no log line complains.
The benchmark is the only thing anywhere that notices, which is why the block
runs it: its direction assertion is the one statement in this repository that a
connector on the wrong side of the sampler cannot satisfy.

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

## Clean up

Every edit above restores in place, so this is a confirmation rather than a
step:

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
`.bak` behind, and the remedy is the same either way. This puts back whatever is
there, restarts what reads it, and says so:

```bash
./scripts/restore-edited-files.sh
```

Then confirm the Collector is running the file that shipped:

```bash
docker compose restart otel-collector
./scripts/send-traffic.sh 200 0
./scripts/wait-until-ready.sh
./scripts/compare-error-rates.sh
```

```
                  calls  errors  error rate
before sampler      200       2        1.0%
after sampler         4       2       50.0%
```

The before row has more calls than the after row again, which is only true when
the sampler sits on the far side of the pre connector. Both rows showing the
same calls is the wrong-side failure from Going deeper, both series counting the
same survivors. The after row is a draw: two failures plus whichever successes
the sampler kept.

This exercise never wrote to ClickHouse, so there is nothing to delete there.
