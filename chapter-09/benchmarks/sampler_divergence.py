"""Chapter 9 benchmark: the pre/post sampler divergence, packaged as a measurement.

The keystone gateway config runs span metrics twice: `spanmetrics/pre` counts
100% of spans before the tail sampler, `spanmetrics/post` counts only the
survivors and weighs each as 1. The tail sampler keeps every error trace but only
one success in a hundred, so the POST error rate reads inflated against the PRE
error rate, which is the ground truth. That divergence is the whole reason
chapter 9 says: derive insights from stream-time aggregates computed before you
sample, never from the sampled survivors.

This script reads both series from Prometheus for one (service, span) grain,
computes both error rates, and writes a dated JSON to results/. It is mechanism
framed: it asserts only the DIRECTION (post rate > pre rate). The magnitude is a
function of the injected error ratio and the sample rate, so the script reports
both rates and their ratio and never a universal constant.

Run (stack must be up, after some /checkout traffic and a spanmetrics flush):
  python sampler_divergence.py
  SERVICE=checkout-service SPAN=fraud.score python sampler_divergence.py
"""
import os
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

PROM = os.environ.get("PROMETHEUS_URL", "http://localhost:9090")
SERVICE = os.environ.get("SERVICE", "checkout-service")
# fraud.score is the deepest span, where checkout.py injects the 1-in-100 error,
# so the divergence is cleanest there. Set SPAN="" to measure the whole service.
SPAN = os.environ.get("SPAN", "fraud.score")
# The burn-rate rule selects SPAN_KIND_SERVER so its ratio is per request rather
# than per span. Nothing recorded here exercised that selector, so the figure the
# rule is justified by had no artifact behind it. Set KIND=SPAN_KIND_SERVER with
# SPAN="" to measure at the grain the rule actually runs at.
KIND = os.environ.get("KIND", "")


def promq(query: str) -> float:
    """Sum the instant-vector result of a PromQL query (0.0 if it is empty)."""
    url = f"{PROM}/api/v1/query?" + urllib.parse.urlencode({"query": query})
    with urllib.request.urlopen(url, timeout=30) as resp:
        result = json.load(resp)["data"]["result"]
    return sum(float(x["value"][1]) for x in result) if result else 0.0


def _selector(status: str = "") -> str:
    parts = [f'service_name="{SERVICE}"']
    if SPAN:
        parts.append(f'span_name="{SPAN}"')
    if KIND:
        parts.append(f'span_kind="{KIND}"')
    if status:
        parts.append(f'status_code="{status}"')
    return "{" + ",".join(parts) + "}"


def run():
    grain = f"{SERVICE}/{SPAN}" if SPAN else SERVICE
    if KIND:
        grain = f"{grain} [{KIND}]"
    print(f"[divergence] Prometheus={PROM} grain={grain}")

    sel, sel_err = _selector(), _selector("STATUS_CODE_ERROR")
    pre_total = promq(f"sum(pre_calls_total{sel})")
    post_total = promq(f"sum(post_calls_total{sel})")
    pre_err = promq(f"sum(pre_calls_total{sel_err})")
    post_err = promq(f"sum(post_calls_total{sel_err})")

    if pre_total <= 0 or post_total <= 0:
        raise SystemExit(
            "[divergence] pre or post span metrics are empty. Drive /checkout "
            "traffic and wait for the 15s spanmetrics flush + a Prometheus scrape, "
            "then rerun.")

    pre_rate = pre_err / pre_total
    post_rate = post_err / post_total

    print(f"[divergence] pre : total={pre_total:.0f} errors={pre_err:.0f} "
          f"rate={pre_rate:.3%}")
    print(f"[divergence] post: total={post_total:.0f} errors={post_err:.0f} "
          f"rate={post_rate:.3%}")

    # Direction only: the sampler drops non-error survivors, so post over-represents
    # errors. Magnitude is f(injected error ratio, sample rate), reported not asserted.
    if not pre_total > post_total:
        raise SystemExit(
            f"[divergence] expected pre total {pre_total:.0f} > post total "
            f"{post_total:.0f} (the sampler drops spans)")
    if not post_rate > pre_rate:
        raise SystemExit(
            f"[divergence] expected post rate {post_rate:.3%} > pre rate "
            f"{pre_rate:.3%} (keep-errors inflates the survivors' error rate)")

    inflation = (post_rate / pre_rate) if pre_rate > 0 else None
    infl_str = f"x{inflation:.1f}" if inflation else "n/a (no pre errors yet)"
    print(f"[divergence] PASS: post error rate > pre error rate (inflation {infl_str}); "
          f"pre total > post total")

    stamp = datetime.now(timezone.utc)
    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(exist_ok=True)
    out = out_dir / f"sampler-divergence-{stamp.strftime('%Y-%m-%dT%H%M%S')}.json"
    out.write_text(json.dumps({
        "benchmark": "sampler_divergence",
        "measured_at_utc": stamp.isoformat(),
        "prometheus": PROM,
        "grain": {"service": SERVICE, "span": SPAN or "(all spans)",
                  "span_kind": KIND or "(all kinds)"},
        "pre": {"total": pre_total, "errors": pre_err, "error_rate": round(pre_rate, 6)},
        "post": {"total": post_total, "errors": post_err, "error_rate": round(post_rate, 6)},
        "inflation_ratio": round(inflation, 3) if inflation else None,
        "note": "Direction is the claim: post error rate > pre error rate, and pre "
                "total > post total. The magnitude is a function of the injected "
                "error ratio and the sample rate, not a universal constant.",
    }, indent=2) + "\n")
    print(f"[divergence] wrote {out}")


if __name__ == "__main__":
    run()
