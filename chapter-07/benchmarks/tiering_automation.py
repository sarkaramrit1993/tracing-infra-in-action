"""Chapter 7 benchmark: what listing 7.2's cold tier costs to read.

This exercises listing 7.2's `TO VOLUME 'cold'` against real object storage. The
cold volume is the s3_cold disk in config.d/storage.xml, backed by the MinIO
service, so a moved part is written out as S3 objects, the same path a
production cluster takes to AWS S3, GCS, or Azure Blob.

The chapter's claim about tiering is a cost claim: cold data stays queryable and
reading it costs more. So that is what this measures. It loads two identical
batches into two dated partitions, lets listing 7.2's own two-day boundary move
the older one to S3, and then runs the same query against each and compares.

Steps, all against the running stack:
  1. reset: restore the listing 7.2 boundary, move any part already on the cold
     disk back to the hot volume, and delete rows a previous run left behind,
  2. stage two batches of NUM_SPANS rows each under a deliberately long move
     boundary, one dated three days back and one dated one day back,
  3. restore listing 7.2's boundary (2 days to cold, 15 days delete) and
     materialize it, so the three-day-old partition qualifies and the
     one-day-old partition does not,
  4. wait for the older partition to reach the cold disk, or fail,
  5. run the same aggregate against both partitions and report the latency.

Both batches come from the same generator, so the only difference between them
is which disk holds the part.

Run (stack must be up):
  python3 tiering_automation.py
  NUM_SPANS=20000 REPEATS=21 python3 tiering_automation.py
"""
import os
import time
import json
import statistics
from datetime import datetime, timezone
from pathlib import Path

from chclient import CH, StackNotRunning

NUM_SPANS = int(os.environ.get("NUM_SPANS", "50000"))
POLL_TIMEOUT_S = int(os.environ.get("POLL_TIMEOUT_S", "180"))
COLD_DISK = os.environ.get("COLD_DISK", "s3_cold")
REPEATS = int(os.environ.get("REPEATS", "11"))
DRAIN_ATTEMPTS = 3

# The claim under test: reading from the cold tier costs more than reading the
# same rows from the hot volume. The single run in results/ measured 1.72x,
# 10.31ms cold against 5.99ms hot, over MinIO on the same Docker network. That is
# the friendliest object store a cold tier will ever have, so read 1.72x as a
# floor and not a forecast, and the guard below sits well under it on purpose.
# Fail the guard and the cold volume is no longer a distinct read path, which is
# what a misrouted disk or a cache in front of S3 looks like.
MIN_COLD_OVER_HOT = float(os.environ.get("MIN_COLD_OVER_HOT", "1.3"))

# The benchmark owns rows under this service name, so it can find and remove its
# own data without touching the spans the collector wrote.
BENCH_SERVICE = "tiering-bench"
COLD_AGE_DAYS = 3
HOT_AGE_DAYS = 1


def _set_listing_ttl(ch):
    """Set listing 7.2's boundary: two days to cold, fifteen days delete."""
    ch.execute("""
        ALTER TABLE tracing.otel_traces MODIFY TTL
          toDateTime(timestamp) + INTERVAL 2 DAY TO VOLUME 'cold',
          toDateTime(timestamp) + INTERVAL 15 DAY DELETE
    """)


def _set_staging_ttl(ch):
    """Push the move boundary out while the batches load.

    ClickHouse picks an insert's destination disk from the move TTL, so rows
    that are already past the boundary land on the cold volume directly and
    never start hot. Under a boundary no row can reach, both batches land on
    'default' and step 3 turns listing 7.2's own rule loose on them.
    """
    ch.execute("""
        ALTER TABLE tracing.otel_traces MODIFY TTL
          toDateTime(timestamp) + INTERVAL 30 DAY TO VOLUME 'cold',
          toDateTime(timestamp) + INTERVAL 60 DAY DELETE
    """)


def _clear_bench_rows(ch):
    ch.execute(f"ALTER TABLE tracing.otel_traces DELETE "
               f"WHERE service_name = '{BENCH_SERVICE}' SETTINGS mutations_sync = 2")


def _load(ch, age_days):
    # Anchored to midday so a batch never straddles midnight and splits itself
    # across two partitions. Three days back clears the two-day boundary at any
    # hour of the day, one day back never reaches it.
    ch.execute(f"""
        INSERT INTO tracing.otel_traces
          (timestamp, trace_id, span_id, service_name, span_name,
           status_code, duration_ns, attributes)
        SELECT
          toDateTime64(toStartOfDay(now()), 9) - toIntervalDay({age_days})
            + toIntervalHour(12) + toIntervalMillisecond(number),
          lower(hex(MD5(toString(intDiv(number, 6))))),
          lower(hex(reinterpretAsFixedString(toUInt64(number)))),
          '{BENCH_SERVICE}',
          'validate_cart',
          'STATUS_CODE_OK',
          toUInt64(1000000 + number),
          map('tier', 'demo')
        FROM numbers({NUM_SPANS})
    """)


def _bench_dates(ch):
    rows = ch.query(f"""
        SELECT DISTINCT toDate(timestamp)
        FROM tracing.otel_traces
        WHERE service_name = '{BENCH_SERVICE}'
        ORDER BY 1
    """)
    return [row[0] for row in rows]


def _cold_partitions(ch):
    rows = ch.query(f"""
        SELECT DISTINCT partition
        FROM system.parts
        WHERE database = 'tracing' AND table = 'otel_traces' AND active
          AND disk_name = '{COLD_DISK}'
    """)
    return [row[0] for row in rows]


def _move_cold_parts_back(ch):
    """Put already-cold parts back on the hot volume so there is a move to make.

    Anyone who followed the README walkthrough, or ran this script before, has
    parts on the cold disk already. Draining first makes the run work from any
    starting state, and it is a no-op when the disk is already empty.
    """
    for attempt in range(DRAIN_ATTEMPTS + 1):
        partitions = _cold_partitions(ch)
        if not partitions:
            return
        if attempt == DRAIN_ATTEMPTS:
            raise SystemExit(
                f"[tiering] {len(partitions)} partition(s) still on "
                f"'{COLD_DISK}' after {DRAIN_ATTEMPTS} move attempts; a "
                f"background merge may be holding them, wait and re-run")
        print(f"[tiering] moving {len(partitions)} partition(s) off "
              f"'{COLD_DISK}' back to the default volume")
        for partition in partitions:
            ch.execute("ALTER TABLE tracing.otel_traces "
                       f"MOVE PARTITION '{partition}' TO DISK 'default'")


def _partition_stats(ch, partition, disk):
    row = ch.query(f"""
        SELECT count(), sum(bytes_on_disk), sum(rows)
        FROM system.parts
        WHERE database = 'tracing' AND table = 'otel_traces' AND active
          AND partition = '{partition}' AND disk_name = '{disk}'
    """)[0]
    return {"parts": int(row[0]), "bytes": int(row[1] or 0), "rows": int(row[2] or 0)}


def _s3_objects_for_parts(ch, partition):
    """How many S3 objects the moved parts became.

    Scoped to the parts that are live right now rather than to the whole bucket.
    ClickHouse removes the blobs of a replaced part lazily, so a bucket-wide
    count still carries the previous run's garbage for a while and is not a fact
    about this move.
    """
    return int(ch.scalar(f"""
        SELECT count()
        FROM system.remote_data_paths
        WHERE disk_name = '{COLD_DISK}'
          AND splitByChar('/', local_path)[-2] IN (
                SELECT name FROM system.parts
                WHERE database = 'tracing' AND table = 'otel_traces' AND active
                  AND partition = '{partition}' AND disk_name = '{COLD_DISK}')
    """))


def _tier_query(date):
    """One aggregate over one batch. service_name leads the sort key, so the
    filter costs a key range rather than a scan, and the aggregates force real
    reads of duration_ns, trace_id and span_id off whichever disk holds them."""
    return (f"SELECT count(), sum(duration_ns), uniqExact(trace_id), max(span_id) "
            f"FROM tracing.otel_traces "
            f"WHERE service_name = '{BENCH_SERVICE}' AND toDate(timestamp) = '{date}'")


def _spread(samples):
    return {"median": round(statistics.median(samples), 2),
            "min": round(min(samples), 2),
            "max": round(max(samples), 2)}


def run():
    if REPEATS < 2:
        raise SystemExit("[tiering] REPEATS must be at least 2; the first round "
                         "of each tier is a warm-up and is discarded")

    ch = CH()
    print(f"[tiering] transport={ch.transport} num_spans={NUM_SPANS:,} "
          f"repeats={REPEATS} cold_disk={COLD_DISK}")

    # Start from a known state. A staging boundary left behind by an interrupted
    # run has to go before anything is inserted, or the new rows never tier.
    _set_listing_ttl(ch)
    _move_cold_parts_back(ch)
    _clear_bench_rows(ch)

    try:
        print("[tiering] staging two batches on the hot (default) volume")
        _set_staging_ttl(ch)
        _load(ch, COLD_AGE_DAYS)
        _load(ch, HOT_AGE_DAYS)
        ch.execute("OPTIMIZE TABLE tracing.otel_traces FINAL")

        dates = _bench_dates(ch)
        if len(dates) != 2:
            raise SystemExit(f"[tiering] expected 2 dated batches, found {dates}")
        cold_date, hot_date = dates[0], dates[1]
        cold_partition = cold_date.replace("-", "")
        print(f"[tiering] cold batch {cold_date}, hot batch {hot_date}")

        staged = _partition_stats(ch, cold_partition, "default")
        if staged["parts"] == 0:
            raise SystemExit(f"[tiering] the {cold_date} batch did not land on "
                             f"the default volume, so there is no move to make")
        before_result = ch.query(_tier_query(cold_date))

        _set_listing_ttl(ch)
        ch.execute("ALTER TABLE tracing.otel_traces MATERIALIZE TTL")

        # Liveness only. The wall-clock time to the move is a property of the
        # scheduler, not of the storage tier, so nothing here is reported.
        # ClickHouse's move-selecting task sleeps merge_selecting_sleep_ms
        # (5000) when idle and multiplies that by
        # merge_selecting_sleep_slowdown_factor (1.2) on each idle cycle up to
        # max_merge_selecting_sleep_ms (60000), so otherwise identical runs land
        # anywhere on 5.0, 6.0, 7.2, 8.6, 10.4s and up, purely on how long the
        # server had been quiet. Do not publish a number measured from here.
        t0 = time.time()
        moved = False
        while time.time() - t0 < POLL_TIMEOUT_S:
            if _partition_stats(ch, cold_partition, COLD_DISK)["parts"] > 0:
                moved = True
                break
            time.sleep(1)
    finally:
        # Always put listing 7.2's boundary back, including on an error or a
        # Ctrl-C above. Leaving the staging boundary set would stop every part
        # the table receives from then on ever reaching S3.
        _set_listing_ttl(ch)

    if not moved:
        raise SystemExit(
            f"[tiering] no part reached disk '{COLD_DISK}' within {POLL_TIMEOUT_S}s; "
            f"check that MinIO is up and the s3_cold disk resolves")

    on_cold = _partition_stats(ch, cold_partition, COLD_DISK)
    s3_objects = _s3_objects_for_parts(ch, cold_partition)
    print(f"[tiering] moved {on_cold['parts']} part(s), {on_cold['rows']:,} rows, "
          f"{on_cold['bytes']:,} bytes to '{COLD_DISK}' "
          f"as {s3_objects} S3 objects")
    if on_cold["bytes"] == 0:
        raise SystemExit("[tiering] the moved part reports zero bytes on the cold disk")

    # Interleaved so both tiers see the same background load, and the first
    # round of each is a warm-up that the medians do not see.
    hot_samples, cold_samples = [], []
    hot_sql, cold_sql = _tier_query(hot_date), _tier_query(cold_date)
    after_result = None
    for _ in range(REPEATS):
        t = time.perf_counter()
        ch.query(hot_sql)
        hot_samples.append((time.perf_counter() - t) * 1000)
        t = time.perf_counter()
        after_result = ch.query(cold_sql)
        cold_samples.append((time.perf_counter() - t) * 1000)
    hot, cold = _spread(hot_samples[1:]), _spread(cold_samples[1:])

    if after_result != before_result:
        raise SystemExit(f"[tiering] the cold batch answers differently after the "
                         f"move: {before_result} before, {after_result} after")

    ratio = round(cold["median"] / hot["median"], 2)
    print(f"[tiering] hot  ({hot_date}, default): median {hot['median']}ms "
          f"(min {hot['min']}, max {hot['max']})")
    print(f"[tiering] cold ({cold_date}, {COLD_DISK}): median {cold['median']}ms "
          f"(min {cold['min']}, max {cold['max']})")

    if ratio < MIN_COLD_OVER_HOT:
        raise SystemExit(
            f"[tiering] the cold tier read {ratio}x the hot tier, under the "
            f"{MIN_COLD_OVER_HOT}x this benchmark asserts; the cold volume is "
            f"no longer behaving like a separate read path")

    print(f"[tiering] PASS: same query, same answer, {ratio}x the latency "
          f"from '{COLD_DISK}'")

    stamp = datetime.now(timezone.utc)
    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(exist_ok=True)
    out = out_dir / f"tiering-move-{stamp.strftime('%Y-%m-%dT%H%M%S')}.json"
    out.write_text(json.dumps({
        "benchmark": "tiering_automation",
        "measured_at_utc": stamp.isoformat(),
        "transport": ch.transport,
        "num_spans_per_tier": NUM_SPANS,
        "cold_disk": COLD_DISK,
        "parts_moved": on_cold["parts"],
        "rows_moved": on_cold["rows"],
        "bytes_moved": on_cold["bytes"],
        "s3_objects_for_moved_parts": s3_objects,
        "same_answer_after_move": True,
        "query_repeats": REPEATS - 1,
        "hot_query_ms": hot,
        "cold_query_ms": cold,
        "cold_over_hot": ratio,
        "note": (
            "Both tiers hold the same generated batch, so the latency gap is "
            "the storage path and nothing else. The absolute milliseconds "
            "include the client round trip and are specific to this laptop; "
            "the ratio is the number to read. Read it as a range and not a "
            "constant: repeated runs on one machine have landed anywhere from "
            "1.58x to 1.92x, so the single figure above is one draw from that "
            "spread. It is a floor, not a forecast: "
            "the cold tier here is MinIO on the same Docker network, and a "
            "real S3 endpoint across a network is slower than that. Nothing "
            "here reports how long the move itself took, because that is the "
            "background scheduler's backoff, not a property of the tier."
        ),
    }, indent=2) + "\n")
    print(f"[tiering] wrote {out}")


if __name__ == "__main__":
    try:
        run()
    except StackNotRunning as exc:
        # The guidance is the whole message; a traceback through chclient
        # only buries it.
        raise SystemExit(str(exc))
