"""Chapter 7 benchmark: TTL automation moves aged parts to the S3 cold tier.

This exercises listing 7.2's `TO VOLUME 'cold'` end to end against real object
storage. The cold volume is the s3_cold disk in config.d/storage.xml, which is
backed by the MinIO service, so a moved part is written out as S3 objects, the
same path a production cluster takes to AWS S3, GCS, or Azure Blob.

Steps, all measured against the running stack:
  1. reset the table: restore the listing 7.2 boundary, then move any part that
     is already on the cold disk back to the hot volume, so the run measures a
     real move whatever state the walkthrough or a previous run left behind,
  2. insert a small recent batch (lands on the default hot volume),
  3. wait AGE_SECONDS so the rows age past a short move boundary,
  4. set the TTL move boundary to a few seconds (listing 7.2 uses two days; the
     demo shortens it so the move is observable in one run) and materialize it,
  5. poll system.parts until an active part's disk_name flips from 'default' to
     's3_cold', timing how long the move takes,
  6. restore the listing 7.2 boundary (2 days to cold, 15 days delete).

The reported latency is the wall-clock time from issuing the ALTER to the first
part appearing on the S3 disk, so it includes the S3 upload, not just metadata.

Run (stack must be up):
  python tiering_automation.py
  NUM_SPANS=20000 MOVE_TTL_SECONDS=5 python tiering_automation.py
"""
import os
import time
import json
from datetime import datetime, timezone
from pathlib import Path

from chclient import CH

NUM_SPANS = int(os.environ.get("NUM_SPANS", "50000"))
AGE_SECONDS = int(os.environ.get("AGE_SECONDS", "12"))
MOVE_TTL_SECONDS = int(os.environ.get("MOVE_TTL_SECONDS", "10"))
POLL_TIMEOUT_S = int(os.environ.get("POLL_TIMEOUT_S", "180"))
COLD_DISK = os.environ.get("COLD_DISK", "s3_cold")
DRAIN_ATTEMPTS = 3


def _insert_recent_batch(ch):
    ch.execute(f"""
        INSERT INTO tracing.otel_traces
          (timestamp, trace_id, span_id, service_name, span_name,
           status_code, duration_ns, attributes)
        SELECT
          now64(9),
          lower(hex(MD5(toString(intDiv(number, 6))))),
          lower(hex(reinterpretAsFixedString(toUInt64(number)))),
          'checkout-service',
          'validate_cart',
          'STATUS_CODE_OK',
          toUInt64(1000000 + number),
          map('tier', 'demo')
        FROM numbers({NUM_SPANS})
    """)
    ch.execute("OPTIMIZE TABLE tracing.otel_traces FINAL")


def _disk_counts(ch):
    rows = ch.query("""
        SELECT disk_name, count()
        FROM system.parts
        WHERE database = 'tracing' AND table = 'otel_traces' AND active
        GROUP BY disk_name
    """)
    return {name: int(n) for name, n in rows}


def _set_listing_ttl(ch):
    """Set listing 7.2's boundary: two days to cold, fifteen days delete."""
    ch.execute("""
        ALTER TABLE tracing.otel_traces MODIFY TTL
          toDateTime(timestamp) + INTERVAL 2 DAY TO VOLUME 'cold',
          toDateTime(timestamp) + INTERVAL 15 DAY DELETE
    """)


def _cold_partitions(ch):
    rows = ch.query(f"""
        SELECT DISTINCT partition
        FROM system.parts
        WHERE database = 'tracing' AND table = 'otel_traces' AND active
          AND disk_name = '{COLD_DISK}'
    """)
    return [row[0] for row in rows]


def _move_cold_parts_back(ch):
    """Put already-cold parts back on the hot volume so there is a move to time.

    Anyone who followed the README walkthrough, or ran this script before, has
    every part on the cold disk already. The OPTIMIZE in _insert_recent_batch
    then merges the fresh hot rows into that cold part, and nothing is left on
    'default' to move. Draining first makes the run work from any starting
    state, and it is a no-op when the disk is already empty.
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


def run():
    ch = CH()
    print(f"[tiering] transport={ch.transport} num_spans={NUM_SPANS:,} "
          f"move_ttl={MOVE_TTL_SECONDS}s cold_disk={COLD_DISK}")

    # Start from a known state. The short boundary from an interrupted run has
    # to go before anything is inserted, or the new rows ship straight to cold.
    _set_listing_ttl(ch)
    _move_cold_parts_back(ch)

    print("[tiering] inserting recent batch on the hot (default) volume")
    _insert_recent_batch(ch)
    before = _disk_counts(ch)
    print(f"[tiering] parts by disk before move: {before}")
    if before.get("default", 0) == 0:
        raise SystemExit("[tiering] no parts on the default volume to move")

    try:
        print(f"[tiering] aging rows {AGE_SECONDS}s past the {MOVE_TTL_SECONDS}s boundary")
        time.sleep(AGE_SECONDS)

        ch.execute(f"""
            ALTER TABLE tracing.otel_traces MODIFY TTL
              toDateTime(timestamp) + INTERVAL {MOVE_TTL_SECONDS} SECOND TO VOLUME 'cold',
              toDateTime(timestamp) + INTERVAL 15 DAY DELETE
        """)
        ch.execute("ALTER TABLE tracing.otel_traces MATERIALIZE TTL")

        t0 = time.time()
        moved = False
        counts = before
        while time.time() - t0 < POLL_TIMEOUT_S:
            counts = _disk_counts(ch)
            if counts.get(COLD_DISK, 0) > 0:
                moved = True
                break
            time.sleep(1)
        elapsed = round(time.time() - t0, 2)

        print(f"[tiering] parts by disk after move: {counts}")
    finally:
        # Always put listing 7.2's boundary back, including on an error or a
        # Ctrl-C in the poll above. Leaving the few-second boundary set would
        # ship every part the table receives from then on straight to S3.
        _set_listing_ttl(ch)

    if not moved:
        raise SystemExit(
            f"[tiering] no part reached disk '{COLD_DISK}' within {POLL_TIMEOUT_S}s; "
            f"check that MinIO is up and the s3_cold disk resolves")
    print(f"[tiering] PASS: part moved to '{COLD_DISK}' in {elapsed}s")

    stamp = datetime.now(timezone.utc)
    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(exist_ok=True)
    out = out_dir / f"tiering-move-{stamp.strftime('%Y-%m-%d')}.json"
    out.write_text(json.dumps({
        "benchmark": "tiering_automation",
        "measured_at_utc": stamp.isoformat(),
        "transport": ch.transport,
        "num_spans": NUM_SPANS,
        "move_ttl_seconds": MOVE_TTL_SECONDS,
        "cold_disk": COLD_DISK,
        "parts_by_disk_before": before,
        "parts_by_disk_after": counts,
        "move_latency_seconds": elapsed,
        "note": (
            "parts_by_disk_after is the snapshot taken the moment the first "
            "part reaches the cold disk, which is also where "
            "move_latency_seconds stops. A remaining count on 'default' at "
            "that instant is expected and does not indicate an incomplete "
            "move: parts age and migrate individually."
        ),
    }, indent=2) + "\n")
    print(f"[tiering] wrote {out}")


if __name__ == "__main__":
    run()
