"""Chapter 7 benchmark: TTL automation moves aged parts to the S3 cold tier.

This exercises listing 7.2's `TO VOLUME 'cold'` end to end against real object
storage. The cold volume is the s3_cold disk in config.d/storage.xml, which is
backed by the MinIO service, so a moved part is written out as S3 objects, the
same path a production cluster takes to AWS S3, GCS, or Azure Blob.

Steps, all measured against the running stack:
  1. insert a small recent batch (lands on the default hot volume),
  2. wait AGE_SECONDS so the rows age past a short move boundary,
  3. set the TTL move boundary to a few seconds (listing 7.2 uses two days; the
     demo shortens it so the move is observable in one run) and materialize it,
  4. poll system.parts until an active part's disk_name flips from 'default' to
     's3_cold', timing how long the move takes,
  5. restore the listing 7.2 boundary (2 days to cold, 15 days delete).

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


def run():
    ch = CH()
    print(f"[tiering] transport={ch.transport} num_spans={NUM_SPANS:,} "
          f"move_ttl={MOVE_TTL_SECONDS}s cold_disk={COLD_DISK}")

    print("[tiering] inserting recent batch on the hot (default) volume")
    _insert_recent_batch(ch)
    before = _disk_counts(ch)
    print(f"[tiering] parts by disk before move: {before}")
    if before.get("default", 0) == 0:
        raise SystemExit("[tiering] no parts on the default volume to move")

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

    # Restore the listing 7.2 boundary (two days to cold, fifteen days delete).
    ch.execute("""
        ALTER TABLE tracing.otel_traces MODIFY TTL
          toDateTime(timestamp) + INTERVAL 2 DAY TO VOLUME 'cold',
          toDateTime(timestamp) + INTERVAL 15 DAY DELETE
    """)

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
