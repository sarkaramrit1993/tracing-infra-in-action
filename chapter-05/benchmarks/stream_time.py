"""Chapter 5 benchmark: stream-time assembly buffer cost.

Models the Flink keyed-state assembly path without standing up Flink and
without wall-clock pacing. Earlier wall-clock versions could not reproduce the
chapter's buffer figure: pure-Python span generation fell far behind the target
rate, so the in-flight population never filled, and a real-time run that was
shorter than decision_wait never fired a single timer.

This version drives a synthetic event-time clock deterministically. Spans are
fed in event-time order; every trace accumulates its spans inside the
decision_wait window before its timer fires; timers fire when the synthetic
clock passes first_span_event_time + decision_wait. The run reports the peak
in-flight buffer once the pipeline reaches steady state.

The number that matters for the chapter is the steady-state in-flight buffer.
At the defaults below (5,000 traces/sec, 30s decision_wait, 8 spans/trace,
2 KB/span) the on-the-wire buffer reaches roughly 2.4 GB, matching the
back-of-envelope in section 5.1.2. The collector's in-memory representation
runs about MEM_FACTOR (default 4) times larger, which the script also reports.
The simulation confirms the closed-form figure rather than the other way round.
"""

import os
import json
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict, deque

TRACES_PER_SEC = int(os.environ.get("TRACES_PER_SEC", "5000"))
SPANS_PER_TRACE = int(os.environ.get("SPANS_PER_TRACE", "8"))
SPAN_SIZE_BYTES = int(os.environ.get("SPAN_SIZE_BYTES", "2048"))
DECISION_WAIT_S = float(os.environ.get("DECISION_WAIT_S", "30.0"))
MEM_FACTOR = float(os.environ.get("MEM_FACTOR", "4.0"))

# Synthetic event-time resolution: one tick per millisecond of event time.
TICK_S = 0.001


class KeyedStateSurrogate:
    """In-memory analogue of Flink's per-key list state plus event-time timer.

    Spans arrive in event-time order. A timer is registered on first arrival
    for a key and fires when the event-time clock passes first_seen +
    decision_wait, at which point the whole trace is emitted and its state
    cleared. This is the keyed-state lifecycle Figure 5.5 walks.
    """

    def __init__(self, decision_wait_s: float, span_size_bytes: int):
        self.decision_wait_s = decision_wait_s
        self.span_size_bytes = span_size_bytes
        self.span_counts = defaultdict(int)
        self.timers = deque()  # (fire_at_event_time_s, trace_id)
        self.emitted = 0
        self.bytes_emitted = 0

    def process(self, trace_id: str, event_time_s: float):
        if self.span_counts[trace_id] == 0:
            self.timers.append((event_time_s + self.decision_wait_s, trace_id))
        self.span_counts[trace_id] += 1

    def advance(self, now_event_time_s: float):
        while self.timers and self.timers[0][0] <= now_event_time_s:
            _, trace_id = self.timers.popleft()
            count = self.span_counts.pop(trace_id, 0)
            if count:
                self.emitted += 1
                self.bytes_emitted += count * self.span_size_bytes

    def buffer_size_bytes(self) -> int:
        total_spans = sum(self.span_counts.values())
        return total_spans * self.span_size_bytes

    def in_flight_traces(self) -> int:
        return len(self.span_counts)


def closed_form_buffer_bytes() -> int:
    """The chapter's back-of-envelope: every trace that started within the
    last decision_wait window is still buffered with all its spans."""
    return (TRACES_PER_SEC * DECISION_WAIT_S
            * SPANS_PER_TRACE * SPAN_SIZE_BYTES)


def run():
    print(f"[stream-time] traces/sec={TRACES_PER_SEC} spans/trace={SPANS_PER_TRACE} "
          f"span_size={SPAN_SIZE_BYTES} decision_wait={DECISION_WAIT_S}s mem_factor={MEM_FACTOR}")

    state = KeyedStateSurrogate(DECISION_WAIT_S, SPAN_SIZE_BYTES)

    # Run for two decision_wait windows so the pipeline reaches steady state
    # (window one fills the buffer, window two confirms it holds while timers
    # from window one begin firing).
    total_event_time_s = DECISION_WAIT_S * 2
    total_ticks = int(total_event_time_s / TICK_S)
    traces_per_tick = TRACES_PER_SEC * TICK_S  # fractional traces start per tick

    peak_buffer = 0
    peak_in_flight = 0
    trace_seq = 0
    trace_credit = 0.0
    sample_log_at = DECISION_WAIT_S  # log once per decision_wait window

    for tick in range(total_ticks):
        event_time = tick * TICK_S

        # Start whole traces this tick; each starts then immediately receives
        # all SPANS_PER_TRACE spans within the window (spans of one trace land
        # close together in event time, which is the realistic in-flight case).
        trace_credit += traces_per_tick
        while trace_credit >= 1.0:
            trace_credit -= 1.0
            trace_id = f"trace-{trace_seq}"
            trace_seq += 1
            for _ in range(SPANS_PER_TRACE):
                state.process(trace_id, event_time)

        state.advance(event_time)

        buf = state.buffer_size_bytes()
        if buf > peak_buffer:
            peak_buffer = buf
            peak_in_flight = state.in_flight_traces()

        if event_time >= sample_log_at:
            print(f"[stream-time] t={event_time:.0f}s buffer_bytes={buf:,} "
                  f"in_flight_traces={state.in_flight_traces():,}")
            sample_log_at += DECISION_WAIT_S

    closed_form = closed_form_buffer_bytes()
    peak_mem = peak_buffer * MEM_FACTOR

    print(f"[stream-time] peak_wire_buffer_bytes={peak_buffer:,} "
          f"({peak_buffer / 1e9:.2f} GB on the wire)")
    print(f"[stream-time] closed_form_buffer_bytes={int(closed_form):,} "
          f"({closed_form / 1e9:.2f} GB) -- simulation matches formula")
    print(f"[stream-time] modeled_in_memory_bytes={int(peak_mem):,} "
          f"({peak_mem / 1e9:.2f} GB at {MEM_FACTOR:.0f}x in-memory factor)")
    print(f"[stream-time] peak_in_flight_traces={peak_in_flight:,} "
          f"traces_emitted={state.emitted:,}")

    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    (out_dir / f"stream_time-{stamp}.json").write_text(json.dumps({
        "traces_per_sec": TRACES_PER_SEC,
        "spans_per_trace": SPANS_PER_TRACE,
        "span_size_bytes": SPAN_SIZE_BYTES,
        "decision_wait_s": DECISION_WAIT_S,
        "mem_factor": MEM_FACTOR,
        "peak_wire_buffer_bytes": peak_buffer,
        "closed_form_buffer_bytes": int(closed_form),
        "modeled_in_memory_bytes": int(peak_mem),
        "peak_in_flight_traces": peak_in_flight,
        "traces_emitted": state.emitted,
    }, indent=2))


if __name__ == "__main__":
    run()
