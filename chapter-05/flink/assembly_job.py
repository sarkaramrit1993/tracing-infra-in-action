"""
Chapter 5: Stream-time trace assembly job (PyFlink 2.2).

Listing 5.2 plus listing 5.4 made concrete. The job:

1. Reads OTLP-encoded spans from the otlp_spans Kafka topic.
2. Assigns event-time watermarks with 5 seconds of bounded out-of-orderness.
3. Keys by trace_id and buffers spans in keyed list state.
4. Registers a per-trace event-time timer on first arrival, set to fire after
   DECISION_WAIT_MS past the first span event time.
5. On timer fire, serializes the buffered spans back into one OTLP TracesData
   payload and emits it to the traces.assembled topic. The keyed state clears
   in the same operator step (atomicity imperative). An emitted-tombstone
   ValueState records that the trace already shipped so a later span for the
   same trace can never re-open it and emit a second single-span "trace".
6. Late spans (event time older than the current watermark, or any span that
   arrives after the trace already shipped) route to the spans.late topic via
   a side output. They are never re-injected into the main pipeline.

Delivery guarantee: both Kafka sinks run DeliveryGuarantee.EXACTLY_ONCE with a
transactional id prefix. The source-sink coordination ties the Kafka offset
commit and the assembled-trace emit into the same checkpoint barrier, matching
the chapter prose. Downstream consumers of traces.assembled / spans.late must
set isolation.level=read_committed to honor that guarantee.
"""

import os
import logging
from typing import Iterable

from pyflink.common import WatermarkStrategy, Duration, Types, Time
from pyflink.common.serialization import ByteArraySchema
from pyflink.datastream import (
    StreamExecutionEnvironment,
    KeyedProcessFunction,
    ProcessFunction,
    OutputTag,
    RuntimeContext,
)
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
    DeliveryGuarantee,
)
from pyflink.datastream.state import (
    ListStateDescriptor,
    ValueStateDescriptor,
    StateTtlConfig,
)
from pyflink.common.typeinfo import Types as PyTypes

from opentelemetry.proto.trace.v1.trace_pb2 import TracesData, ResourceSpans, ScopeSpans, Span
from opentelemetry.proto.common.v1.common_pb2 import KeyValue, AnyValue
from opentelemetry.proto.resource.v1.resource_pb2 import Resource

logging.basicConfig(level=logging.INFO, format="%(asctime)s [flink-assembly] %(message)s")
log = logging.getLogger("assembly")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka-1:9093,kafka-2:9093,kafka-3:9093")
SOURCE_TOPIC = os.environ.get("SOURCE_TOPIC", "otlp_spans")
ASSEMBLED_TOPIC = os.environ.get("ASSEMBLED_TOPIC", "traces.assembled")
LATE_TOPIC = os.environ.get("LATE_TOPIC", "spans.late")
DECISION_WAIT_MS = int(os.environ.get("DECISION_WAIT_MS", "10000"))
OUT_OF_ORDER_SEC = int(os.environ.get("OUT_OF_ORDER_SEC", "5"))
# Parallelism cannot exceed the available task slots. This stack runs one
# TaskManager slot, so parallelism is 1; raise both together to scale out.
PARALLELISM = int(os.environ.get("PARALLELISM", "1"))

LATE_TAG = OutputTag("late-spans", Types.PICKLED_BYTE_ARRAY())

# Tombstone state for emitted traces lives at most this long after a trace
# ships. Long enough to catch the stragglers that drift in behind a lagging
# watermark, short enough that the keyed-state footprint stays bounded.
EMITTED_TTL_MIN = int(os.environ.get("EMITTED_TTL_MIN", "10"))


def _bytes_schema() -> ByteArraySchema:
    """Public PyFlink raw-bytes (de)serializer for the Kafka source and sinks.

    The gateway writes spans as binary protobuf (encoding=otlp_proto). A string
    schema would re-encode those bytes as UTF-8 and corrupt the payload before
    ParseFromString sees it, so source and sink both use the byte-array schema
    and the protobuf parser works on the raw bytes. One instance is fine: the
    schema is stateless and Flink serializes it to each operator.
    """
    return ByteArraySchema()


def _explode_payload(raw: bytes) -> Iterable[bytes]:
    """OTLP TracesData payload contains many spans across resource_spans.
    For keyed-state assembly we need one record per span, each carrying its
    own trace_id, event-time stamp, and the resource and scope context needed
    to reconstitute the original payload on emit.
    """
    td = TracesData()
    td.ParseFromString(raw)
    for rs in td.resource_spans:
        for ss in rs.scope_spans:
            for span in ss.spans:
                single = TracesData()
                sr = single.resource_spans.add()
                if rs.HasField("resource"):
                    sr.resource.CopyFrom(rs.resource)
                sr.schema_url = rs.schema_url
                ss_new = sr.scope_spans.add()
                if ss.HasField("scope"):
                    ss_new.scope.CopyFrom(ss.scope)
                ss_new.schema_url = ss.schema_url
                ss_new.spans.add().CopyFrom(span)
                yield single.SerializeToString()


def _trace_id_of(payload: bytes) -> str:
    td = TracesData()
    td.ParseFromString(payload)
    for rs in td.resource_spans:
        for ss in rs.scope_spans:
            for s in ss.spans:
                return s.trace_id.hex()
    return ""


def _event_time_ms_of(payload: bytes) -> int:
    td = TracesData()
    td.ParseFromString(payload)
    for rs in td.resource_spans:
        for ss in rs.scope_spans:
            for s in ss.spans:
                return int(s.start_time_unix_nano / 1_000_000)
    return 0


class SpanExploder(ProcessFunction):
    """Turns one OTLP envelope into one record per span."""

    def process_element(self, value: bytes, ctx: 'ProcessFunction.Context'):
        for single in _explode_payload(value):
            yield single


class SpanTimestampAssigner:
    """Used inside WatermarkStrategy.with_timestamp_assigner."""

    def extract_timestamp(self, value: bytes, record_timestamp: int) -> int:
        return _event_time_ms_of(value)


class TraceAssembler(KeyedProcessFunction):
    """Listing 5.2 made concrete in PyFlink.

    Per-key state:
      - spans       list of single-span OTLP payloads buffered for this trace
      - fire_at_ms  event-time fire deadline for the per-trace timer
      - emitted     tombstone (true) once the trace has shipped; TTL-expired so
                    the keyed-state footprint stays bounded
    """

    def open(self, ctx: RuntimeContext):
        self.spans = ctx.get_list_state(ListStateDescriptor(
            "spans", PyTypes.PICKLED_BYTE_ARRAY()))
        self.fire_at = ctx.get_state(ValueStateDescriptor(
            "fire_at_ms", PyTypes.LONG()))

        # Tombstone that survives the spans/fire_at clear so a late re-arrival
        # cannot re-open the trace. TTL bounds the leak: once no straggler can
        # plausibly still arrive, the tombstone expires and the key drops out.
        ttl = (StateTtlConfig
               .new_builder(Time.minutes(EMITTED_TTL_MIN))
               .set_update_type(
                   StateTtlConfig.UpdateType.OnCreateAndWrite)
               .set_state_visibility(
                   StateTtlConfig.StateVisibility.NeverReturnExpired)
               .cleanup_full_snapshot()
               .build())
        emitted_desc = ValueStateDescriptor(
            "emitted", PyTypes.BOOLEAN())
        emitted_desc.enable_time_to_live(ttl)
        self.emitted = ctx.get_state(emitted_desc)

    def process_element(self, value: bytes, ctx: 'KeyedProcessFunction.Context'):
        # The trace already shipped. Any span arriving now is a straggler that
        # the bounded watermark did not classify as late (the watermark lags
        # the slowest partition). Re-buffering it would register a fresh timer
        # and emit a SECOND single-span "trace", which is exactly the
        # atomicity-imperative violation the chapter warns against. Route it to
        # the late side output and stop.
        if self.emitted.value():
            yield LATE_TAG, value
            return

        # Late spans route to side output; the atomicity imperative says the
        # assembled trace already shipped or is about to ship, so we never
        # re-open a closed trace from a late arrival.
        watermark = ctx.timer_service().current_watermark()
        event_ms = ctx.timestamp() or 0
        if watermark > 0 and event_ms < watermark:
            yield LATE_TAG, value
            return

        self.spans.add(value)
        if self.fire_at.value() is None:
            t = event_ms + DECISION_WAIT_MS
            self.fire_at.update(t)
            ctx.timer_service().register_event_time_timer(t)

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        buffered = list(self.spans.get() or [])
        if not buffered:
            self.fire_at.clear()
            return
        merged = self._merge_spans(buffered)
        yield merged
        # Mark the trace emitted before clearing the buffer, then clear keyed
        # state and the timer in the same operator step. Flink's checkpoint
        # barrier rides this boundary so the Kafka offset commit and the
        # assembled-trace emit move forward together. The tombstone outlives
        # this clear (TTL-bounded) so a late re-arrival cannot re-emit.
        self.emitted.update(True)
        self.spans.clear()
        self.fire_at.clear()

    def _merge_spans(self, buffered: Iterable[bytes]) -> bytes:
        out = TracesData()
        # Group by (resource fingerprint, scope fingerprint) so the emitted
        # payload mirrors the OTLP wire shape upstream.
        by_resource: dict = {}
        for raw in buffered:
            td = TracesData()
            td.ParseFromString(raw)
            for rs in td.resource_spans:
                rkey = rs.resource.SerializeToString() if rs.HasField("resource") else b""
                by_resource.setdefault((rkey, rs.schema_url), []).append(rs)

        for (rkey, schema_url), resource_spans in by_resource.items():
            merged_rs = out.resource_spans.add()
            merged_rs.schema_url = schema_url
            if rkey:
                merged_rs.resource.CopyFrom(resource_spans[0].resource)
            by_scope: dict = {}
            for rs in resource_spans:
                for ss in rs.scope_spans:
                    skey = ss.scope.SerializeToString() if ss.HasField("scope") else b""
                    by_scope.setdefault((skey, ss.schema_url), []).extend(ss.spans)
            for (skey, sschema), spans in by_scope.items():
                merged_ss = merged_rs.scope_spans.add()
                merged_ss.schema_url = sschema
                if skey:
                    merged_ss.scope.ParseFromString(skey)
                for s in spans:
                    merged_ss.spans.add().CopyFrom(s)
        return out.SerializeToString()


def build_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(PARALLELISM)
    env.enable_checkpointing(30_000)

    source = (KafkaSource.builder()
              .set_bootstrap_servers(KAFKA_BOOTSTRAP)
              .set_topics(SOURCE_TOPIC)
              .set_group_id("flink-trace-assembly")
              # earliest() so a fresh run assembles the spans already in the
              # topic (a reader who just brought the stack up wants to see the
              # backlog processed, not only spans produced after job start).
              # On restart, checkpointed offsets take over.
              .set_starting_offsets(KafkaOffsetsInitializer.earliest())
              .set_value_only_deserializer(_bytes_schema())
              .build())

    # No watermarks on the source: it carries the un-exploded multi-span
    # envelope, the wrong granularity for event time. Event time only makes
    # sense per span, so watermarks get assigned once, on the exploded stream.
    raw = env.from_source(
        source, WatermarkStrategy.no_watermarks(), "kafka-otlp-spans")

    watermark = (WatermarkStrategy
                 .for_bounded_out_of_orderness(Duration.of_seconds(OUT_OF_ORDER_SEC))
                 .with_timestamp_assigner(SpanTimestampAssigner()))

    spans = (raw
             .process(SpanExploder(), output_type=PyTypes.PICKLED_BYTE_ARRAY())
             .assign_timestamps_and_watermarks(watermark))

    assembled = (spans
                 .key_by(_trace_id_of, key_type=PyTypes.STRING())
                 .process(TraceAssembler(), output_type=PyTypes.PICKLED_BYTE_ARRAY())
                 .name("trace-assembly"))

    # Exactly-once sinks. The transactional-id prefix scopes the producer
    # transactions per sink; the transaction timeout must exceed the checkpoint
    # interval plus restart slack and stay under the broker's
    # transaction.max.timeout.ms. Downstream consumers must set
    # isolation.level=read_committed to read only committed transactions.
    assembled_sink = (KafkaSink.builder()
                      .set_bootstrap_servers(KAFKA_BOOTSTRAP)
                      .set_record_serializer(
                          KafkaRecordSerializationSchema.builder()
                          .set_topic(ASSEMBLED_TOPIC)
                          .set_value_serialization_schema(_bytes_schema())
                          .build())
                      .set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)
                      .set_transactional_id_prefix("ch5-assembled")
                      .set_property("transaction.timeout.ms", "900000")
                      .build())
    assembled.sink_to(assembled_sink).name("kafka-assembled-sink")

    late = assembled.get_side_output(LATE_TAG)
    late_sink = (KafkaSink.builder()
                 .set_bootstrap_servers(KAFKA_BOOTSTRAP)
                 .set_record_serializer(
                     KafkaRecordSerializationSchema.builder()
                     .set_topic(LATE_TOPIC)
                     .set_value_serialization_schema(_bytes_schema())
                     .build())
                 .set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)
                 .set_transactional_id_prefix("ch5-late")
                 .set_property("transaction.timeout.ms", "900000")
                 .build())
    late.sink_to(late_sink).name("kafka-late-sink")

    env.execute("chapter5-trace-assembly")


if __name__ == "__main__":
    build_job()
