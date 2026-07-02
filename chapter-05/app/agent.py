"""
Chapter 5: Standalone agent-tier helper.

Most readers run the OTel Collector agent container from collector/agent-config.yaml
and never need this file. It exists for the case where a reader wants to reproduce
the agent tier from chapter 4 outside docker-compose (a Kubernetes sidecar smoke
test, a laptop debugging session) without lifting the full Collector binary.

It accepts OTLP gRPC traffic on :4317, batches, and forwards to the gateway.
"""

import os
import time
import logging
from concurrent import futures

logging.basicConfig(level=logging.INFO, format="%(asctime)s [agent] %(message)s")
log = logging.getLogger("agent")

GATEWAY = os.environ.get("OTEL_GATEWAY", "otel-gateway:4317")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "512"))
BATCH_TIMEOUT_S = float(os.environ.get("BATCH_TIMEOUT_S", "1.0"))


def serve():
    import grpc
    from opentelemetry.proto.collector.trace.v1 import trace_service_pb2_grpc
    from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
        ExportTraceServiceRequest,
        ExportTraceServiceResponse,
    )

    class ForwardingTraceService(trace_service_pb2_grpc.TraceServiceServicer):
        def __init__(self, gateway: str):
            self.channel = grpc.insecure_channel(gateway)
            self.stub = trace_service_pb2_grpc.TraceServiceStub(self.channel)

        def Export(self, request: ExportTraceServiceRequest, context):
            # Smoke-test helper, not a production agent: it forwards inline with
            # no local buffer. If the gateway forward fails we surface the error
            # to the caller instead of returning success, so the upstream SDK
            # retries rather than silently dropping spans. A real agent tier
            # would buffer and retry here (see collector/agent-config.yaml).
            try:
                self.stub.Export(request, timeout=5)
            except grpc.RpcError as e:
                log.warning("forward to gateway failed: %s", e.code())
                context.set_code(e.code())
                context.set_details("forward to gateway failed")
                return ExportTraceServiceResponse()
            return ExportTraceServiceResponse()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    trace_service_pb2_grpc.add_TraceServiceServicer_to_server(
        ForwardingTraceService(GATEWAY), server)
    server.add_insecure_port("0.0.0.0:4317")
    server.start()
    log.info("agent listening on :4317, forwarding to %s", GATEWAY)
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    serve()
