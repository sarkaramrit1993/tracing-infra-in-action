FROM flink:2.2.1-scala_2.12-java17

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-venv python3-dev build-essential && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# PyFlink wheel that matches Flink 2.2.1. --prefer-binary so pip takes wheels
# where they exist (numpy etc.); build-essential + python3-dev above let any
# source-only deps (e.g. pemja, which has no linux/arm64 wheel) compile.
RUN pip3 install --break-system-packages --no-cache-dir --prefer-binary \
        apache-flink==2.2.1 \
        opentelemetry-proto==1.42.1 \
        'protobuf>=4.25,<6'

COPY assembly_job.py /opt/flink/jobs/assembly_job.py

# DataStream Kafka connector for Flink 2.2. The DataStream KafkaSource/KafkaSink
# API needs the thin flink-connector-kafka jar, not the flink-sql-connector
# fat jar (that one is built for the Table/SQL API). The thin jar does not
# bundle kafka-clients, so we add the matching kafka-clients 4.2.0 jar
# (flink-connector-kafka-parent 5.0.0-2.2 pins kafka.version=4.2.0).
RUN curl -fsSL -o /opt/flink/lib/flink-connector-kafka.jar \
        https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/5.0.0-2.2/flink-connector-kafka-5.0.0-2.2.jar && \
    curl -fsSL -o /opt/flink/lib/kafka-clients.jar \
        https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/4.2.0/kafka-clients-4.2.0.jar

# Prometheus metrics reporter. In Flink 2.x it ships as a plugin under
# /opt/flink/plugins/metrics-prometheus/. Copy the jar onto the flat lib
# classpath so the PrometheusReporterFactory named in config.yaml resolves
# and the reporter binds port 9249.
RUN cp /opt/flink/plugins/metrics-prometheus/flink-metrics-prometheus-*.jar \
       /opt/flink/lib/

# The checkpoint dir is a named volume; create it owned by the flink user so the
# JobManager can write checkpoint state (a fresh named volume inherits this
# ownership). Without this the job fails: "Failed to create directory ... /shared".
RUN mkdir -p /flink-checkpoints && chown -R flink:flink /flink-checkpoints

USER flink
