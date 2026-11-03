FROM flink:2.2.1-scala_2.12-java17
RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-5.0.0-2.2.jar \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar
COPY red.sql /opt/flink/sql/red.sql
