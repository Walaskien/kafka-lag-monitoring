# Stock Apache Kafka (KRaft) plus the Prometheus JMX exporter as a java agent.
FROM alpine:3.22 AS jmx
ARG JMX_VERSION=1.0.1
ARG JMX_SHA256=7d61f737fd661610ccc14aea79764faa1ea94a340cbc8f0029b3d2edea3d80c1
RUN wget -q -O /jmx_prometheus_javaagent.jar \
      "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_VERSION}/jmx_prometheus_javaagent-${JMX_VERSION}.jar" \
 && echo "${JMX_SHA256}  /jmx_prometheus_javaagent.jar" | sha256sum -c -

FROM apache/kafka:3.9.1
COPY --from=jmx /jmx_prometheus_javaagent.jar /opt/jmx/jmx_prometheus_javaagent.jar
COPY jmx-exporter.yml /opt/jmx/jmx-exporter.yml
