FROM alpine:3.24 AS avro-converter

SHELL ["/bin/ash", "-eo", "pipefail", "-c"]

ARG AVRO_CONVERTER_VERSION=8.2.0
ARG AVRO_CONVERTER_SHA256=7c05f4fb33e00be5cd4bc3ae042a8fd890ef7ecc02d82e13ed294fceb6a61a15

RUN wget -q -O /tmp/converter.zip \
        "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-avro-converter/versions/${AVRO_CONVERTER_VERSION}/confluentinc-kafka-connect-avro-converter-${AVRO_CONVERTER_VERSION}.zip" \
    && echo "${AVRO_CONVERTER_SHA256}  /tmp/converter.zip" | sha256sum -c - \
    && unzip -q /tmp/converter.zip -d /tmp \
    && mv "/tmp/confluentinc-kafka-connect-avro-converter-${AVRO_CONVERTER_VERSION}/lib" /avro-converter

FROM alpine:3.24 AS http-sink

SHELL ["/bin/ash", "-eo", "pipefail", "-c"]

ARG HTTP_SINK_VERSION=0.9.0
ARG HTTP_SINK_SHA256=72fea14ea3c58a5c9d08e08c5dae2404cb5b04a8f73206c5d717d97e13fbbf07

RUN wget -q -O /tmp/http-sink.tar \
        "https://github.com/Aiven-Open/http-connector-for-apache-kafka/releases/download/v${HTTP_SINK_VERSION}/http-connector-for-apache-kafka-${HTTP_SINK_VERSION}.tar" \
    && echo "${HTTP_SINK_SHA256}  /tmp/http-sink.tar" | sha256sum -c - \
    && tar -xf /tmp/http-sink.tar -C /tmp \
    && mv "/tmp/http-connector-for-apache-kafka-${HTTP_SINK_VERSION}" /http-sink

FROM alpine:3.24 AS jmx-exporter

SHELL ["/bin/ash", "-eo", "pipefail", "-c"]

ARG JMX_EXPORTER_VERSION=1.3.0
ARG JMX_EXPORTER_SHA256=ba74aad73934e59f4ca9c0779b49f1663c1fc7a81d5d99fc665bd5e5039310f5

RUN wget -q -O /jmx_prometheus_javaagent.jar \
        "https://github.com/prometheus/jmx_exporter/releases/download/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar" \
    && echo "${JMX_EXPORTER_SHA256}  /jmx_prometheus_javaagent.jar" | sha256sum -c -

FROM quay.io/debezium/connect:3.6.1.Final

COPY --from=avro-converter /avro-converter /kafka/connect/confluent-avro-converter
COPY --from=http-sink /http-sink /kafka/connect/aiven-http-sink
COPY --from=jmx-exporter /jmx_prometheus_javaagent.jar /kafka/libs/jmx_prometheus_javaagent-1.3.0.jar
