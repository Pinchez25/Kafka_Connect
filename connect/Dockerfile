FROM alpine:3.22 AS avro-converter

ARG AVRO_CONVERTER_VERSION=8.2.0
ARG AVRO_CONVERTER_SHA256=7c05f4fb33e00be5cd4bc3ae042a8fd890ef7ecc02d82e13ed294fceb6a61a15

RUN wget -q -O /tmp/converter.zip \
        "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-avro-converter/versions/${AVRO_CONVERTER_VERSION}/confluentinc-kafka-connect-avro-converter-${AVRO_CONVERTER_VERSION}.zip" \
    && echo "${AVRO_CONVERTER_SHA256}  /tmp/converter.zip" | sha256sum -c - \
    && unzip -q /tmp/converter.zip -d /tmp \
    && mv "/tmp/confluentinc-kafka-connect-avro-converter-${AVRO_CONVERTER_VERSION}/lib" /avro-converter

FROM quay.io/debezium/connect:3.6.1.Final

COPY --from=avro-converter /avro-converter /kafka/connect/confluent-avro-converter
