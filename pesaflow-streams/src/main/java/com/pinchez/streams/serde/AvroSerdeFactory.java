package com.pinchez.streams.serde;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;

public final class AvroSerdeFactory {

    private final Map<String, String> config;

    public AvroSerdeFactory(String schemaRegistryUrl) {
        this.config = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    }

    public <T extends SpecificRecord> SpecificAvroSerde<T> valueSerde() {
        var serde = new SpecificAvroSerde<T>();
        serde.configure(config, false);
        return serde;
    }
}
