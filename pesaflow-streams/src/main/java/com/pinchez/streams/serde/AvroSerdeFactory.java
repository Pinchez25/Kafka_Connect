package com.pinchez.streams.serde;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Map;
import java.util.Set;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.ClassSecurityValidator;

public final class AvroSerdeFactory {

    private static final Set<String> GENERATED_PACKAGES = Set.of("com.pinchez.cdc", "com.pinchez.events");

    // Not the org.apache.avro.SERIALIZABLE_PACKAGES property: Avro reads it once, on first load.
    static {
        var trusted = ClassSecurityValidator.getGlobal();
        ClassSecurityValidator.setGlobal(
                ClassSecurityValidator.composite(trusted, type -> GENERATED_PACKAGES.contains(type.getPackageName())));
    }

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
