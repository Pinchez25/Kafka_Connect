package com.pinchez.streams.processor;

import com.pinchez.cdc.Transaction;
import java.time.Duration;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

/**
 * Drops a CDC row whose transaction id was already seen. Ids live in a window store keyed by the row's event time, so a
 * redelivered row (same id, same created_at) is found with a single-timestamp fetch, and old ids expire with their
 * segment instead of needing a cleanup job.
 */
@Slf4j
public final class DeduplicationProcessor implements FixedKeyProcessor<String, Transaction, Transaction> {

    private final String storeName;
    private FixedKeyProcessorContext<String, Transaction> context;
    private WindowStore<String, Long> seen;
    private Sensor duplicatesDropped;

    private DeduplicationProcessor(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(FixedKeyProcessorContext<String, Transaction> context) {
        this.context = context;
        this.seen = context.getStateStore(storeName);
        this.duplicatesDropped = context.metrics()
                .addRateTotalSensor(
                        "pesaflow-deduplication",
                        context.taskId().toString(),
                        "duplicates-dropped",
                        Sensor.RecordingLevel.INFO);
    }

    @Override
    public void process(FixedKeyRecord<String, Transaction> record) {
        if (record.key() == null || record.value() == null) {
            context.forward(record);
            return;
        }
        var eventTime = record.timestamp();
        try (var earlier = seen.fetch(record.key(), eventTime, eventTime)) {
            if (earlier.hasNext()) {
                duplicatesDropped.record();
                log.debug(
                        "Dropped duplicate transaction {} (Debezium op {}) from {}",
                        record.key(),
                        PesaflowHeaders.read(record.headers(), PesaflowHeaders.DEBEZIUM_OP)
                                .orElse("?"),
                        source());
                return;
            }
        }
        seen.put(record.key(), context.currentSystemTimeMs(), eventTime);
        context.forward(record.withHeaders(PesaflowHeaders.with(record.headers(), PesaflowHeaders.SOURCE, source())));
    }

    @Override
    public void close() {
        context.metrics().removeSensor(duplicatesDropped);
    }

    private String source() {
        return context.recordMetadata()
                .map(metadata -> "%s-%d@%d".formatted(metadata.topic(), metadata.partition(), metadata.offset()))
                .orElse("unknown");
    }

    public record Supplier(String storeName, Duration retention)
            implements FixedKeyProcessorSupplier<String, Transaction, Transaction> {

        @Override
        public FixedKeyProcessor<String, Transaction, Transaction> get() {
            return new DeduplicationProcessor(storeName);
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            return Set.of(Stores.windowStoreBuilder(
                    Stores.persistentWindowStore(storeName, retention, retention, false),
                    Serdes.String(),
                    Serdes.Long()));
        }
    }
}
