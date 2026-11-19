package com.pinchez.streams.topology.fraud;

import com.pinchez.events.EnrichedTransaction;
import com.pinchez.events.FraudAlert;
import com.pinchez.events.FraudRule;
import com.pinchez.events.TransactionType;
import com.pinchez.streams.config.StreamsProperties;
import com.pinchez.streams.mapper.FraudAlertMapper;
import com.pinchez.streams.serde.AvroSerdeFactory;
import com.pinchez.streams.topology.CdcSources;
import com.pinchez.streams.topology.StoreNames;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;

/** Tumbling window + count + suppress: more than N transfers from one account in a fixed window. */
public final class TransferVelocityRule implements FraudRuleTopology {

    private final StreamsProperties.Fraud.TransferVelocity settings;
    private final AvroSerdeFactory serdes;

    public TransferVelocityRule(StreamsProperties.Fraud.TransferVelocity settings, AvroSerdeFactory serdes) {
        this.settings = settings;
        this.serdes = serdes;
    }

    @Override
    public KStream<String, FraudAlert> detect(KStream<String, EnrichedTransaction> enriched, CdcSources sources) {
        return enriched.filter(
                        (_, transaction) -> transaction.getType() == TransactionType.P2P, Named.as("velocity-p2p-only"))
                .groupByKey(Grouped.with("velocity-p2p-by-account", Serdes.String(), serdes.valueSerde()))
                .windowedBy(TimeWindows.ofSizeAndGrace(settings.window(), settings.grace()))
                .count(
                        Named.as("velocity-count"),
                        Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(StoreNames.TRANSFER_VELOCITY)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long())
                                .withRetention(settings.window().plus(settings.grace())))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded())
                        .withName("velocity-final-count"))
                .toStream(Named.as("velocity-windows"))
                .filter((_, count) -> count != null && count > settings.maxTransfers(), Named.as("velocity-exceeded"))
                .map(
                        (window, count) -> KeyValue.pair(
                                window.key(),
                                FraudAlertMapper.alert(
                                                FraudRule.TRANSFER_VELOCITY,
                                                window.key(),
                                                window.window().endTime(),
                                                window.window().start())
                                        .setDescription("%d transfers within %s (limit %d)"
                                                .formatted(
                                                        count,
                                                        FraudAlertMapper.humanize(settings.window()),
                                                        settings.maxTransfers()))
                                        .setWindowStart(window.window().startTime())
                                        .setWindowEnd(window.window().endTime())
                                        .setTransactionCount(count)
                                        .build()),
                        Named.as("velocity-alert"));
    }
}
