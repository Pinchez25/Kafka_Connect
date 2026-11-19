package com.pinchez.streams.topology.fraud;

import com.pinchez.events.EnrichedTransaction;
import com.pinchez.events.FraudAlert;
import com.pinchez.events.SpendingBaseline;
import com.pinchez.streams.config.StreamsProperties;
import com.pinchez.streams.processor.UnusualAmountProcessor;
import com.pinchez.streams.serde.AvroSerdeFactory;
import com.pinchez.streams.topology.StoreNames;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;

/**
 * A fraud rule built only with the low-level {@link Topology} API: source, processor, state store and two sinks, each
 * wired by name. It reads the enriched topic the DSL part of the application writes.
 */
public final class UnusualAmountRule {

    public static final String SOURCE = "unusual-amount-source";
    public static final String PROCESSOR = "detect-unusual-amount";
    public static final String HIGH_SEVERITY_SINK = "unusual-amount-alerts-sink";
    public static final String REVIEW_SINK = "unusual-amount-review-sink";

    private final StreamsProperties.Fraud.UnusualAmount settings;
    private final StreamsProperties.Topics topics;
    private final AvroSerdeFactory serdes;

    public UnusualAmountRule(
            StreamsProperties.Fraud.UnusualAmount settings, StreamsProperties.Topics topics, AvroSerdeFactory serdes) {
        this.settings = settings;
        this.topics = topics;
        this.serdes = serdes;
    }

    public void addTo(Topology topology) {
        var alertSerializer = serdes.<FraudAlert>valueSerde().serializer();
        topology.addSource(
                        SOURCE,
                        Serdes.String().deserializer(),
                        serdes.<EnrichedTransaction>valueSerde().deserializer(),
                        topics.enrichedTransactions())
                .addProcessor(
                        PROCESSOR,
                        () -> new UnusualAmountProcessor(
                                settings, StoreNames.SPENDING_BASELINES, HIGH_SEVERITY_SINK, REVIEW_SINK),
                        SOURCE)
                .addStateStore(
                        Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(StoreNames.SPENDING_BASELINES),
                                Serdes.String(),
                                serdes.<SpendingBaseline>valueSerde()),
                        PROCESSOR)
                .addSink(
                        HIGH_SEVERITY_SINK,
                        topics.fraudAlerts(),
                        Serdes.String().serializer(),
                        alertSerializer,
                        PROCESSOR)
                .addSink(REVIEW_SINK, topics.fraudReview(), Serdes.String().serializer(), alertSerializer, PROCESSOR);
    }
}
