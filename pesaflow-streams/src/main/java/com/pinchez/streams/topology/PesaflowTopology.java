package com.pinchez.streams.topology;

import com.pinchez.streams.config.StreamsProperties;
import com.pinchez.streams.serde.AvroSerdeFactory;
import com.pinchez.streams.topology.fraud.FraudDetectionTopology;
import org.apache.kafka.streams.StreamsBuilder;

public final class PesaflowTopology {

    private final StreamsProperties properties;
    private final AvroSerdeFactory serdes;

    public PesaflowTopology(StreamsProperties properties, AvroSerdeFactory serdes) {
        this.properties = properties;
        this.serdes = serdes;
    }

    public void addTo(StreamsBuilder builder) {
        var topics = properties.topics();
        var sources = CdcSources.register(builder, properties, serdes);

        new TransactionRoutingTopology(topics, serdes).addTo(sources.completedTransactions());
        var enriched = new TransactionEnrichmentTopology(topics, serdes).addTo(sources);
        var balances = new BalanceTopology(topics, serdes).addTo(sources);
        new AccountInsightsTopology(serdes).addTo(sources, enriched, balances);
        new TransactionSenderTopology(topics, serdes).addTo(sources);
        new AccountActivityTopology(properties.activity(), serdes).addTo(enriched);
        new FraudDetectionTopology(properties, serdes).addTo(enriched, sources);
    }
}
