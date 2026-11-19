package com.pinchez.streams.topology.fraud;

import com.pinchez.events.EnrichedTransaction;
import com.pinchez.events.FraudAlert;
import com.pinchez.streams.config.StreamsProperties;
import com.pinchez.streams.serde.AvroSerdeFactory;
import com.pinchez.streams.topology.CdcSources;
import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

public final class FraudDetectionTopology {

    private final StreamsProperties properties;
    private final AvroSerdeFactory serdes;

    public FraudDetectionTopology(StreamsProperties properties, AvroSerdeFactory serdes) {
        this.properties = properties;
        this.serdes = serdes;
    }

    public void addTo(KStream<String, EnrichedTransaction> enriched, CdcSources sources) {
        var fraud = properties.fraud();
        List<FraudRuleTopology> rules = List.of(
                new TransferVelocityRule(fraud.transferVelocity(), serdes),
                new RollingSpendRule(fraud.rollingSpend(), serdes),
                new NewAccountLargeTransferRule(fraud.newAccount()),
                new NightBettingBurstRule(fraud.nightBetting(), fraud.zoneId(), serdes),
                new RoundTripTransferRule(fraud.roundTrip(), serdes));

        KStream<String, FraudAlert> alerts = null;
        for (var rule : rules) {
            var detected = rule.detect(enriched, sources);
            alerts = alerts == null
                    ? detected
                    : alerts.merge(detected, Named.as("merge-" + rule.getClass().getSimpleName()));
        }
        alerts.to(
                properties.topics().fraudAlerts(),
                Produced.with(Serdes.String(), serdes.<FraudAlert>valueSerde()).withName("fraud-alerts-sink"));
    }
}
