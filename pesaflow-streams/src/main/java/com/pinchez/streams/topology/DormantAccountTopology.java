package com.pinchez.streams.topology;

import com.pinchez.events.DormantAccount;
import com.pinchez.events.LedgerEntry;
import com.pinchez.streams.config.StreamsProperties;
import com.pinchez.streams.processor.DormantAccountProcessor;
import com.pinchez.streams.serde.AvroSerdeFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

/** A Processor API processor plugged into the DSL, connected to its own store and to the DSL's balances store. */
public final class DormantAccountTopology {

    private final StreamsProperties.DormantAccount settings;
    private final StreamsProperties.Topics topics;
    private final AvroSerdeFactory serdes;

    public DormantAccountTopology(
            StreamsProperties.DormantAccount settings, StreamsProperties.Topics topics, AvroSerdeFactory serdes) {
        this.settings = settings;
        this.topics = topics;
        this.serdes = serdes;
    }

    public void addTo(KStream<String, LedgerEntry> ledgerByAccount) {
        ledgerByAccount
                .process(
                        new DormantAccountProcessor.Supplier(
                                settings, StoreNames.LAST_ACTIVITY, StoreNames.BALANCES, serdes),
                        Named.as("detect-dormant-accounts"),
                        StoreNames.BALANCES)
                .to(
                        topics.dormantAccounts(),
                        Produced.with(Serdes.String(), serdes.<DormantAccount>valueSerde())
                                .withName("dormant-accounts-sink"));
    }
}
