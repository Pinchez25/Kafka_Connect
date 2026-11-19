package com.pinchez.streams.topology;

import com.pinchez.events.EnrichedTransaction;
import com.pinchez.streams.config.StreamsProperties;
import com.pinchez.streams.serde.AvroSerdeFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;

/** Per-account transaction counts per window, kept for {@code retention} and served by interactive queries. */
public final class AccountActivityTopology {

    private final StreamsProperties.Activity settings;
    private final AvroSerdeFactory serdes;

    public AccountActivityTopology(StreamsProperties.Activity settings, AvroSerdeFactory serdes) {
        this.settings = settings;
        this.serdes = serdes;
    }

    public void addTo(KStream<String, EnrichedTransaction> enriched) {
        enriched.groupByKey(Grouped.with("activity-by-account", Serdes.String(), serdes.valueSerde()))
                .windowedBy(TimeWindows.ofSizeAndGrace(settings.window(), settings.grace()))
                .count(
                        Named.as("account-activity-count"),
                        Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(StoreNames.ACCOUNT_ACTIVITY)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long())
                                .withRetention(settings.retention())
                                // Only interactive queries read this store, and the cache would keep serving
                                // windows past their retention until evicted.
                                .withCachingDisabled());
    }
}
