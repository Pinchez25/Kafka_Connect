package com.pinchez.streams.topology;

import com.pinchez.events.Balance;
import com.pinchez.events.LedgerEntry;
import com.pinchez.streams.aggregator.BalanceAggregator;
import com.pinchez.streams.config.StreamsProperties;
import com.pinchez.streams.mapper.LedgerEntryMapper;
import com.pinchez.streams.serde.AvroSerdeFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public final class BalanceTopology {

    private final StreamsProperties.Topics topics;
    private final AvroSerdeFactory serdes;

    public BalanceTopology(StreamsProperties.Topics topics, AvroSerdeFactory serdes) {
        this.topics = topics;
        this.serdes = serdes;
    }

    public KTable<String, Balance> addTo(CdcSources sources) {
        var balanceSerde = serdes.<Balance>valueSerde();

        var balances = sources.completedTransactions()
                .flatMap(
                        (_, transaction) -> LedgerEntryMapper.toLedgerEntries(transaction),
                        Named.as("to-ledger-entries"))
                .groupByKey(
                        Grouped.with("ledger-entries-by-account", Serdes.String(), serdes.<LedgerEntry>valueSerde()))
                .aggregate(
                        BalanceAggregator::initial,
                        BalanceAggregator::apply,
                        Named.as("aggregate-balance"),
                        Materialized.<String, Balance, KeyValueStore<Bytes, byte[]>>as(StoreNames.BALANCES)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(balanceSerde));

        balances.toStream(Named.as("balance-changes"))
                .to(
                        topics.balances(),
                        Produced.with(Serdes.String(), balanceSerde).withName("balances-sink"));
        return balances;
    }
}
