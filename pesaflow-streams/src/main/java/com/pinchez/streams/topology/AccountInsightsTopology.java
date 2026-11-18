package com.pinchez.streams.topology;

import com.pinchez.events.AccountProfile;
import com.pinchez.events.AccountSummary;
import com.pinchez.events.Balance;
import com.pinchez.events.EnrichedTransaction;
import com.pinchez.events.TransactionType;
import com.pinchez.streams.aggregator.AccountProfileAggregator;
import com.pinchez.streams.mapper.AccountSummaryMapper;
import com.pinchez.streams.serde.AvroSerdeFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.KeyValueStore;

/** Per-account state, each built a different way: reduce, cogroup, and a table–table join. */
public final class AccountInsightsTopology {

    private final AvroSerdeFactory serdes;

    public AccountInsightsTopology(AvroSerdeFactory serdes) {
        this.serdes = serdes;
    }

    public void addTo(
            CdcSources sources, KStream<String, EnrichedTransaction> enriched, KTable<String, Balance> balances) {
        addLargestTransaction(enriched);
        addProfile(enriched);
        addSummary(sources, balances);
    }

    private void addLargestTransaction(KStream<String, EnrichedTransaction> enriched) {
        enriched.groupByKey(Grouped.with("enriched-by-account", Serdes.String(), serdes.valueSerde()))
                .reduce(
                        (largest, candidate) ->
                                candidate.getAmount().compareTo(largest.getAmount()) > 0 ? candidate : largest,
                        Named.as("largest-transaction"),
                        Materialized.<String, EnrichedTransaction, KeyValueStore<Bytes, byte[]>>as(
                                        StoreNames.LARGEST_TRANSACTIONS)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(serdes.valueSerde()));
    }

    private void addProfile(KStream<String, EnrichedTransaction> enriched) {
        groupedByType(enriched, TransactionType.TOPUP, "top-ups")
                .cogroup(AccountProfileAggregator::addTopUp)
                .cogroup(groupedByType(enriched, TransactionType.P2P, "p2p-sent"), AccountProfileAggregator::addP2pSent)
                .cogroup(
                        groupedByType(enriched, TransactionType.MERCHANT_PAYMENT, "merchant-payments"),
                        AccountProfileAggregator::addMerchantPayment)
                .aggregate(
                        AccountProfileAggregator::initial,
                        Named.as("account-profile"),
                        Materialized.<String, AccountProfile, KeyValueStore<Bytes, byte[]>>as(
                                        StoreNames.ACCOUNT_PROFILES)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(serdes.valueSerde()));
    }

    private KGroupedStream<String, EnrichedTransaction> groupedByType(
            KStream<String, EnrichedTransaction> enriched, TransactionType type, String name) {
        return enriched.filter((_, transaction) -> transaction.getType() == type, Named.as(name + "-only"))
                .groupByKey(Grouped.with(name + "-by-account", Serdes.String(), serdes.valueSerde()));
    }

    private void addSummary(CdcSources sources, KTable<String, Balance> balances) {
        sources.accounts()
                .leftJoin(
                        balances,
                        AccountSummaryMapper::toSummary,
                        Named.as("account-summary"),
                        Materialized.<String, AccountSummary, KeyValueStore<Bytes, byte[]>>as(
                                        StoreNames.ACCOUNT_SUMMARIES)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(serdes.valueSerde()));
    }
}
