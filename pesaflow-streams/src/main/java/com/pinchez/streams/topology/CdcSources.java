package com.pinchez.streams.topology;

import com.pinchez.cdc.Account;
import com.pinchez.cdc.Merchant;
import com.pinchez.cdc.Transaction;
import com.pinchez.events.CompletedTransaction;
import com.pinchez.streams.config.StreamsProperties;
import com.pinchez.streams.extractor.CdcTimestampExtractor;
import com.pinchez.streams.mapper.CompletedTransactionMapper;
import com.pinchez.streams.serde.AvroSerdeFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

/** The CDC inputs, registered once: Kafka Streams allows only one source per topic. */
public record CdcSources(
        KStream<String, CompletedTransaction> completedTransactions,
        KTable<String, Account> accounts,
        GlobalKTable<String, Merchant> merchants) {

    private static final String COMPLETED = "COMPLETED";
    private static final CdcTimestampExtractor EVENT_TIME = new CdcTimestampExtractor();

    public static CdcSources register(StreamsBuilder builder, StreamsProperties properties, AvroSerdeFactory serdes) {
        var topics = properties.topics();

        var completedTransactions = builder.stream(
                        topics.cdcTransactions(),
                        Consumed.with(Serdes.String(), serdes.<Transaction>valueSerde())
                                .withTimestampExtractor(EVENT_TIME)
                                .withName("cdc-transactions"))
                .filter((_, row) -> row != null && COMPLETED.equals(row.getStatus()), Named.as("completed-only"))
                .mapValues(CompletedTransactionMapper::toCompletedTransaction, Named.as("to-completed-transaction"));

        var accounts = builder.table(
                topics.cdcAccounts(),
                Consumed.with(Serdes.String(), serdes.<Account>valueSerde())
                        .withTimestampExtractor(EVENT_TIME)
                        .withName("cdc-accounts"),
                accountsStore(properties.accountsTable())
                        .withKeySerde(Serdes.String())
                        .withValueSerde(serdes.valueSerde()));

        var merchants = builder.globalTable(
                topics.cdcMerchants(),
                Consumed.with(Serdes.String(), serdes.<Merchant>valueSerde())
                        .withTimestampExtractor(EVENT_TIME)
                        .withName("cdc-merchants"),
                Materialized.<String, Merchant, KeyValueStore<Bytes, byte[]>>as(StoreNames.MERCHANTS)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(serdes.valueSerde()));

        return new CdcSources(completedTransactions, accounts, merchants);
    }

    private static Materialized<String, Account, KeyValueStore<Bytes, byte[]>> accountsStore(
            StreamsProperties.AccountsTable settings) {
        return settings.versioned()
                ? Materialized.as(
                        Stores.persistentVersionedKeyValueStore(StoreNames.ACCOUNTS, settings.historyRetention()))
                : Materialized.as(StoreNames.ACCOUNTS);
    }
}
