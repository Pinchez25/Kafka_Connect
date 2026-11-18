package com.pinchez.streams.topology;

import com.pinchez.events.CompletedTransaction;
import com.pinchez.events.TransactionSender;
import com.pinchez.streams.config.StreamsProperties;
import com.pinchez.streams.mapper.TransactionSenderMapper;
import com.pinchez.streams.serde.AvroSerdeFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TableJoined;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Foreign-key join: transactions (keyed by transaction id) to their sender's account. When an account changes, every
 * transaction it sent is re-emitted.
 */
public final class TransactionSenderTopology {

    private final StreamsProperties.Topics topics;
    private final AvroSerdeFactory serdes;

    public TransactionSenderTopology(StreamsProperties.Topics topics, AvroSerdeFactory serdes) {
        this.topics = topics;
        this.serdes = serdes;
    }

    public void addTo(CdcSources sources) {
        var senderSerde = serdes.<TransactionSender>valueSerde();

        sources.completedTransactions()
                .filter((_, transaction) -> transaction.getFromAccountId().isPresent(), Named.as("with-sender-only"))
                .toTable(
                        Named.as("transactions-as-table"),
                        Materialized.<String, CompletedTransaction, KeyValueStore<Bytes, byte[]>>as(
                                        StoreNames.TRANSACTIONS_BY_ID)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(serdes.valueSerde()))
                .join(
                        sources.accounts(),
                        transaction ->
                                transaction.getFromAccountId().orElseThrow().toString(),
                        TransactionSenderMapper::toTransactionSender,
                        TableJoined.as("transaction-sender"),
                        Materialized.<String, TransactionSender, KeyValueStore<Bytes, byte[]>>as(
                                        StoreNames.TRANSACTION_SENDERS)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(senderSerde))
                .toStream(Named.as("transaction-sender-changes"))
                .to(
                        topics.transactionSenders(),
                        Produced.with(Serdes.String(), senderSerde).withName("transaction-senders-sink"));
    }
}
