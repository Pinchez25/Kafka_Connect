package com.pinchez.streams.topology;

import com.pinchez.cdc.Transaction;
import com.pinchez.events.CompletedTransaction;
import com.pinchez.events.TransactionType;
import com.pinchez.streams.config.StreamsProperties;
import com.pinchez.streams.mapper.CompletedTransactionMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;

public final class TransactionRoutingTopology {

    private static final String COMPLETED = "COMPLETED";

    private final StreamsProperties.Topics topics;
    private final Serde<Transaction> cdcTransactionSerde;
    private final Serde<CompletedTransaction> completedTransactionSerde;

    public TransactionRoutingTopology(
            StreamsProperties.Topics topics,
            Serde<Transaction> cdcTransactionSerde,
            Serde<CompletedTransaction> completedTransactionSerde) {
        this.topics = topics;
        this.cdcTransactionSerde = cdcTransactionSerde;
        this.completedTransactionSerde = completedTransactionSerde;
    }

    public void addTo(StreamsBuilder builder) {
        builder.stream(
                        topics.cdcTransactions(),
                        Consumed.with(Serdes.String(), cdcTransactionSerde).withName("cdc-transactions"))
                .filter((_, row) -> row != null && COMPLETED.equals(row.getStatus()), Named.as("completed-only"))
                .mapValues(CompletedTransactionMapper::toCompletedTransaction, Named.as("to-completed-transaction"))
                .split(Named.as("by-type-"))
                .branch(isType(TransactionType.P2P), sinkTo(topics.p2pTransfers(), "p2p"))
                .branch(isType(TransactionType.TOPUP), sinkTo(topics.topUps(), "top-up"))
                .branch(isType(TransactionType.MERCHANT_PAYMENT), sinkTo(topics.merchantPayments(), "merchant-payment"))
                .noDefaultBranch();
    }

    private static Predicate<String, CompletedTransaction> isType(TransactionType type) {
        return (_, transaction) -> transaction.getType() == type;
    }

    private Branched<String, CompletedTransaction> sinkTo(String topic, String name) {
        var produced = Produced.with(Serdes.String(), completedTransactionSerde).withName(name + "-sink");
        return Branched.<String, CompletedTransaction>withConsumer(stream -> stream.to(topic, produced))
                .withName(name);
    }
}
