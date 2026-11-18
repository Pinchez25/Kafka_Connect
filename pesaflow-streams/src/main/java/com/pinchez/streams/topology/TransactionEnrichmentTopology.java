package com.pinchez.streams.topology;

import com.pinchez.cdc.Account;
import com.pinchez.events.CompletedTransaction;
import com.pinchez.events.EnrichedTransaction;
import com.pinchez.streams.config.StreamsProperties;
import com.pinchez.streams.mapper.EnrichedTransactionMapper;
import com.pinchez.streams.serde.AvroSerdeFactory;
import java.util.UUID;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

/** Stream–table joins: each transaction picks up its account (KTable) and merchant (GlobalKTable). */
public final class TransactionEnrichmentTopology {

    private final StreamsProperties.Topics topics;
    private final AvroSerdeFactory serdes;

    public TransactionEnrichmentTopology(StreamsProperties.Topics topics, AvroSerdeFactory serdes) {
        this.topics = topics;
        this.serdes = serdes;
    }

    /** Returns enriched transactions keyed by account id. */
    public KStream<String, EnrichedTransaction> addTo(CdcSources sources) {
        var enriched = sources.completedTransactions()
                .selectKey(
                        (_, transaction) -> EnrichedTransactionMapper.accountIdOf(transaction)
                                .toString(),
                        Named.as("key-by-account"))
                .leftJoin(
                        sources.accounts(),
                        EnrichedTransactionMapper::withAccount,
                        Joined.<String, CompletedTransaction, Account>with(
                                        Serdes.String(), serdes.valueSerde(), serdes.valueSerde())
                                .withName("join-account"))
                .leftJoin(
                        sources.merchants(),
                        (_, transaction) ->
                                transaction.getMerchantId().map(UUID::toString).orElse(null),
                        EnrichedTransactionMapper::withMerchant,
                        Named.as("join-merchant"));

        enriched.to(
                topics.enrichedTransactions(),
                Produced.with(Serdes.String(), serdes.<EnrichedTransaction>valueSerde())
                        .withName("enriched-sink"));
        return enriched;
    }
}
