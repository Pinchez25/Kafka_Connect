package com.pinchez.streams.topology;

import com.pinchez.events.LedgerEntry;
import com.pinchez.streams.mapper.LedgerEntryMapper;
import com.pinchez.streams.serde.AvroSerdeFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Repartitioned;

/**
 * Debit and credit entries, re-keyed and repartitioned by account. The repartition is explicit so that more than one
 * consumer (balances and dormancy) can share it; the topic name is the one {@code groupByKey} used to create.
 */
public final class LedgerTopology {

    private LedgerTopology() {}

    public static KStream<String, LedgerEntry> byAccount(CdcSources sources, AvroSerdeFactory serdes) {
        return sources.completedTransactions()
                .flatMap(
                        (_, transaction) -> LedgerEntryMapper.toLedgerEntries(transaction),
                        Named.as("to-ledger-entries"))
                .repartition(Repartitioned.<String, LedgerEntry>as("ledger-entries-by-account")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(serdes.valueSerde()));
    }
}
