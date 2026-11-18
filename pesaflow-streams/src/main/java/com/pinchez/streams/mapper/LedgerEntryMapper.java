package com.pinchez.streams.mapper;

import com.pinchez.events.CompletedTransaction;
import com.pinchez.events.LedgerEntry;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.streams.KeyValue;

public final class LedgerEntryMapper {

    private LedgerEntryMapper() {}

    /** Splits a transaction into one debit and/or credit per affected account, keyed by account id. */
    public static List<KeyValue<String, LedgerEntry>> toLedgerEntries(CompletedTransaction transaction) {
        var amount = transaction.getAmount();
        var from = transaction.getFromAccountId();
        var to = transaction.getToAccountId();
        return switch (transaction.getType()) {
            case P2P ->
                List.of(
                        entry(transaction, from.orElseThrow(), amount.negate()),
                        entry(transaction, to.orElseThrow(), amount));
            case TOPUP -> List.of(entry(transaction, to.orElseThrow(), amount));
            case MERCHANT_PAYMENT -> List.of(entry(transaction, from.orElseThrow(), amount.negate()));
        };
    }

    private static KeyValue<String, LedgerEntry> entry(
            CompletedTransaction transaction, UUID accountId, BigDecimal amount) {
        var entry = LedgerEntry.newBuilder()
                .setTransactionId(transaction.getTransactionId())
                .setAccountId(accountId)
                .setType(transaction.getType())
                .setAmount(amount)
                .setOccurredAt(transaction.getOccurredAt())
                .build();
        return KeyValue.pair(accountId.toString(), entry);
    }
}
