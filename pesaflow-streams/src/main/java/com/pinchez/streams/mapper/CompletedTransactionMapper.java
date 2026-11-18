package com.pinchez.streams.mapper;

import com.pinchez.cdc.Transaction;
import com.pinchez.events.CompletedTransaction;
import com.pinchez.events.TransactionType;
import java.time.Instant;

public final class CompletedTransactionMapper {

    private CompletedTransactionMapper() {}

    public static CompletedTransaction toCompletedTransaction(Transaction row) {
        return CompletedTransaction.newBuilder()
                .setTransactionId(row.getId())
                .setReference(row.getReference())
                .setType(TransactionType.valueOf(row.getType()))
                .setFromAccountId(row.getFromAccountId().orElse(null))
                .setToAccountId(row.getToAccountId().orElse(null))
                .setMerchantId(row.getMerchantId().orElse(null))
                .setAgentNumber(row.getAgentNumber().orElse(null))
                .setAmount(row.getAmount())
                .setCurrency(row.getCurrency())
                .setOccurredAt(Instant.parse(row.getCreatedAt()))
                .build();
    }
}
