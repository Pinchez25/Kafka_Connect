package com.pinchez.streams.aggregator;

import com.pinchez.events.Balance;
import com.pinchez.events.LedgerEntry;
import java.math.BigDecimal;

public final class BalanceAggregator {

    private BalanceAggregator() {}

    public static Balance initial() {
        return Balance.newBuilder()
                .setAmount(BigDecimal.ZERO.setScale(2))
                .setEntryCount(0)
                .build();
    }

    public static Balance apply(String accountId, LedgerEntry entry, Balance balance) {
        return Balance.newBuilder(balance)
                .setAmount(balance.getAmount().add(entry.getAmount()))
                .setEntryCount(balance.getEntryCount() + 1)
                .setLastTransactionId(entry.getTransactionId())
                .setUpdatedAt(entry.getOccurredAt())
                .build();
    }
}
