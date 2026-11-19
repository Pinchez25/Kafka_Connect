package com.pinchez.streams.aggregator;

import com.pinchez.events.EnrichedTransaction;
import com.pinchez.events.RollingSpend;
import java.math.BigDecimal;

public final class RollingSpendAggregator {

    private RollingSpendAggregator() {}

    public static RollingSpend initial() {
        return RollingSpend.newBuilder()
                .setAmount(BigDecimal.ZERO.setScale(2))
                .setTransactionCount(0)
                .build();
    }

    public static RollingSpend add(String accountId, EnrichedTransaction transaction, RollingSpend spend) {
        return RollingSpend.newBuilder(spend)
                .setAmount(spend.getAmount().add(transaction.getAmount()))
                .setTransactionCount(spend.getTransactionCount() + 1)
                .build();
    }
}
