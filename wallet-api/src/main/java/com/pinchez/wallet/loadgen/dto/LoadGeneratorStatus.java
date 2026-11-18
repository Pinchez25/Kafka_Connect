package com.pinchez.wallet.loadgen.dto;

import com.pinchez.wallet.transaction.enums.TransactionType;
import java.time.Duration;
import java.util.Map;

public record LoadGeneratorStatus(
        boolean running,
        Duration interval,
        int transactionsPerTick,
        long accountsOpened,
        long merchantsRegistered,
        Map<TransactionType, Long> transactionsSubmitted,
        long transactionsDeclined,
        long errors) {}
