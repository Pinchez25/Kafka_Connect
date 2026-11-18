package com.pinchez.wallet.transaction.dto;

import com.pinchez.wallet.transaction.enums.FailureReason;
import com.pinchez.wallet.transaction.enums.TransactionStatus;
import com.pinchez.wallet.transaction.enums.TransactionType;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record TransactionResponse(
        UUID id,
        String reference,
        TransactionType type,
        UUID fromAccountId,
        UUID toAccountId,
        UUID merchantId,
        String agentNumber,
        BigDecimal amount,
        String currency,
        TransactionStatus status,
        FailureReason failureReason,
        Instant createdAt) {}
