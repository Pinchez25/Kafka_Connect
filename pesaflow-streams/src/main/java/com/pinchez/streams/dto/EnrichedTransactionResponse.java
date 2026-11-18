package com.pinchez.streams.dto;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record EnrichedTransactionResponse(
        UUID transactionId,
        String type,
        UUID accountId,
        UUID counterpartyAccountId,
        BigDecimal amount,
        Instant occurredAt,
        String kycLevel,
        String accountStatus,
        Long accountAgeSeconds,
        UUID merchantId,
        String merchantCategory) {}
