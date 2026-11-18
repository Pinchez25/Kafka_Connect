package com.pinchez.streams.dto;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record BalanceResponse(
        UUID accountId, BigDecimal balance, long entryCount, UUID lastTransactionId, Instant updatedAt) {}
