package com.pinchez.streams.dto;

import java.math.BigDecimal;
import java.util.UUID;

public record AccountSummaryResponse(
        UUID accountId,
        String fullName,
        String kycLevel,
        String status,
        BigDecimal dailyLimit,
        BigDecimal balance,
        long entryCount) {}
