package com.pinchez.wallet.account.dto;

import com.pinchez.wallet.account.enums.AccountStatus;
import com.pinchez.wallet.account.enums.KycLevel;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record AccountResponse(
        UUID id,
        String phoneNumber,
        String fullName,
        KycLevel kycLevel,
        AccountStatus status,
        BigDecimal dailyLimit,
        Instant createdAt,
        Instant updatedAt) {}
