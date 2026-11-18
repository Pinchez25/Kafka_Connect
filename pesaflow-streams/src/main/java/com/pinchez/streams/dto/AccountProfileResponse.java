package com.pinchez.streams.dto;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

public record AccountProfileResponse(
        UUID accountId,
        BigDecimal totalTopUps,
        long topUpCount,
        BigDecimal totalP2pSent,
        long p2pSentCount,
        BigDecimal totalMerchantSpend,
        long merchantPaymentCount,
        Map<String, Long> merchantPaymentsByCategory,
        String favouriteMerchantCategory) {}
