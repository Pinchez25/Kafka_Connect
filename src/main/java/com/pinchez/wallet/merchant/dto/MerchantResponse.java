package com.pinchez.wallet.merchant.dto;

import com.pinchez.wallet.merchant.enums.MerchantCategory;
import java.time.Instant;
import java.util.UUID;

public record MerchantResponse(UUID id, String tillNumber, String name, MerchantCategory category, Instant createdAt) {}
