package com.pinchez.streams.support;

import com.pinchez.cdc.Transaction;
import java.math.BigDecimal;
import java.util.UUID;

public final class CdcTransactions {

    private CdcTransactions() {}

    public static Transaction.Builder completed(String type) {
        var builder = Transaction.newBuilder()
                .setId(UUID.randomUUID())
                .setCreatedAt("2026-11-18T09:10:31.829078Z")
                .setAmount(new BigDecimal("350.50"))
                .setReference("TKL8XQ2M9A")
                .setType(type)
                .setCurrency("KES")
                .setStatus("COMPLETED")
                .setOp("c");
        return switch (type) {
            case "P2P" -> builder.setFromAccountId(UUID.randomUUID()).setToAccountId(UUID.randomUUID());
            case "TOPUP" -> builder.setToAccountId(UUID.randomUUID()).setAgentNumber("123456");
            case "MERCHANT_PAYMENT" ->
                builder.setFromAccountId(UUID.randomUUID()).setMerchantId(UUID.randomUUID());
            default -> throw new IllegalArgumentException("Unknown transaction type " + type);
        };
    }

    public static Transaction.Builder failed(String type, String reason) {
        return completed(type).setStatus("FAILED").setFailureReason(reason);
    }
}
