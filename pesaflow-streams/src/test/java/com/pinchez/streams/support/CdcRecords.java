package com.pinchez.streams.support;

import com.pinchez.cdc.Account;
import com.pinchez.cdc.Merchant;
import com.pinchez.cdc.Transaction;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public final class CdcRecords {

    public static final Instant OPENED_AT = Instant.parse("2026-11-18T08:00:00Z");

    private CdcRecords() {}

    public static Account.Builder account(UUID id, String kycLevel) {
        return Account.newBuilder()
                .setId(id)
                .setCreatedAt(OPENED_AT.toString())
                .setUpdatedAt(OPENED_AT.toString())
                .setVersion(0)
                .setDailyLimit(new BigDecimal("70000.00"))
                .setPhoneNumber("7a3231b57d20296")
                .setFullName("Wanjiku Kamau")
                .setKycLevel(kycLevel)
                .setStatus("ACTIVE")
                .setOp("c");
    }

    public static Merchant merchant(UUID id, String category) {
        return Merchant.newBuilder()
                .setId(id)
                .setCreatedAt(OPENED_AT.toString())
                .setTillNumber("512345")
                .setName("Mama Njeri Groceries")
                .setCategory(category)
                .setOp("c")
                .build();
    }

    public static Account.Builder accountVersion(UUID id, String kycLevel, Instant updatedAt) {
        return account(id, kycLevel).setUpdatedAt(updatedAt.toString());
    }

    public static Transaction.Builder p2p(UUID from, UUID to, String amount) {
        return transaction("P2P", amount).setFromAccountId(from).setToAccountId(to);
    }

    public static Transaction.Builder topUp(UUID to, String amount) {
        return transaction("TOPUP", amount).setToAccountId(to).setAgentNumber("123456");
    }

    public static Transaction.Builder merchantPayment(UUID from, UUID merchantId, String amount) {
        return transaction("MERCHANT_PAYMENT", amount).setFromAccountId(from).setMerchantId(merchantId);
    }

    private static Transaction.Builder transaction(String type, String amount) {
        return Transaction.newBuilder()
                .setId(UUID.randomUUID())
                .setCreatedAt(OPENED_AT.plusSeconds(3_600).toString())
                .setAmount(new BigDecimal(amount))
                .setReference("TKL8XQ2M9A")
                .setType(type)
                .setCurrency("KES")
                .setStatus("COMPLETED")
                .setOp("c");
    }
}
