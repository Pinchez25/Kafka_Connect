package com.pinchez.streams.aggregator;

import com.pinchez.events.AccountProfile;
import com.pinchez.events.EnrichedTransaction;
import com.pinchez.events.MerchantCategory;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public final class AccountProfileAggregator {

    private static final BigDecimal ZERO = BigDecimal.ZERO.setScale(2);

    private AccountProfileAggregator() {}

    public static AccountProfile initial() {
        return AccountProfile.newBuilder()
                .setTotalTopUps(ZERO)
                .setTopUpCount(0)
                .setTotalP2pSent(ZERO)
                .setP2pSentCount(0)
                .setTotalMerchantSpend(ZERO)
                .setMerchantPaymentCount(0)
                .setMerchantPaymentsByCategory(Map.of())
                .build();
    }

    public static AccountProfile addTopUp(String accountId, EnrichedTransaction topUp, AccountProfile profile) {
        return AccountProfile.newBuilder(profile)
                .setTotalTopUps(profile.getTotalTopUps().add(topUp.getAmount()))
                .setTopUpCount(profile.getTopUpCount() + 1)
                .build();
    }

    public static AccountProfile addP2pSent(String accountId, EnrichedTransaction transfer, AccountProfile profile) {
        return AccountProfile.newBuilder(profile)
                .setTotalP2pSent(profile.getTotalP2pSent().add(transfer.getAmount()))
                .setP2pSentCount(profile.getP2pSentCount() + 1)
                .build();
    }

    public static AccountProfile addMerchantPayment(
            String accountId, EnrichedTransaction payment, AccountProfile profile) {
        var byCategory = new HashMap<>(profile.getMerchantPaymentsByCategory());
        payment.getMerchantCategory().ifPresent(category -> byCategory.merge(category.name(), 1L, Long::sum));
        return AccountProfile.newBuilder(profile)
                .setTotalMerchantSpend(profile.getTotalMerchantSpend().add(payment.getAmount()))
                .setMerchantPaymentCount(profile.getMerchantPaymentCount() + 1)
                .setMerchantPaymentsByCategory(byCategory)
                .setFavouriteMerchantCategory(favourite(byCategory))
                .build();
    }

    private static MerchantCategory favourite(Map<String, Long> byCategory) {
        return byCategory.entrySet().stream()
                .max(Map.Entry.<String, Long>comparingByValue().thenComparing(Map.Entry.comparingByKey()))
                .map(entry -> MerchantCategory.valueOf(entry.getKey()))
                .orElse(null);
    }
}
