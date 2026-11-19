package com.pinchez.streams.mapper;

import com.pinchez.events.AccountProfile;
import com.pinchez.events.AccountSummary;
import com.pinchez.events.Balance;
import com.pinchez.events.EnrichedTransaction;
import com.pinchez.events.LastActivity;
import com.pinchez.streams.dto.AccountProfileResponse;
import com.pinchez.streams.dto.AccountSummaryResponse;
import com.pinchez.streams.dto.BalanceResponse;
import com.pinchez.streams.dto.EnrichedTransactionResponse;
import com.pinchez.streams.dto.LastActivityResponse;
import java.util.UUID;

/** Avro state → REST DTOs. Hand-written because MapStruct can't read the generated Optional getters. */
public final class QueryResponseMapper {

    private QueryResponseMapper() {}

    public static BalanceResponse toResponse(UUID accountId, Balance balance) {
        return new BalanceResponse(
                accountId,
                balance.getAmount(),
                balance.getEntryCount(),
                balance.getLastTransactionId().orElse(null),
                balance.getUpdatedAt().orElse(null));
    }

    public static AccountProfileResponse toResponse(UUID accountId, AccountProfile profile) {
        return new AccountProfileResponse(
                accountId,
                profile.getTotalTopUps(),
                profile.getTopUpCount(),
                profile.getTotalP2pSent(),
                profile.getP2pSentCount(),
                profile.getTotalMerchantSpend(),
                profile.getMerchantPaymentCount(),
                profile.getMerchantPaymentsByCategory(),
                profile.getFavouriteMerchantCategory().map(Enum::name).orElse(null));
    }

    public static AccountSummaryResponse toResponse(UUID accountId, AccountSummary summary) {
        return new AccountSummaryResponse(
                accountId,
                summary.getFullName(),
                summary.getKycLevel().name(),
                summary.getStatus().name(),
                summary.getDailyLimit(),
                summary.getBalance(),
                summary.getEntryCount());
    }

    public static EnrichedTransactionResponse toResponse(EnrichedTransaction transaction) {
        return new EnrichedTransactionResponse(
                transaction.getTransactionId(),
                transaction.getType().name(),
                transaction.getAccountId(),
                transaction.getCounterpartyAccountId().orElse(null),
                transaction.getAmount(),
                transaction.getOccurredAt(),
                transaction.getKycLevel().map(Enum::name).orElse(null),
                transaction.getAccountStatus().map(Enum::name).orElse(null),
                transaction.getAccountAgeSeconds().orElse(null),
                transaction.getMerchantId().orElse(null),
                transaction.getMerchantCategory().map(Enum::name).orElse(null));
    }

    public static LastActivityResponse toResponse(UUID accountId, LastActivity activity) {
        return new LastActivityResponse(
                accountId,
                activity.getLastTransactionId(),
                activity.getLastActivityAt(),
                activity.getReportedDormantAt().orElse(null));
    }
}
