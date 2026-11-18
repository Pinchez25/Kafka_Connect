package com.pinchez.streams.mapper;

import com.pinchez.cdc.Account;
import com.pinchez.cdc.Merchant;
import com.pinchez.events.CompletedTransaction;
import com.pinchez.events.EnrichedTransaction;
import com.pinchez.events.MerchantCategory;
import java.time.Duration;
import java.util.UUID;

public final class EnrichedTransactionMapper {

    private EnrichedTransactionMapper() {}

    /** The account a transaction is attributed to: the sender, or the recipient of a top-up. */
    public static UUID accountIdOf(CompletedTransaction transaction) {
        return transaction.getFromAccountId().or(transaction::getToAccountId).orElseThrow();
    }

    public static EnrichedTransaction withAccount(CompletedTransaction transaction, Account account) {
        var accountId = accountIdOf(transaction);
        var builder = EnrichedTransaction.newBuilder()
                .setTransactionId(transaction.getTransactionId())
                .setType(transaction.getType())
                .setAccountId(accountId)
                .setCounterpartyAccountId(transaction
                        .getToAccountId()
                        .filter(to -> !to.equals(accountId))
                        .orElse(null))
                .setAmount(transaction.getAmount())
                .setOccurredAt(transaction.getOccurredAt())
                .setMerchantId(transaction.getMerchantId().orElse(null));
        if (account != null) {
            builder.setKycLevel(AccountStateMapper.kycLevel(account))
                    .setAccountStatus(AccountStateMapper.status(account))
                    .setAccountAgeSeconds(
                            Duration.between(AccountStateMapper.openedAt(account), transaction.getOccurredAt())
                                    .toSeconds());
        }
        return builder.build();
    }

    public static EnrichedTransaction withMerchant(EnrichedTransaction transaction, Merchant merchant) {
        if (merchant == null) {
            return transaction;
        }
        return EnrichedTransaction.newBuilder(transaction)
                .setMerchantCategory(MerchantCategory.valueOf(merchant.getCategory()))
                .build();
    }
}
