package com.pinchez.streams.mapper;

import com.pinchez.cdc.Account;
import com.pinchez.events.AccountSummary;
import com.pinchez.events.Balance;
import java.math.BigDecimal;

public final class AccountSummaryMapper {

    private static final BigDecimal ZERO = BigDecimal.ZERO.setScale(2);

    private AccountSummaryMapper() {}

    public static AccountSummary toSummary(Account account, Balance balance) {
        return AccountSummary.newBuilder()
                .setFullName(account.getFullName())
                .setKycLevel(AccountStateMapper.kycLevel(account))
                .setStatus(AccountStateMapper.status(account))
                .setDailyLimit(account.getDailyLimit())
                .setBalance(balance == null ? ZERO : balance.getAmount())
                .setEntryCount(balance == null ? 0 : balance.getEntryCount())
                .build();
    }
}
