package com.pinchez.streams.mapper;

import com.pinchez.cdc.Account;
import com.pinchez.events.AccountStatus;
import com.pinchez.events.KycLevel;
import java.time.Instant;

public final class AccountStateMapper {

    private AccountStateMapper() {}

    public static KycLevel kycLevel(Account account) {
        return KycLevel.valueOf(account.getKycLevel());
    }

    public static AccountStatus status(Account account) {
        return AccountStatus.valueOf(account.getStatus());
    }

    public static Instant openedAt(Account account) {
        return Instant.parse(account.getCreatedAt());
    }
}
