package com.pinchez.wallet.account.entity;

import com.pinchez.wallet.account.enums.AccountStatus;
import com.pinchez.wallet.account.enums.KycLevel;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.UuidGenerator;

@Entity
@Table(name = "accounts")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Account {

    @Id
    @GeneratedValue
    @UuidGenerator(style = UuidGenerator.Style.VERSION_7)
    private UUID id;

    @Column(nullable = false, unique = true, length = 15)
    private String phoneNumber;

    @Column(nullable = false, length = 120)
    private String fullName;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 10)
    private KycLevel kycLevel;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 10)
    private AccountStatus status;

    @Column(nullable = false, precision = 19, scale = 2)
    private BigDecimal dailyLimit;

    @Column(nullable = false)
    private Instant createdAt;

    @Column(nullable = false)
    private Instant updatedAt;

    @Version
    private long version;

    public static Account open(
            String phoneNumber, String fullName, KycLevel kycLevel, BigDecimal dailyLimit, Instant now) {
        var account = new Account();
        account.phoneNumber = phoneNumber;
        account.fullName = fullName;
        account.kycLevel = kycLevel;
        account.dailyLimit = dailyLimit;
        account.status = AccountStatus.ACTIVE;
        account.createdAt = now;
        account.updatedAt = now;
        return account;
    }

    public boolean isActive() {
        return status == AccountStatus.ACTIVE;
    }

    public void changeStatus(AccountStatus newStatus, Instant now) {
        this.status = newStatus;
        this.updatedAt = now;
    }

    public void changeKycLevel(KycLevel newKycLevel, BigDecimal newDailyLimit, Instant now) {
        this.kycLevel = newKycLevel;
        this.dailyLimit = newDailyLimit;
        this.updatedAt = now;
    }
}
