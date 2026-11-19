package com.pinchez.wallet.transaction.entity;

import com.pinchez.wallet.account.entity.Account;
import com.pinchez.wallet.common.entity.CreationAuditedEntity;
import com.pinchez.wallet.merchant.entity.Merchant;
import com.pinchez.wallet.transaction.enums.FailureReason;
import com.pinchez.wallet.transaction.enums.TransactionStatus;
import com.pinchez.wallet.transaction.enums.TransactionType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Currency;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.UuidGenerator;

@Entity
@Table(name = "transactions")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class WalletTransaction extends CreationAuditedEntity {

    @Id
    @GeneratedValue
    @UuidGenerator(style = UuidGenerator.Style.VERSION_7)
    private UUID id;

    @Column(nullable = false, unique = true, length = 12)
    private String reference;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private TransactionType type;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "from_account_id")
    private Account fromAccount;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "to_account_id")
    private Account toAccount;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "merchant_id")
    private Merchant merchant;

    @Column(length = 10)
    private String agentNumber;

    @Column(nullable = false, precision = 19, scale = 2)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private Currency currency;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 10)
    private TransactionStatus status;

    @Enumerated(EnumType.STRING)
    @Column(length = 30)
    private FailureReason failureReason;

    public static WalletTransaction p2p(
            String reference, Account from, Account to, BigDecimal amount, Currency currency) {
        var transaction = completed(reference, TransactionType.P2P, amount, currency);
        transaction.fromAccount = from;
        transaction.toAccount = to;
        return transaction;
    }

    public static WalletTransaction topUp(
            String reference, String agentNumber, Account to, BigDecimal amount, Currency currency) {
        var transaction = completed(reference, TransactionType.TOPUP, amount, currency);
        transaction.agentNumber = agentNumber;
        transaction.toAccount = to;
        return transaction;
    }

    public static WalletTransaction merchantPayment(
            String reference, Account from, Merchant merchant, BigDecimal amount, Currency currency) {
        var transaction = completed(reference, TransactionType.MERCHANT_PAYMENT, amount, currency);
        transaction.fromAccount = from;
        transaction.merchant = merchant;
        return transaction;
    }

    public void decline(FailureReason reason) {
        this.status = TransactionStatus.FAILED;
        this.failureReason = reason;
    }

    private static WalletTransaction completed(
            String reference, TransactionType type, BigDecimal amount, Currency currency) {
        var transaction = new WalletTransaction();
        transaction.reference = reference;
        transaction.type = type;
        transaction.amount = amount.setScale(2, RoundingMode.UNNECESSARY);
        transaction.currency = currency;
        transaction.status = TransactionStatus.COMPLETED;
        return transaction;
    }
}
