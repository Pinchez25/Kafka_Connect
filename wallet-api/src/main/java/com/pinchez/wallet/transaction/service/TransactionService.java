package com.pinchez.wallet.transaction.service;

import com.pinchez.common.exception.ResourceNotFoundException;
import com.pinchez.wallet.account.entity.Account;
import com.pinchez.wallet.account.repository.AccountRepository;
import com.pinchez.wallet.common.dto.PageResponse;
import com.pinchez.wallet.config.WalletProperties;
import com.pinchez.wallet.merchant.entity.Merchant;
import com.pinchez.wallet.merchant.repository.MerchantRepository;
import com.pinchez.wallet.transaction.dto.TransactionRequest;
import com.pinchez.wallet.transaction.dto.TransactionRequest.MerchantPayment;
import com.pinchez.wallet.transaction.dto.TransactionRequest.P2pTransfer;
import com.pinchez.wallet.transaction.dto.TransactionRequest.TopUp;
import com.pinchez.wallet.transaction.dto.TransactionResponse;
import com.pinchez.wallet.transaction.entity.WalletTransaction;
import com.pinchez.wallet.transaction.enums.FailureReason;
import com.pinchez.wallet.transaction.mapper.TransactionMapper;
import com.pinchez.wallet.transaction.repository.WalletTransactionRepository;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Currency;
import java.util.Optional;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.Nullable;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Declined attempts are persisted with status FAILED rather than rejected with an error, because downstream fraud
 * detection needs to see them.
 */
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class TransactionService {

    private final WalletTransactionRepository transactions;
    private final AccountRepository accounts;
    private final MerchantRepository merchants;
    private final TransactionMapper mapper;
    private final TransactionReferenceGenerator referenceGenerator;
    private final WalletProperties walletProperties;
    private final Clock clock;

    @Transactional
    public TransactionResponse submit(TransactionRequest request) {
        var now = clock.instant();
        var transaction = switch (request) {
            case P2pTransfer(var fromAccountId, var toAccountId, var amount) ->
                transfer(fromAccountId, toAccountId, amount, now);
            case TopUp(var toAccountId, var agentNumber, var amount) -> topUp(toAccountId, agentNumber, amount, now);
            case MerchantPayment(var fromAccountId, var tillNumber, var amount) ->
                payMerchant(fromAccountId, tillNumber, amount, now);
        };
        return mapper.toResponse(transactions.save(transaction));
    }

    public TransactionResponse get(UUID id) {
        return transactions
                .findById(id)
                .map(mapper::toResponse)
                .orElseThrow(() -> new ResourceNotFoundException(WalletTransaction.class, id));
    }

    public PageResponse<TransactionResponse> list(@Nullable UUID accountId, Pageable pageable) {
        var page = accountId == null
                ? transactions.findAll(pageable)
                : transactions.findInvolvingAccount(accountId, pageable);
        return PageResponse.from(page.map(mapper::toResponse));
    }

    private WalletTransaction transfer(UUID fromAccountId, UUID toAccountId, BigDecimal amount, Instant now) {
        var sender = lockSender(fromAccountId);
        var recipient = findAccount(toAccountId);
        var transaction = WalletTransaction.p2p(referenceGenerator.next(), sender, recipient, amount, currency(), now);
        senderRejection(sender, amount, now)
                .or(() -> recipientRejection(recipient))
                .ifPresent(transaction::decline);
        return transaction;
    }

    private WalletTransaction topUp(UUID toAccountId, String agentNumber, BigDecimal amount, Instant now) {
        var recipient = findAccount(toAccountId);
        var transaction =
                WalletTransaction.topUp(referenceGenerator.next(), agentNumber, recipient, amount, currency(), now);
        recipientRejection(recipient).ifPresent(transaction::decline);
        return transaction;
    }

    private WalletTransaction payMerchant(UUID fromAccountId, String tillNumber, BigDecimal amount, Instant now) {
        var sender = lockSender(fromAccountId);
        var merchant = merchants
                .findByTillNumber(tillNumber)
                .orElseThrow(() -> new ResourceNotFoundException(Merchant.class, tillNumber));
        var transaction =
                WalletTransaction.merchantPayment(referenceGenerator.next(), sender, merchant, amount, currency(), now);
        senderRejection(sender, amount, now).ifPresent(transaction::decline);
        return transaction;
    }

    private Optional<FailureReason> senderRejection(Account sender, BigDecimal amount, Instant now) {
        if (!sender.isActive()) {
            return Optional.of(FailureReason.SENDER_FROZEN);
        }
        var sentToday = transactions.sumCompletedOutgoingSince(sender.getId(), startOfBusinessDay(now));
        if (sentToday.add(amount).compareTo(sender.getDailyLimit()) > 0) {
            return Optional.of(FailureReason.DAILY_LIMIT_EXCEEDED);
        }
        return Optional.empty();
    }

    private static Optional<FailureReason> recipientRejection(Account recipient) {
        return recipient.isActive() ? Optional.empty() : Optional.of(FailureReason.RECIPIENT_FROZEN);
    }

    private Instant startOfBusinessDay(Instant now) {
        var zone = walletProperties.zoneId();
        return LocalDate.ofInstant(now, zone).atStartOfDay(zone).toInstant();
    }

    // Row lock serialises concurrent sends so they can't jointly exceed the daily limit.
    private Account lockSender(UUID accountId) {
        return accounts.findByIdForUpdate(accountId)
                .orElseThrow(() -> new ResourceNotFoundException(Account.class, accountId));
    }

    private Account findAccount(UUID accountId) {
        return accounts.findById(accountId).orElseThrow(() -> new ResourceNotFoundException(Account.class, accountId));
    }

    private Currency currency() {
        return walletProperties.currency();
    }
}
