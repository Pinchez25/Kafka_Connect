package com.pinchez.wallet.transaction.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import com.pinchez.TestcontainersConfiguration;
import com.pinchez.wallet.account.dto.ChangeAccountStatusRequest;
import com.pinchez.wallet.account.dto.OpenAccountRequest;
import com.pinchez.wallet.account.enums.AccountStatus;
import com.pinchez.wallet.account.enums.KycLevel;
import com.pinchez.wallet.account.service.AccountService;
import com.pinchez.wallet.merchant.dto.RegisterMerchantRequest;
import com.pinchez.wallet.merchant.enums.MerchantCategory;
import com.pinchez.wallet.merchant.service.MerchantService;
import com.pinchez.wallet.transaction.dto.TransactionRequest.MerchantPayment;
import com.pinchez.wallet.transaction.dto.TransactionRequest.P2pTransfer;
import com.pinchez.wallet.transaction.dto.TransactionRequest.TopUp;
import com.pinchez.wallet.transaction.dto.TransactionResponse;
import com.pinchez.wallet.transaction.enums.FailureReason;
import com.pinchez.wallet.transaction.enums.TransactionStatus;
import com.pinchez.wallet.transaction.enums.TransactionType;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.assertj.MockMvcTester;

@SpringBootTest
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
class TransactionIntegrationTest {

    @Autowired
    TransactionService transactionService;

    @Autowired
    AccountService accountService;

    @Autowired
    MerchantService merchantService;

    @Autowired
    MockMvcTester mvc;

    @Test
    void completesTransfersWithinTheDailyLimitAndDeclinesBeyondIt() {
        var sender = openAccount(KycLevel.BASIC);
        var recipient = openAccount(KycLevel.BASIC);

        var first = transactionService.submit(new P2pTransfer(sender, recipient, new BigDecimal("60000")));
        var second = transactionService.submit(new P2pTransfer(sender, recipient, new BigDecimal("10000")));
        var third = transactionService.submit(new P2pTransfer(sender, recipient, new BigDecimal("1")));

        assertThat(first.status()).isEqualTo(TransactionStatus.COMPLETED);
        assertThat(second.status()).isEqualTo(TransactionStatus.COMPLETED);
        assertThat(third.status()).isEqualTo(TransactionStatus.FAILED);
        assertThat(third.failureReason()).isEqualTo(FailureReason.DAILY_LIMIT_EXCEEDED);
    }

    @Test
    void declinedAttemptsDoNotCountTowardsTheDailyLimit() {
        var sender = openAccount(KycLevel.BASIC);
        var recipient = openAccount(KycLevel.BASIC);

        var declined = transactionService.submit(new P2pTransfer(sender, recipient, new BigDecimal("70001")));
        var completed = transactionService.submit(new P2pTransfer(sender, recipient, new BigDecimal("70000")));

        assertThat(declined.failureReason()).isEqualTo(FailureReason.DAILY_LIMIT_EXCEEDED);
        assertThat(completed.status()).isEqualTo(TransactionStatus.COMPLETED);
    }

    @Test
    void declinesWhenEitherPartyIsFrozen() {
        var frozen = openAccount(KycLevel.FULL);
        var active = openAccount(KycLevel.FULL);
        accountService.changeStatus(frozen, new ChangeAccountStatusRequest(AccountStatus.FROZEN));
        var till = registerMerchant();

        var fromFrozen = transactionService.submit(new P2pTransfer(frozen, active, BigDecimal.TEN));
        var toFrozen = transactionService.submit(new P2pTransfer(active, frozen, BigDecimal.TEN));
        var topUpFrozen = transactionService.submit(new TopUp(frozen, "123456", BigDecimal.TEN));
        var payFromFrozen = transactionService.submit(new MerchantPayment(frozen, till, BigDecimal.TEN));

        assertThat(List.of(fromFrozen, toFrozen, topUpFrozen, payFromFrozen))
                .extracting(TransactionResponse::type, TransactionResponse::failureReason)
                .containsExactly(
                        tuple(TransactionType.P2P, FailureReason.SENDER_FROZEN),
                        tuple(TransactionType.P2P, FailureReason.RECIPIENT_FROZEN),
                        tuple(TransactionType.TOPUP, FailureReason.RECIPIENT_FROZEN),
                        tuple(TransactionType.MERCHANT_PAYMENT, FailureReason.SENDER_FROZEN));
    }

    @Test
    void recordsMerchantPaymentsAgainstTheTill() {
        var payer = openAccount(KycLevel.BASIC);
        var till = registerMerchant();

        var payment = transactionService.submit(new MerchantPayment(payer, till, new BigDecimal("350.50")));

        assertThat(payment.status()).isEqualTo(TransactionStatus.COMPLETED);
        assertThat(payment.merchantId()).isNotNull();
        assertThat(payment.amount()).isEqualTo(new BigDecimal("350.50"));
        assertThat(transactionService.get(payment.id())).isEqualTo(payment);
    }

    @Test
    void acceptsPolymorphicJsonAndRejectsUnknownTypes() {
        var sender = openAccount(KycLevel.BASIC);
        var recipient = openAccount(KycLevel.BASIC);

        var created = mvc.post()
                .uri("/transactions")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                        {"type": "P2P", "fromAccountId": "%s", "toAccountId": "%s", "amount": 250}
                        """.formatted(sender, recipient));
        var unknownType = mvc.post()
                .uri("/transactions")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                        {"type": "REVERSAL", "amount": 250}
                        """);

        assertThat(created).hasStatus(HttpStatus.CREATED).containsHeader("Location");
        assertThat(created).bodyJson().extractingPath("$.status").isEqualTo("COMPLETED");
        assertThat(unknownType).hasStatus(HttpStatus.BAD_REQUEST);
        assertThat(unknownType).bodyJson().extractingPath("$.type").isEqualTo("urn:pesaflow:problem:malformed-request");
    }

    @Test
    void unknownAccountIsNotFoundAndNothingIsRecorded() {
        var recipient = openAccount(KycLevel.BASIC);

        var result = mvc.post()
                .uri("/transactions")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                        {"type": "P2P", "fromAccountId": "%s", "toAccountId": "%s", "amount": 250}
                        """.formatted(UUID.randomUUID(), recipient));

        assertThat(result).hasStatus(HttpStatus.NOT_FOUND);
        assertThat(transactionService.list(recipient, Pageable.unpaged()).totalElements())
                .isZero();
    }

    private UUID openAccount(KycLevel kycLevel) {
        var phone = "2547%08d".formatted(ThreadLocalRandom.current().nextInt(100_000_000));
        return accountService
                .open(new OpenAccountRequest(phone, "Test User", kycLevel))
                .id();
    }

    private String registerMerchant() {
        var till = String.valueOf(ThreadLocalRandom.current().nextInt(100_000, 1_000_000));
        return merchantService
                .register(new RegisterMerchantRequest(till, "Test Shop", MerchantCategory.GROCERY))
                .tillNumber();
    }
}
