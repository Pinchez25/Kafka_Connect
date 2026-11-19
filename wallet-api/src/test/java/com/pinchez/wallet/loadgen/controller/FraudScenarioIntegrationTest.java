package com.pinchez.wallet.loadgen.controller;

import static org.assertj.core.api.Assertions.assertThat;

import com.jayway.jsonpath.JsonPath;
import com.pinchez.TestcontainersConfiguration;
import com.pinchez.wallet.transaction.enums.TransactionStatus;
import com.pinchez.wallet.transaction.service.TransactionService;
import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

@SpringBootTest
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
class FraudScenarioIntegrationTest {

    @Autowired
    MockMvcTester mvc;

    @Autowired
    TransactionService transactionService;

    @Test
    void velocityBurstSendsEightTransfersFromOneFreshAccount() throws Exception {
        var result = mvc.post().uri("/load-generator/scenarios/velocity-burst").exchange();

        assertThat(result).hasStatus(HttpStatus.OK);
        var transactions = ids(result, "$.transactionIds").stream()
                .map(transactionService::get)
                .toList();
        assertThat(transactions)
                .hasSize(8)
                .allSatisfy(transaction -> assertThat(transaction.status()).isEqualTo(TransactionStatus.COMPLETED));
        assertThat(transactions)
                .extracting(transaction -> transaction.fromAccountId())
                .containsOnly(transactions.getFirst().fromAccountId());
    }

    @Test
    void lateEventsAreBackdated() throws Exception {
        var result = mvc.post().uri("/load-generator/scenarios/LATE_EVENTS").exchange();

        var createdAt = ids(result, "$.transactionIds").stream()
                .map(id -> transactionService.get(id).createdAt())
                .toList();
        var now = Instant.now();
        assertThat(createdAt.subList(0, 3)).allSatisfy(at -> assertThat(at).isBefore(now.minus(Duration.ofMinutes(9))));
        assertThat(createdAt.subList(3, 6))
                .allSatisfy(at -> assertThat(at).isBetween(now.minus(Duration.ofMinutes(1)), now));
    }

    @Test
    void unusualAmountEndsEachAccountsHabitWithOneLargePayment() throws Exception {
        var result = mvc.post().uri("/load-generator/scenarios/unusual-amount").exchange();

        var transactions = ids(result, "$.transactionIds").stream()
                .map(transactionService::get)
                .toList();
        assertThat(transactions)
                .hasSize(22)
                .allSatisfy(transaction -> assertThat(transaction.status()).isEqualTo(TransactionStatus.COMPLETED));
        assertThat(transactions.subList(20, 22))
                .extracting(transaction -> transaction.amount().intValue())
                .containsExactly(2500, 1500);
    }

    @Test
    void dormantAccountHasOnlyAFortyDayOldTopUp() throws Exception {
        var result = mvc.post().uri("/load-generator/scenarios/dormant-account").exchange();

        var transactions = ids(result, "$.transactionIds");
        assertThat(transactions).hasSize(1);
        assertThat(transactionService.get(transactions.getFirst()).createdAt())
                .isBefore(Instant.now().minus(Duration.ofDays(39)));
    }

    @Test
    void unknownScenarioIsABadRequest() {
        assertThat(mvc.post().uri("/load-generator/scenarios/heist")).hasStatus(HttpStatus.BAD_REQUEST);
    }

    private static List<UUID> ids(MvcTestResult result, String path) throws UnsupportedEncodingException {
        List<String> values = JsonPath.read(result.getResponse().getContentAsString(), path);
        return values.stream().map(UUID::fromString).toList();
    }
}
