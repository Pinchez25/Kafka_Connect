package com.pinchez.wallet.fraud.controller;

import static org.assertj.core.api.Assertions.assertThat;

import com.pinchez.TestcontainersConfiguration;
import com.pinchez.wallet.account.dto.OpenAccountRequest;
import com.pinchez.wallet.account.enums.AccountStatus;
import com.pinchez.wallet.account.service.AccountService;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

@SpringBootTest
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
class FraudAlertWebhookIntegrationTest {

    @Autowired
    MockMvcTester mvc;

    @Autowired
    AccountService accountService;

    @Test
    void anAlertFreezesTheAccountAndARedeliveryChangesNothing() {
        var account = openAccount();

        assertThat(post(alert(account))).bodyJson().extractingPath("$.outcome").isEqualTo("FROZEN");
        assertThat(accountService.get(account).status()).isEqualTo(AccountStatus.FROZEN);

        assertThat(post(alert(account))).bodyJson().extractingPath("$.outcome").isEqualTo("ALREADY_FROZEN");
    }

    @Test
    void anUnknownAccountIsAcknowledgedSoTheSinkDoesNotRetryForever() {
        assertThat(post(alert(UUID.randomUUID())))
                .hasStatusOk()
                .bodyJson()
                .extractingPath("$.outcome")
                .isEqualTo("ACCOUNT_NOT_FOUND");
    }

    @Test
    void extraAlertFieldsAreIgnoredButMissingOnesAreRejected() {
        var account = openAccount();
        var full = alert(account).replace("}", ", \"windowStart\": null, \"transactionIds\": []}");

        assertThat(post(full)).hasStatusOk();
        assertThat(post("{\"rule\": \"TRANSFER_VELOCITY\"}")).hasStatus(HttpStatus.BAD_REQUEST);
    }

    private MvcTestResult post(String body) {
        return mvc.post()
                .uri("/webhooks/fraud-alerts")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body)
                .exchange();
    }

    private static String alert(UUID accountId) {
        return """
                {"alertId": "%s", "rule": "TRANSFER_VELOCITY", "accountId": "%s", "description": "6 transfers"}""".formatted(UUID.randomUUID(), accountId);
    }

    private UUID openAccount() {
        var phone = "2547%08d".formatted(ThreadLocalRandom.current().nextInt(100_000_000));
        return accountService
                .open(new OpenAccountRequest(phone, "Webhook User", null))
                .id();
    }
}
