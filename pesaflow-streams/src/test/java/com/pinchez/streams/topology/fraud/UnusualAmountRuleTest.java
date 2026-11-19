package com.pinchez.streams.topology.fraud;

import static com.pinchez.streams.topology.fraud.FraudRuleTestSupport.alerts;
import static org.assertj.core.api.Assertions.assertThat;

import com.pinchez.events.FraudAlert;
import com.pinchez.events.FraudRule;
import com.pinchez.streams.processor.PesaflowHeaders;
import com.pinchez.streams.support.CdcRecords;
import com.pinchez.streams.support.TopologyHarness;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class UnusualAmountRuleTest {

    private static final Instant START = CdcRecords.OPENED_AT.plusSeconds(3_600);

    private final TopologyHarness harness = TopologyHarness.withLatestAccounts();
    private final TestOutputTopic<String, FraudAlert> alerts = alerts(harness);
    private final TestOutputTopic<String, FraudAlert> review = harness.output(TopologyHarness.TOPICS.fraudReview());

    @AfterEach
    void tearDown() {
        harness.close();
    }

    @Test
    void anAmountFarAboveTheAccountsHabitGoesToTheFraudAlertsTopic() {
        var sender = UUID.randomUUID();
        sendHabitualPayments(sender, 10);

        var large = send(sender, "2500.00", 10);

        var alert = unusualAmountAlerts(alerts);
        assertThat(alert.value().getAccountId()).isEqualTo(sender);
        assertThat(alert.value().getTransactionIds()).containsExactly(large);
        assertThat(PesaflowHeaders.read(alert.headers(), PesaflowHeaders.SEVERITY))
                .contains("HIGH");
        assertThat(review.isEmpty()).isTrue();
    }

    @Test
    void aModeratelyUnusualAmountGoesToTheReviewQueue() {
        var sender = UUID.randomUUID();
        sendHabitualPayments(sender, 10);

        send(sender, "1500.00", 10);

        var queued = review.readRecord();
        assertThat(queued.value().getRule()).isEqualTo(FraudRule.UNUSUAL_AMOUNT);
        assertThat(PesaflowHeaders.read(queued.headers(), PesaflowHeaders.SEVERITY))
                .contains("REVIEW");
        assertThat(alerts.readValuesToList()).noneMatch(alert -> alert.getRule() == FraudRule.UNUSUAL_AMOUNT);
    }

    @Test
    void staysQuietUntilThereIsEnoughHistory() {
        var sender = UUID.randomUUID();
        sendHabitualPayments(sender, 9);

        send(sender, "5000.00", 9);

        assertThat(review.isEmpty()).isTrue();
        assertThat(alerts.readValuesToList()).noneMatch(alert -> alert.getRule() == FraudRule.UNUSUAL_AMOUNT);
    }

    @Test
    void dslAlertsGetFreshHeadersRatherThanTheTriggeringRecordsOnes() {
        var sender = UUID.randomUUID();
        harness.pipe(CdcRecords.account(sender, "BASIC").build());
        harness.pipe(CdcRecords.p2p(sender, UUID.randomUUID(), "15000.00").build());

        var alert = alerts.readRecord();

        assertThat(PesaflowHeaders.read(alert.headers(), PesaflowHeaders.FRAUD_RULE))
                .contains(FraudRule.NEW_ACCOUNT_LARGE_TRANSFER.name());
        assertThat(PesaflowHeaders.read(alert.headers(), PesaflowHeaders.ALERT_ID))
                .contains(alert.value().getAlertId().toString());
        assertThat(alert.headers().lastHeader(PesaflowHeaders.SOURCE)).isNull();
    }

    /** Merchant payments, two minutes apart, so no other rule is involved. */
    private void sendHabitualPayments(UUID sender, int count) {
        for (int i = 0; i < count; i++) {
            send(sender, "100.00", i);
        }
    }

    private UUID send(UUID sender, String amount, int minutesIn) {
        return harness.pipe(CdcRecords.merchantPayment(sender, UUID.randomUUID(), amount)
                        .setCreatedAt(START.plusSeconds(120L * minutesIn).toString())
                        .build())
                .getId();
    }

    private static TestRecord<String, FraudAlert> unusualAmountAlerts(TestOutputTopic<String, FraudAlert> topic) {
        return topic.readRecordsToList().stream()
                .filter(record -> record.value().getRule() == FraudRule.UNUSUAL_AMOUNT)
                .findFirst()
                .orElseThrow();
    }
}
