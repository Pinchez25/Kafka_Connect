package com.pinchez.streams.topology.fraud;

import static com.pinchez.streams.topology.fraud.FraudRuleTestSupport.NINE_AM_UTC;
import static com.pinchez.streams.topology.fraud.FraudRuleTestSupport.alerts;
import static org.assertj.core.api.Assertions.assertThat;

import com.pinchez.events.FraudAlert;
import com.pinchez.events.FraudRule;
import com.pinchez.streams.support.CdcRecords;
import com.pinchez.streams.support.TopologyHarness;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RollingSpendRuleTest {

    private static final UUID SENDER = UUID.randomUUID();

    private final TopologyHarness harness = TopologyHarness.withLatestAccounts();
    private final TestOutputTopic<String, FraudAlert> alerts = alerts(harness);

    @BeforeEach
    void openBasicAccount() {
        harness.pipe(CdcRecords.account(SENDER, "BASIC").build());
    }

    @AfterEach
    void tearDown() {
        harness.close();
    }

    @Test
    void alertsWhenTenMinutesOfSpendingExceedsHalfTheDailyLimit() {
        for (int i = 0; i < 4; i++) {
            send("9000.00", NINE_AM_UTC.plus(Duration.ofMinutes(2L * i)));
        }
        assertThat(alerts.isEmpty())
                .as("emit-on-window-close holds results back")
                .isTrue();

        harness.advanceWallClock(Duration.ofSeconds(5));
        tick(NINE_AM_UTC.plus(Duration.ofMinutes(20)));

        var alert = alerts.readValue();
        assertThat(alert.getRule()).isEqualTo(FraudRule.ROLLING_SPEND);
        assertThat(alert.getAmount())
                .hasValueSatisfying(amount -> assertThat(amount).isEqualByComparingTo("36000.00"));
        assertThat(alert.getTransactionCount()).contains(4L);
        assertThat(alerts.isEmpty()).isTrue();
    }

    @Test
    void staysQuietWhenTheSameSpendIsSpreadOut() {
        for (int i = 0; i < 4; i++) {
            send("9000.00", NINE_AM_UTC.plus(Duration.ofMinutes(11L * i)));
        }

        harness.advanceWallClock(Duration.ofSeconds(5));
        tick(NINE_AM_UTC.plus(Duration.ofHours(2)));

        assertThat(alerts.isEmpty()).isTrue();
    }

    private void send(String amount, Instant at) {
        harness.pipe(CdcRecords.p2p(SENDER, UUID.randomUUID(), amount)
                .setCreatedAt(at.toString())
                .build());
    }

    private void tick(Instant at) {
        harness.pipe(CdcRecords.p2p(UUID.randomUUID(), UUID.randomUUID(), "1.00")
                .setCreatedAt(at.toString())
                .build());
    }
}
