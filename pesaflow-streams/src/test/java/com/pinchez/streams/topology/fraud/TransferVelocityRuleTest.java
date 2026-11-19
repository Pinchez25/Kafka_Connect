package com.pinchez.streams.topology.fraud;

import static com.pinchez.streams.topology.fraud.FraudRuleTestSupport.NINE_AM_UTC;
import static com.pinchez.streams.topology.fraud.FraudRuleTestSupport.alerts;
import static org.assertj.core.api.Assertions.assertThat;

import com.pinchez.events.FraudAlert;
import com.pinchez.events.FraudRule;
import com.pinchez.streams.support.CdcRecords;
import com.pinchez.streams.support.TopologyHarness;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TransferVelocityRuleTest {

    private static final UUID SENDER = UUID.randomUUID();

    private final TopologyHarness harness = TopologyHarness.withLatestAccounts();
    private final TestOutputTopic<String, FraudAlert> alerts = alerts(harness);

    @AfterEach
    void tearDown() {
        harness.close();
    }

    @Test
    void emitsOneFinalAlertOnlyAfterTheWindowAndGraceHaveClosed() {
        sendTransfers(6, NINE_AM_UTC);

        assertThat(alerts.isEmpty()).as("window still open").isTrue();

        tick(NINE_AM_UTC.plusSeconds(85));
        assertThat(alerts.isEmpty())
                .as("window closed at 09:01, but grace runs until 09:01:30")
                .isTrue();

        tick(NINE_AM_UTC.plusSeconds(95));
        var alert = alerts.readValue();
        assertThat(alert.getRule()).isEqualTo(FraudRule.TRANSFER_VELOCITY);
        assertThat(alert.getAccountId()).isEqualTo(SENDER);
        assertThat(alert.getTransactionCount()).contains(6L);
        assertThat(alert.getWindowStart()).contains(NINE_AM_UTC);
        assertThat(alerts.isEmpty())
                .as("suppress emits exactly one result per window")
                .isTrue();
    }

    @Test
    void staysQuietAtTheLimit() {
        sendTransfers(5, NINE_AM_UTC);

        tick(NINE_AM_UTC.plusSeconds(120));

        assertThat(alerts.isEmpty()).isTrue();
    }

    @Test
    void lateTransfersWithinGraceStillCountButLaterOnesAreDropped() {
        sendTransfers(5, NINE_AM_UTC);
        tick(NINE_AM_UTC.plusSeconds(75));
        sendTransfers(1, NINE_AM_UTC.plusSeconds(30));
        assertThat(harness.droppedRecords())
                .as("09:00:30 arrives at 09:01:15, inside grace")
                .isZero();

        tick(NINE_AM_UTC.plusSeconds(120));
        sendTransfers(1, NINE_AM_UTC.plusSeconds(40));

        assertThat(harness.droppedRecords())
                .as("09:00:40 arrives after the window closed")
                .isPositive();
        assertThat(alerts.readValue().getTransactionCount()).contains(6L);
    }

    private void sendTransfers(int count, Instant from) {
        for (int i = 0; i < count; i++) {
            harness.pipe(CdcRecords.p2p(SENDER, UUID.randomUUID(), "100.00")
                    .setCreatedAt(from.plusSeconds(i).toString())
                    .build());
        }
    }

    /** Stream time only moves when records arrive, so another account's transfer moves it forward. */
    private void tick(Instant at) {
        harness.pipe(CdcRecords.p2p(UUID.randomUUID(), UUID.randomUUID(), "1.00")
                .setCreatedAt(at.toString())
                .build());
    }
}
