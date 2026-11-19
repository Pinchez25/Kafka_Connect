package com.pinchez.streams.topology.fraud;

import static com.pinchez.streams.topology.fraud.FraudRuleTestSupport.NINE_AM_UTC;
import static com.pinchez.streams.topology.fraud.FraudRuleTestSupport.alerts;
import static org.assertj.core.api.Assertions.assertThat;

import com.pinchez.events.FraudAlert;
import com.pinchez.events.FraudRule;
import com.pinchez.streams.support.CdcRecords;
import com.pinchez.streams.support.TopologyHarness;
import java.time.Duration;
import java.util.UUID;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class RoundTripTransferRuleTest {

    private static final UUID ALICE = UUID.randomUUID();
    private static final UUID BOB = UUID.randomUUID();

    private final TopologyHarness harness = TopologyHarness.withLatestAccounts();
    private final TestOutputTopic<String, FraudAlert> alerts = alerts(harness);

    @AfterEach
    void tearDown() {
        harness.close();
    }

    @Test
    void alertsTheOriginalSenderWhenMoneyComesStraightBack() {
        var outbound = harness.pipe(CdcRecords.p2p(ALICE, BOB, "2000.00")
                .setCreatedAt(NINE_AM_UTC.toString())
                .build());
        var returned = harness.pipe(CdcRecords.p2p(BOB, ALICE, "1950.00")
                .setCreatedAt(NINE_AM_UTC.plus(Duration.ofMinutes(2)).toString())
                .build());

        var record = alerts.readKeyValue();
        assertThat(record.key).isEqualTo(ALICE.toString());
        assertThat(record.value.getRule()).isEqualTo(FraudRule.ROUND_TRIP_TRANSFER);
        assertThat(record.value.getTransactionIds()).containsExactly(outbound.getId(), returned.getId());
        assertThat(alerts.isEmpty())
                .as("the pair matches in both directions but alerts once")
                .isTrue();
    }

    @Test
    void ignoresUnrelatedPaymentsBetweenTheSamePair() {
        harness.pipe(CdcRecords.p2p(ALICE, BOB, "2000.00")
                .setCreatedAt(NINE_AM_UTC.toString())
                .build());
        harness.pipe(CdcRecords.p2p(BOB, ALICE, "150.00")
                .setCreatedAt(NINE_AM_UTC.plus(Duration.ofMinutes(2)).toString())
                .build());

        assertThat(alerts.isEmpty()).isTrue();
    }

    @Test
    void ignoresReturnsOutsideTheWindow() {
        harness.pipe(CdcRecords.p2p(ALICE, BOB, "2000.00")
                .setCreatedAt(NINE_AM_UTC.toString())
                .build());
        harness.pipe(CdcRecords.p2p(BOB, ALICE, "1950.00")
                .setCreatedAt(NINE_AM_UTC.plus(Duration.ofMinutes(6)).toString())
                .build());

        assertThat(alerts.isEmpty()).isTrue();
    }
}
