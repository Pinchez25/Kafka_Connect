package com.pinchez.streams.topology.fraud;

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

class NewAccountLargeTransferRuleTest {

    private static final UUID SENDER = UUID.randomUUID();

    private final TopologyHarness harness = TopologyHarness.withLatestAccounts();
    private final TestOutputTopic<String, FraudAlert> alerts = alerts(harness);

    @AfterEach
    void tearDown() {
        harness.close();
    }

    @Test
    void alertsImmediatelyWhenAnHourOldAccountSendsALargeAmount() {
        harness.pipe(CdcRecords.account(SENDER, "BASIC").build());

        var transfer = harness.pipe(
                CdcRecords.p2p(SENDER, UUID.randomUUID(), "15000.00").build());

        var alert = alerts.readValue();
        assertThat(alert.getRule()).isEqualTo(FraudRule.NEW_ACCOUNT_LARGE_TRANSFER);
        assertThat(alert.getTransactionIds()).containsExactly(transfer.getId());
    }

    @Test
    void ignoresEstablishedAccountsAndSmallAmounts() {
        harness.pipe(CdcRecords.account(SENDER, "BASIC")
                .setCreatedAt(CdcRecords.OPENED_AT.minus(Duration.ofDays(2)).toString())
                .build());
        harness.pipe(CdcRecords.p2p(SENDER, UUID.randomUUID(), "15000.00").build());

        var young = UUID.randomUUID();
        harness.pipe(CdcRecords.account(young, "BASIC").build());
        harness.pipe(CdcRecords.p2p(young, UUID.randomUUID(), "9999.00").build());

        assertThat(alerts.isEmpty()).isTrue();
    }
}
