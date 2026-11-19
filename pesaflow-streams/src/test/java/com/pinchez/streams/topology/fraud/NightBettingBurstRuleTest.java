package com.pinchez.streams.topology.fraud;

import static com.pinchez.streams.topology.fraud.FraudRuleTestSupport.alerts;
import static com.pinchez.streams.topology.fraud.FraudRuleTestSupport.nairobi;
import static org.assertj.core.api.Assertions.assertThat;

import com.pinchez.events.FraudAlert;
import com.pinchez.events.FraudRule;
import com.pinchez.streams.support.CdcRecords;
import com.pinchez.streams.support.TopologyHarness;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NightBettingBurstRuleTest {

    private static final UUID GAMBLER = UUID.randomUUID();
    private static final UUID BOOKMAKER = UUID.randomUUID();

    private final TopologyHarness harness = TopologyHarness.withLatestAccounts();
    private final TestOutputTopic<String, FraudAlert> alerts = alerts(harness);

    @BeforeEach
    void registerBookmaker() {
        harness.pipe(CdcRecords.merchant(BOOKMAKER, "BETTING"));
    }

    @AfterEach
    void tearDown() {
        harness.close();
    }

    @Test
    void alertsOnceTheNightSessionGoesQuiet() {
        bet(GAMBLER, nairobi("2026-11-19T01:00:00"));
        bet(GAMBLER, nairobi("2026-11-19T01:04:00"));
        bet(GAMBLER, nairobi("2026-11-19T01:09:00"));

        bet(UUID.randomUUID(), nairobi("2026-11-19T01:15:00"));
        assertThat(alerts.isEmpty())
                .as("session ends 01:09 + 10m gap + 2m grace = 01:21")
                .isTrue();

        bet(UUID.randomUUID(), nairobi("2026-11-19T01:30:00"));
        var alert = alerts.readValue();
        assertThat(alert.getRule()).isEqualTo(FraudRule.NIGHT_BETTING_BURST);
        assertThat(alert.getAccountId()).isEqualTo(GAMBLER);
        assertThat(alert.getTransactionCount()).contains(3L);
    }

    @Test
    void ignoresDaytimeBetting() {
        bet(GAMBLER, nairobi("2026-11-19T14:00:00"));
        bet(GAMBLER, nairobi("2026-11-19T14:01:00"));
        bet(GAMBLER, nairobi("2026-11-19T14:02:00"));

        bet(UUID.randomUUID(), nairobi("2026-11-20T02:00:00"));

        assertThat(alerts.isEmpty()).isTrue();
    }

    private void bet(UUID account, Instant at) {
        harness.pipe(CdcRecords.merchantPayment(account, BOOKMAKER, "500.00")
                .setCreatedAt(at.toString())
                .build());
    }
}
