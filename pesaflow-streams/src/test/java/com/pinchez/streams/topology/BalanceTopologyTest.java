package com.pinchez.streams.topology;

import static org.assertj.core.api.Assertions.assertThat;

import com.pinchez.events.Balance;
import com.pinchez.streams.support.CdcRecords;
import com.pinchez.streams.support.TopologyHarness;
import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class BalanceTopologyTest {

    private static final UUID WANJIKU = UUID.randomUUID();
    private static final UUID OTIENO = UUID.randomUUID();

    private final TopologyHarness harness = TopologyHarness.withLatestAccounts();

    @AfterEach
    void tearDown() {
        harness.close();
    }

    @Test
    void creditsTopUpsDebitsPaymentsAndMovesTransfersBetweenAccounts() {
        harness.pipe(CdcRecords.topUp(WANJIKU, "1000.00").build());
        harness.pipe(CdcRecords.p2p(WANJIKU, OTIENO, "250.00").build());
        harness.pipe(
                CdcRecords.merchantPayment(WANJIKU, UUID.randomUUID(), "100.50").build());

        assertThat(balanceOf(WANJIKU)).isEqualByComparingTo("649.50");
        assertThat(balanceOf(OTIENO)).isEqualByComparingTo("250.00");
        assertThat(harness.<Balance>store(StoreNames.BALANCES)
                        .get(WANJIKU.toString())
                        .getEntryCount())
                .isEqualTo(3);
    }

    @Test
    void ignoresDeclinedAttempts() {
        harness.pipe(CdcRecords.topUp(WANJIKU, "1000.00").build());
        harness.pipe(CdcRecords.p2p(WANJIKU, OTIENO, "250.00")
                .setStatus("FAILED")
                .setFailureReason("SENDER_FROZEN")
                .build());

        assertThat(balanceOf(WANJIKU)).isEqualByComparingTo("1000.00");
        assertThat(harness.<Balance>store(StoreNames.BALANCES).get(OTIENO.toString()))
                .isNull();
    }

    @Test
    void publishesEveryBalanceChangeToTheBalancesTopic() {
        var balances = harness.<Balance>output(TopologyHarness.TOPICS.balances());

        harness.pipe(CdcRecords.topUp(WANJIKU, "1000.00").build());
        harness.pipe(CdcRecords.topUp(WANJIKU, "500.00").build());

        assertThat(balances.readKeyValuesToMap()).containsOnlyKeys(WANJIKU.toString());
        assertThat(balanceOf(WANJIKU)).isEqualByComparingTo("1500.00");
    }

    private BigDecimal balanceOf(UUID accountId) {
        return harness.<Balance>store(StoreNames.BALANCES)
                .get(accountId.toString())
                .getAmount();
    }
}
