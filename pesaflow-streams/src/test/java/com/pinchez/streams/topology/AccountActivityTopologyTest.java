package com.pinchez.streams.topology;

import static org.assertj.core.api.Assertions.assertThat;

import com.pinchez.streams.support.CdcRecords;
import com.pinchez.streams.support.TopologyHarness;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class AccountActivityTopologyTest {

    private static final UUID ACCOUNT = UUID.randomUUID();
    private static final Instant NINE = Instant.parse("2026-11-18T09:00:00Z");

    private final TopologyHarness harness = TopologyHarness.withLatestAccounts();

    @AfterEach
    void tearDown() {
        harness.close();
    }

    @Test
    void countsTransactionsPerMinute() {
        topUpAt(NINE.plusSeconds(10));
        topUpAt(NINE.plusSeconds(40));
        topUpAt(NINE.plusSeconds(70));

        assertThat(countsBetween(NINE, NINE.plus(Duration.ofMinutes(5))))
                .containsExactly(Map.entry(NINE, 2L), Map.entry(NINE.plusSeconds(60), 1L));
    }

    @Test
    void windowsOlderThanTheRetentionPeriodAreGone() {
        topUpAt(NINE);
        topUpAt(NINE.plus(Duration.ofHours(2)));

        assertThat(countsBetween(NINE.minus(Duration.ofMinutes(1)), NINE.plus(Duration.ofMinutes(1))))
                .isEmpty();
    }

    private Map<Instant, Long> countsBetween(Instant from, Instant to) {
        var counts = new LinkedHashMap<Instant, Long>();
        try (var windows =
                harness.<Long>windowStore(StoreNames.ACCOUNT_ACTIVITY).fetch(ACCOUNT.toString(), from, to)) {
            windows.forEachRemaining(window -> counts.put(Instant.ofEpochMilli(window.key), window.value));
        }
        return counts;
    }

    private void topUpAt(Instant at) {
        harness.pipe(
                CdcRecords.topUp(ACCOUNT, "100.00").setCreatedAt(at.toString()).build());
    }
}
