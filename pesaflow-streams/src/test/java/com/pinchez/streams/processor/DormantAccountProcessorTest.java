package com.pinchez.streams.processor;

import static org.assertj.core.api.Assertions.assertThat;

import com.pinchez.events.DormantAccount;
import com.pinchez.events.LastActivity;
import com.pinchez.streams.support.CdcRecords;
import com.pinchez.streams.support.TopologyHarness;
import com.pinchez.streams.topology.StoreNames;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.processor.PunctuationType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class DormantAccountProcessorTest {

    private static final UUID QUIET = UUID.randomUUID();
    private static final Instant TOPPED_UP_AT = CdcRecords.OPENED_AT.plusSeconds(3_600);

    private TopologyHarness harness;
    private TestOutputTopic<String, DormantAccount> dormant;

    @AfterEach
    void tearDown() {
        harness.close();
    }

    @Test
    void streamTimePunctuationReportsAQuietAccountOnceWithItsBalance() {
        start(PunctuationType.STREAM_TIME);
        harness.pipe(CdcRecords.topUp(QUIET, "1500.00").build());

        tick(TOPPED_UP_AT.plus(Duration.ofDays(29)));
        assertThat(reportsForQuietAccount()).as("29 days quiet").isEmpty();

        tick(TOPPED_UP_AT.plus(TopologyHarness.DORMANCY).plusSeconds(60));
        assertThat(reportsForQuietAccount()).singleElement().satisfies(report -> {
            assertThat(report.getLastActivityAt()).isEqualTo(TOPPED_UP_AT);
            assertThat(report.getBalance()).contains(new BigDecimal("1500.00"));
        });

        tick(TOPPED_UP_AT.plus(TopologyHarness.DORMANCY).plus(Duration.ofHours(1)));
        assertThat(reportsForQuietAccount()).as("reported once per quiet spell").isEmpty();
    }

    @Test
    void newActivityStartsANewQuietSpell() {
        start(PunctuationType.STREAM_TIME);
        harness.pipe(CdcRecords.topUp(QUIET, "1500.00").build());
        tick(TOPPED_UP_AT.plus(TopologyHarness.DORMANCY).plusSeconds(60));
        assertThat(reportsForQuietAccount()).hasSize(1);

        var cameBack = TOPPED_UP_AT.plus(Duration.ofDays(40));
        harness.pipe(CdcRecords.topUp(QUIET, "10.00")
                .setCreatedAt(cameBack.toString())
                .build());
        assertThat(harness.<LastActivity>store(StoreNames.LAST_ACTIVITY)
                        .get(QUIET.toString())
                        .getReportedDormantAt())
                .isEmpty();

        tick(cameBack.plus(TopologyHarness.DORMANCY).plusSeconds(60));
        assertThat(reportsForQuietAccount())
                .singleElement()
                .satisfies(report -> assertThat(report.getLastActivityAt()).isEqualTo(cameBack));
    }

    @Test
    void streamTimeDoesNotMoveWithoutRecords() {
        start(PunctuationType.STREAM_TIME);
        harness.pipe(CdcRecords.topUp(QUIET, "1500.00").build());

        harness.advanceWallClock(TopologyHarness.DORMANCY.plusDays(1));

        assertThat(dormant.isEmpty()).isTrue();
    }

    @Test
    void wallClockPunctuationFiresWithNoTrafficAtAll() {
        start(PunctuationType.WALL_CLOCK_TIME);
        harness.pipe(CdcRecords.topUp(QUIET, "1500.00").build());

        harness.advanceWallClock(TopologyHarness.DORMANCY.plusDays(1));

        assertThat(dormant.readValue().getAccountId()).isEqualTo(QUIET);
    }

    private void start(PunctuationType punctuationType) {
        harness = TopologyHarness.withDormancyScan(punctuationType);
        dormant = harness.output(TopologyHarness.TOPICS.dormantAccounts());
    }

    /** Activity on another account, only to move stream time forward. */
    private void tick(Instant at) {
        harness.pipe(CdcRecords.topUp(UUID.randomUUID(), "1.00")
                .setCreatedAt(at.toString())
                .build());
    }

    /** Tick accounts eventually go quiet too, so only reports for the account under test count. */
    private List<DormantAccount> reportsForQuietAccount() {
        return dormant.readValuesToList().stream()
                .filter(report -> report.getAccountId().equals(QUIET))
                .toList();
    }
}
