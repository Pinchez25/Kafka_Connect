package com.pinchez.streams.handler;

import static org.assertj.core.api.Assertions.assertThat;

import com.pinchez.events.Balance;
import com.pinchez.streams.support.CdcRecords;
import com.pinchez.streams.support.TopologyHarness;
import com.pinchez.streams.topology.StoreNames;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class PoisonRecordTest {

    private final TopologyHarness harness = TopologyHarness.withLatestAccounts();

    @AfterEach
    void tearDown() {
        harness.close();
    }

    @Test
    void aPoisonCdcRecordIsDeadLetteredAndTheNextRecordIsStillProcessed() {
        var poison = "this is not avro".getBytes(StandardCharsets.UTF_8);
        harness.pipeRawTransaction("poison-key", poison);

        var recipient = UUID.randomUUID();
        harness.pipe(CdcRecords.topUp(recipient, "100.00").build());

        var deadLetter = harness.deadLetters().readRecord();
        assertThat(deadLetter.value()).isEqualTo(poison);
        assertThat(new String(
                        deadLetter.headers().lastHeader(DeadLetterRecords.TOPIC).value(), StandardCharsets.UTF_8))
                .isEqualTo(TopologyHarness.TOPICS.cdcTransactions());
        assertThat(harness.<Balance>store(StoreNames.BALANCES)
                        .get(recipient.toString())
                        .getAmount())
                .isEqualByComparingTo("100.00");
    }
}
