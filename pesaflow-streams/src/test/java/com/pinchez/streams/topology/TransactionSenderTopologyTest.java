package com.pinchez.streams.topology;

import static org.assertj.core.api.Assertions.assertThat;

import com.pinchez.events.KycLevel;
import com.pinchez.events.TransactionSender;
import com.pinchez.streams.support.CdcRecords;
import com.pinchez.streams.support.TopologyHarness;
import java.time.Duration;
import java.util.UUID;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TransactionSenderTopologyTest {

    private static final UUID SENDER = UUID.randomUUID();

    private final TopologyHarness harness = TopologyHarness.withLatestAccounts();
    private final TestOutputTopic<String, TransactionSender> senders =
            harness.output(TopologyHarness.TOPICS.transactionSenders());

    @AfterEach
    void tearDown() {
        harness.close();
    }

    @Test
    void joinsEachTransactionToItsSendersAccount() {
        harness.pipe(CdcRecords.account(SENDER, "BASIC").build());

        var transfer =
                harness.pipe(CdcRecords.p2p(SENDER, UUID.randomUUID(), "100.00").build());

        var record = senders.readKeyValue();
        assertThat(record.key).isEqualTo(transfer.getId().toString());
        assertThat(record.value.getSenderKycLevel()).isEqualTo(KycLevel.BASIC);
    }

    @Test
    void reEmitsEveryTransactionWhenTheSenderChanges() {
        harness.pipe(CdcRecords.account(SENDER, "BASIC").build());
        var first =
                harness.pipe(CdcRecords.p2p(SENDER, UUID.randomUUID(), "100.00").build());
        var second = harness.pipe(
                CdcRecords.merchantPayment(SENDER, UUID.randomUUID(), "50.00").build());
        senders.readValuesToList();

        harness.pipe(
                CdcRecords.account(SENDER, "FULL").setVersion(1).build(),
                CdcRecords.OPENED_AT.plus(Duration.ofHours(2)));

        var updated = senders.readKeyValuesToMap();
        assertThat(updated)
                .containsOnlyKeys(first.getId().toString(), second.getId().toString());
        assertThat(updated.values()).allMatch(sender -> sender.getSenderKycLevel() == KycLevel.FULL);
    }

    @Test
    void skipsTopUpsBecauseTheyHaveNoSender() {
        harness.pipe(CdcRecords.account(SENDER, "BASIC").build());

        harness.pipe(CdcRecords.topUp(SENDER, "100.00").build());

        assertThat(senders.isEmpty()).isTrue();
    }
}
