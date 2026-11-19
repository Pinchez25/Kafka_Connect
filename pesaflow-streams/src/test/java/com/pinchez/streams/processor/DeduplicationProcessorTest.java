package com.pinchez.streams.processor;

import static org.assertj.core.api.Assertions.assertThat;

import com.pinchez.cdc.Transaction;
import com.pinchez.events.Balance;
import com.pinchez.events.EnrichedTransaction;
import com.pinchez.streams.support.CdcRecords;
import com.pinchez.streams.support.TopologyHarness;
import com.pinchez.streams.topology.StoreNames;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class DeduplicationProcessorTest {

    private static final UUID SENDER = UUID.randomUUID();
    private static final UUID RECIPIENT = UUID.randomUUID();

    private final TopologyHarness harness = TopologyHarness.withLatestAccounts();
    private final TestOutputTopic<String, EnrichedTransaction> enriched =
            harness.output(TopologyHarness.TOPICS.enrichedTransactions());

    @AfterEach
    void tearDown() {
        harness.close();
    }

    @Test
    void aRedeliveredRowIsDroppedBeforeItCanBeCountedTwice() {
        var transfer = CdcRecords.p2p(SENDER, RECIPIENT, "250.00").build();

        harness.pipe(transfer);
        harness.pipe(Transaction.newBuilder(transfer).setOp("r").build());

        assertThat(enriched.readValuesToList()).hasSize(1);
        assertThat(recipientBalance().getEntryCount()).isEqualTo(1);
        assertThat(harness.metricTotal("duplicates-dropped-total")).isEqualTo(1.0);
    }

    @Test
    void forwardedRecordsCarryTheirCdcSourceAsAHeader() {
        harness.pipe(CdcRecords.p2p(SENDER, RECIPIENT, "250.00").build());

        var record = enriched.readRecord();

        assertThat(PesaflowHeaders.read(record.headers(), PesaflowHeaders.SOURCE))
                .contains("cdc.transactions-0@0");
    }

    @Test
    void idsAreOnlyRememberedForTheRetentionPeriod() {
        var transfer = CdcRecords.p2p(SENDER, RECIPIENT, "250.00").build();
        harness.pipe(transfer);

        var afterRetention = Instant.parse(transfer.getCreatedAt())
                .plus(TopologyHarness.DEDUPLICATION_RETENTION)
                .plusSeconds(3_600);
        harness.pipe(CdcRecords.topUp(UUID.randomUUID(), "1.00")
                .setCreatedAt(afterRetention.toString())
                .build());
        harness.pipe(transfer);

        assertThat(recipientBalance().getEntryCount()).isEqualTo(2);
    }

    private Balance recipientBalance() {
        return harness.<Balance>store(StoreNames.BALANCES).get(RECIPIENT.toString());
    }
}
