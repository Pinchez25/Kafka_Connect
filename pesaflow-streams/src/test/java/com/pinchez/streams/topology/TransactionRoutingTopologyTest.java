package com.pinchez.streams.topology;

import static org.assertj.core.api.Assertions.assertThat;

import com.pinchez.events.CompletedTransaction;
import com.pinchez.events.TransactionType;
import com.pinchez.streams.support.CdcRecords;
import com.pinchez.streams.support.TopologyHarness;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TransactionRoutingTopologyTest {

    private final TopologyHarness harness = TopologyHarness.withLatestAccounts();
    private final TestOutputTopic<String, CompletedTransaction> p2pTransfers =
            harness.output(TopologyHarness.TOPICS.p2pTransfers());
    private final TestOutputTopic<String, CompletedTransaction> topUps =
            harness.output(TopologyHarness.TOPICS.topUps());
    private final TestOutputTopic<String, CompletedTransaction> merchantPayments =
            harness.output(TopologyHarness.TOPICS.merchantPayments());

    @AfterEach
    void tearDown() {
        harness.close();
    }

    @Test
    void routesEachCompletedTransactionToTheTopicForItsType() {
        var p2p = harness.pipe(
                CdcRecords.p2p(UUID.randomUUID(), UUID.randomUUID(), "100.00").build());
        var topUp = harness.pipe(CdcRecords.topUp(UUID.randomUUID(), "200.00").build());
        var payment = harness.pipe(CdcRecords.merchantPayment(UUID.randomUUID(), UUID.randomUUID(), "300.00")
                .build());

        assertThat(p2pTransfers.readKeyValuesToList())
                .singleElement()
                .satisfies(
                        record -> assertThat(record.key).isEqualTo(p2p.getId().toString()));
        assertThat(topUps.readValuesToList())
                .extracting(CompletedTransaction::getTransactionId)
                .containsExactly(topUp.getId());
        assertThat(merchantPayments.readValuesToList())
                .extracting(CompletedTransaction::getTransactionId)
                .containsExactly(payment.getId());
    }

    @Test
    void dropsDeclinedAttempts() {
        harness.pipe(CdcRecords.p2p(UUID.randomUUID(), UUID.randomUUID(), "100.00")
                .setStatus("FAILED")
                .setFailureReason("DAILY_LIMIT_EXCEEDED")
                .build());

        assertThat(p2pTransfers.isEmpty()).isTrue();
    }

    @Test
    void mapsTheCdcRowToACleanEvent() {
        var row = harness.pipe(CdcRecords.merchantPayment(UUID.randomUUID(), UUID.randomUUID(), "350.50")
                .build());

        var event = merchantPayments.readValue();

        assertThat(event.getTransactionId()).isEqualTo(row.getId());
        assertThat(event.getType()).isEqualTo(TransactionType.MERCHANT_PAYMENT);
        assertThat(event.getFromAccountId()).isEqualTo(row.getFromAccountId());
        assertThat(event.getToAccountId()).isEmpty();
        assertThat(event.getMerchantId()).isEqualTo(row.getMerchantId());
        assertThat(event.getAmount()).isEqualByComparingTo(new BigDecimal("350.50"));
        assertThat(event.getOccurredAt()).isEqualTo(Instant.parse(row.getCreatedAt()));
    }

    @Test
    void ignoresTombstones() {
        harness.pipeTombstone("some-id");

        assertThat(p2pTransfers.isEmpty()).isTrue();
        assertThat(topUps.isEmpty()).isTrue();
        assertThat(merchantPayments.isEmpty()).isTrue();
    }
}
