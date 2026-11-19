package com.pinchez.streams.topology;

import static org.assertj.core.api.Assertions.assertThat;

import com.pinchez.events.AccountStatus;
import com.pinchez.events.EnrichedTransaction;
import com.pinchez.events.KycLevel;
import com.pinchez.events.MerchantCategory;
import com.pinchez.streams.support.CdcRecords;
import com.pinchez.streams.support.TopologyHarness;
import java.time.Duration;
import java.util.UUID;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;

class TransactionEnrichmentTopologyTest {

    private static final UUID SENDER = UUID.randomUUID();
    private static final UUID RECIPIENT = UUID.randomUUID();
    private static final UUID MERCHANT = UUID.randomUUID();

    @Test
    void addsSenderStateAccountAgeAndMerchantCategory() {
        try (var harness = TopologyHarness.withLatestAccounts()) {
            var enriched = harness.<EnrichedTransaction>output(TopologyHarness.TOPICS.enrichedTransactions());
            harness.pipe(CdcRecords.account(SENDER, "BASIC").build());
            harness.pipe(CdcRecords.merchant(MERCHANT, "BETTING"));

            var payment = harness.pipe(
                    CdcRecords.merchantPayment(SENDER, MERCHANT, "500.00").build());

            var record = enriched.readKeyValue();
            assertThat(record.key).isEqualTo(SENDER.toString());
            assertThat(record.value.getTransactionId()).isEqualTo(payment.getId());
            assertThat(record.value.getKycLevel()).contains(KycLevel.BASIC);
            assertThat(record.value.getAccountStatus()).contains(AccountStatus.ACTIVE);
            assertThat(record.value.getAccountAgeSeconds())
                    .contains(Duration.ofHours(1).toSeconds());
            assertThat(record.value.getMerchantCategory()).contains(MerchantCategory.BETTING);
        }
    }

    @Test
    void attributesTransfersToTheSenderAndTopUpsToTheRecipient() {
        try (var harness = TopologyHarness.withLatestAccounts()) {
            var enriched = harness.<EnrichedTransaction>output(TopologyHarness.TOPICS.enrichedTransactions());

            harness.pipe(CdcRecords.p2p(SENDER, RECIPIENT, "100.00").build());
            harness.pipe(CdcRecords.topUp(RECIPIENT, "200.00").build());

            assertThat(enriched.readValuesToList())
                    .extracting(
                            EnrichedTransaction::getAccountId,
                            transaction ->
                                    transaction.getCounterpartyAccountId().orElse(null))
                    .containsExactly(Tuple.tuple(SENDER, RECIPIENT), Tuple.tuple(RECIPIENT, null));
        }
    }

    @Test
    void keepsTransactionsWhoseAccountIsNotYetKnown() {
        try (var harness = TopologyHarness.withLatestAccounts()) {
            var enriched = harness.<EnrichedTransaction>output(TopologyHarness.TOPICS.enrichedTransactions());

            harness.pipe(CdcRecords.p2p(SENDER, RECIPIENT, "100.00").build());

            var value = enriched.readValue();
            assertThat(value.getKycLevel()).isEmpty();
            assertThat(value.getAccountAgeSeconds()).isEmpty();
        }
    }

    @Test
    void versionedAccountsJoinTheAccountAsItWasWhenTheTransactionHappened() {
        try (var harness = TopologyHarness.withVersionedAccounts()) {
            assertThat(kycLevelSeenByLateTransaction(harness)).isEqualTo(KycLevel.BASIC);
        }
    }

    @Test
    void latestAccountsJoinWhateverTheAccountLooksLikeNow() {
        try (var harness = TopologyHarness.withLatestAccounts()) {
            assertThat(kycLevelSeenByLateTransaction(harness)).isEqualTo(KycLevel.FULL);
        }
    }

    /** BASIC at 08:00, upgraded to FULL at 10:00, then a transaction stamped 09:00 arrives. */
    private static KycLevel kycLevelSeenByLateTransaction(TopologyHarness harness) {
        var enriched = harness.<EnrichedTransaction>output(TopologyHarness.TOPICS.enrichedTransactions());
        harness.pipe(CdcRecords.account(SENDER, "BASIC").build());
        harness.pipe(CdcRecords.accountVersion(SENDER, "FULL", CdcRecords.OPENED_AT.plus(Duration.ofHours(2)))
                .setVersion(1)
                .build());

        harness.pipe(CdcRecords.p2p(SENDER, RECIPIENT, "100.00").build());

        return enriched.readValue().getKycLevel().orElseThrow();
    }
}
