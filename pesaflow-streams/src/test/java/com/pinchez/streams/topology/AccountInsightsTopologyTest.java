package com.pinchez.streams.topology;

import static org.assertj.core.api.Assertions.assertThat;

import com.pinchez.events.AccountProfile;
import com.pinchez.events.AccountStatus;
import com.pinchez.events.AccountSummary;
import com.pinchez.events.EnrichedTransaction;
import com.pinchez.events.MerchantCategory;
import com.pinchez.streams.support.CdcRecords;
import com.pinchez.streams.support.TopologyHarness;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class AccountInsightsTopologyTest {

    private static final UUID ACCOUNT = UUID.randomUUID();
    private static final UUID GROCER = UUID.randomUUID();
    private static final UUID BOOKMAKER = UUID.randomUUID();

    private final TopologyHarness harness = TopologyHarness.withLatestAccounts();

    @AfterEach
    void tearDown() {
        harness.close();
    }

    @Test
    void reduceKeepsTheLargestTransaction() {
        harness.pipe(CdcRecords.topUp(ACCOUNT, "300.00").build());
        var largest = harness.pipe(CdcRecords.topUp(ACCOUNT, "900.00").build());
        harness.pipe(CdcRecords.topUp(ACCOUNT, "100.00").build());

        var stored = harness.<EnrichedTransaction>store(StoreNames.LARGEST_TRANSACTIONS)
                .get(ACCOUNT.toString());

        assertThat(stored.getTransactionId()).isEqualTo(largest.getId());
    }

    @Test
    void cogroupBuildsOneProfileFromThreeKindsOfTransaction() {
        harness.pipe(CdcRecords.merchant(GROCER, "GROCERY"));
        harness.pipe(CdcRecords.merchant(BOOKMAKER, "BETTING"));

        harness.pipe(CdcRecords.topUp(ACCOUNT, "1000.00").build());
        harness.pipe(CdcRecords.p2p(ACCOUNT, UUID.randomUUID(), "200.00").build());
        harness.pipe(CdcRecords.merchantPayment(ACCOUNT, BOOKMAKER, "50.00").build());
        harness.pipe(CdcRecords.merchantPayment(ACCOUNT, BOOKMAKER, "50.00").build());
        harness.pipe(CdcRecords.merchantPayment(ACCOUNT, GROCER, "300.00").build());

        var profile = harness.<AccountProfile>store(StoreNames.ACCOUNT_PROFILES).get(ACCOUNT.toString());

        assertThat(profile.getTotalTopUps()).isEqualByComparingTo("1000.00");
        assertThat(profile.getTotalP2pSent()).isEqualByComparingTo("200.00");
        assertThat(profile.getTotalMerchantSpend()).isEqualByComparingTo("400.00");
        assertThat(profile.getMerchantPaymentsByCategory())
                .containsEntry("BETTING", 2L)
                .containsEntry("GROCERY", 1L);
        assertThat(profile.getFavouriteMerchantCategory()).contains(MerchantCategory.BETTING);
    }

    @Test
    void summaryJoinsAccountStateWithItsBalance() {
        var summaries = harness.<AccountSummary>store(StoreNames.ACCOUNT_SUMMARIES);
        harness.pipe(CdcRecords.account(ACCOUNT, "BASIC").build());

        assertThat(summaries.get(ACCOUNT.toString()).getBalance()).isEqualByComparingTo("0.00");

        harness.pipe(CdcRecords.topUp(ACCOUNT, "750.00").build());
        harness.pipe(CdcRecords.accountVersion(ACCOUNT, "BASIC", CdcRecords.OPENED_AT.plus(Duration.ofHours(2)))
                .setStatus("FROZEN")
                .build());

        var summary = summaries.get(ACCOUNT.toString());
        assertThat(summary.getBalance()).isEqualByComparingTo("750.00");
        assertThat(summary.getStatus()).isEqualTo(AccountStatus.FROZEN);
    }

    @Test
    void deletingTheAccountRemovesItsSummary() {
        harness.pipe(CdcRecords.account(ACCOUNT, "BASIC").build());

        harness.deleteAccount(ACCOUNT, CdcRecords.OPENED_AT.plus(Duration.ofHours(2)));

        assertThat(harness.<AccountSummary>store(StoreNames.ACCOUNT_SUMMARIES).get(ACCOUNT.toString()))
                .isNull();
    }
}
