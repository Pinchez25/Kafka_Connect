package com.pinchez.wallet.loadgen.service;

import com.pinchez.common.exception.DuplicateResourceException;
import com.pinchez.wallet.account.dto.OpenAccountRequest;
import com.pinchez.wallet.account.service.AccountService;
import com.pinchez.wallet.loadgen.dto.ScenarioResult;
import com.pinchez.wallet.loadgen.enums.FraudScenario;
import com.pinchez.wallet.merchant.dto.RegisterMerchantRequest;
import com.pinchez.wallet.merchant.enums.MerchantCategory;
import com.pinchez.wallet.merchant.service.MerchantService;
import com.pinchez.wallet.transaction.dto.TransactionRequest.MerchantPayment;
import com.pinchez.wallet.transaction.dto.TransactionRequest.P2pTransfer;
import com.pinchez.wallet.transaction.dto.TransactionRequest.TopUp;
import com.pinchez.wallet.transaction.service.TransactionService;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

/** Injects traffic shaped to trip one fraud rule each, using fresh accounts so results are easy to find. */
@Service
@RequiredArgsConstructor
public class FraudScenarioService {

    private static final Duration HABIT_SETTLE_TIME = Duration.ofSeconds(2);

    private final AccountService accountService;
    private final MerchantService merchantService;
    private final TransactionService transactionService;
    private final Clock clock;

    public ScenarioResult run(FraudScenario scenario) {
        return switch (scenario) {
            case VELOCITY_BURST -> velocityBurst();
            case ROLLING_SPEND -> rollingSpend();
            case NEW_ACCOUNT_LARGE_TRANSFER -> newAccountLargeTransfer();
            case NIGHT_BETTING_BURST -> nightBettingBurst();
            case ROUND_TRIP -> roundTrip();
            case LATE_EVENTS -> lateEvents();
            case UNUSUAL_AMOUNT -> unusualAmount();
            case DORMANT_ACCOUNT -> dormantAccount();
        };
    }

    private ScenarioResult velocityBurst() {
        var sender = openAccount();
        var recipient = openAccount();
        var sent = repeat(8, () -> transfer(sender, recipient, "100"));
        return new ScenarioResult(
                FraudScenario.VELOCITY_BURST,
                List.of(sender, recipient),
                sent,
                "TRANSFER_VELOCITY for the sender once its 1-minute window plus grace has passed in stream time");
    }

    private ScenarioResult rollingSpend() {
        var sender = openAccount();
        var recipient = openAccount();
        var sent = repeat(4, () -> transfer(sender, recipient, "9000"));
        return new ScenarioResult(
                FraudScenario.ROLLING_SPEND,
                List.of(sender, recipient),
                sent,
                "ROLLING_SPEND for the sender (36,000 sent against a 70,000 daily limit) after the sliding window closes");
    }

    private ScenarioResult newAccountLargeTransfer() {
        var sender = openAccount();
        var recipient = openAccount();
        return new ScenarioResult(
                FraudScenario.NEW_ACCOUNT_LARGE_TRANSFER,
                List.of(sender, recipient),
                List.of(transfer(sender, recipient, "15000")),
                "NEW_ACCOUNT_LARGE_TRANSFER for the sender straight away (no window involved)");
    }

    private ScenarioResult nightBettingBurst() {
        var gambler = openAccount();
        var tillNumber = till(MerchantCategory.BETTING);
        var paid = repeat(
                4,
                () -> transactionService
                        .submit(new MerchantPayment(gambler, tillNumber, new BigDecimal("500")))
                        .id());
        return new ScenarioResult(
                FraudScenario.NIGHT_BETTING_BURST,
                List.of(gambler),
                paid,
                "NIGHT_BETTING_BURST only if 'now' falls inside pesaflow.streams.fraud.night-betting, once the session"
                        + " has been quiet for the inactivity gap plus grace");
    }

    private ScenarioResult roundTrip() {
        var alice = openAccount();
        var bob = openAccount();
        return new ScenarioResult(
                FraudScenario.ROUND_TRIP,
                List.of(alice, bob),
                List.of(transfer(alice, bob, "2000"), transfer(bob, alice, "1950")),
                "ROUND_TRIP_TRANSFER for the first account as soon as the return leg arrives");
    }

    private ScenarioResult lateEvents() {
        var sender = openAccount();
        var recipient = openAccount();
        var now = clock.instant();
        var sent = new ArrayList<UUID>();
        sent.addAll(repeat(3, () -> transferAt(sender, recipient, now.minus(Duration.ofMinutes(10)))));
        sent.addAll(repeat(3, () -> transferAt(sender, recipient, now.minus(Duration.ofSeconds(20)))));
        return new ScenarioResult(
                FraudScenario.LATE_EVENTS,
                List.of(sender, recipient),
                sent,
                "The first 3 are 10 minutes late (past window + grace): windowed aggregations drop them and"
                        + " dropped-records-total rises. The last 3 are 20 seconds late and still counted.");
    }

    private ScenarioResult unusualAmount() {
        var tillNumber = till(MerchantCategory.GROCERY);
        var flagged = openAccount();
        var queued = openAccount();
        var sent = new ArrayList<UUID>();
        for (var account : List.of(flagged, queued)) {
            sent.addAll(repeat(10, () -> pay(account, tillNumber, "100")));
        }
        letHabitArriveFirst();
        sent.add(pay(flagged, tillNumber, "2500"));
        sent.add(pay(queued, tillNumber, "1500"));
        return new ScenarioResult(
                FraudScenario.UNUSUAL_AMOUNT,
                List.of(flagged, queued),
                sent,
                "Both accounts make 10 payments of 100. UNUSUAL_AMOUNT in pesaflow.fraud.alerts for the first (2,500 is"
                        + " 25x its average) and in pesaflow.fraud.review for the second (1,500 is 15x)");
    }

    private ScenarioResult dormantAccount() {
        var account = openAccount();
        var agentNumber = FakeData.agentNumber(ThreadLocalRandom.current());
        var lastSeen = clock.instant().minus(Duration.ofDays(40));
        var topUp = transactionService
                .submitAt(new TopUp(account, agentNumber, new BigDecimal("2500")), lastSeen)
                .id();
        return new ScenarioResult(
                FraudScenario.DORMANT_ACCOUNT,
                List.of(account),
                List.of(topUp),
                "A top-up stamped 40 days ago and nothing since: a DormantAccount record in pesaflow.accounts.dormant"
                        + " at the next dormancy scan");
    }

    /**
     * CDC records are keyed by transaction id, so one account's payments sit on different partitions and can be
     * reordered once Kafka Streams re-keys them by account. The pause lets the habit reach the baseline first.
     */
    private static void letHabitArriveFirst() {
        try {
            Thread.sleep(HABIT_SETTLE_TIME);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private UUID pay(UUID from, String tillNumber, String amount) {
        return transactionService
                .submit(new MerchantPayment(from, tillNumber, new BigDecimal(amount)))
                .id();
    }

    private UUID transfer(UUID from, UUID to, String amount) {
        return transactionService
                .submit(new P2pTransfer(from, to, new BigDecimal(amount)))
                .id();
    }

    private UUID transferAt(UUID from, UUID to, Instant occurredAt) {
        return transactionService
                .submitAt(new P2pTransfer(from, to, new BigDecimal("100")), occurredAt)
                .id();
    }

    private UUID openAccount() {
        var random = ThreadLocalRandom.current();
        while (true) {
            try {
                return accountService
                        .open(new OpenAccountRequest(FakeData.phoneNumber(random), FakeData.fullName(random), null))
                        .id();
            } catch (DuplicateResourceException _) {
            }
        }
    }

    private String till(MerchantCategory category) {
        var random = ThreadLocalRandom.current();
        while (true) {
            var existing = merchantService.anyInCategory(category);
            if (existing.isPresent()) {
                return existing.get().tillNumber();
            }
            try {
                return merchantService
                        .register(new RegisterMerchantRequest(
                                FakeData.tillNumber(random), FakeData.merchantName(category, random), category))
                        .tillNumber();
            } catch (DuplicateResourceException _) {
            }
        }
    }

    private static List<UUID> repeat(int times, Supplier<UUID> action) {
        var ids = new ArrayList<UUID>(times);
        for (int i = 0; i < times; i++) {
            ids.add(action.get());
        }
        return ids;
    }
}
