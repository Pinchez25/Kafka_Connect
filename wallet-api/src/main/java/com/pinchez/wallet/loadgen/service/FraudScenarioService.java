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
        var tillNumber = bettingTill();
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

    private String bettingTill() {
        var random = ThreadLocalRandom.current();
        while (true) {
            var existing = merchantService.anyInCategory(MerchantCategory.BETTING);
            if (existing.isPresent()) {
                return existing.get().tillNumber();
            }
            try {
                return merchantService
                        .register(new RegisterMerchantRequest(
                                FakeData.tillNumber(random),
                                FakeData.merchantName(MerchantCategory.BETTING, random),
                                MerchantCategory.BETTING))
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
