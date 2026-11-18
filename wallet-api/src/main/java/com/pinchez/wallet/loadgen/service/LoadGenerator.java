package com.pinchez.wallet.loadgen.service;

import com.pinchez.common.exception.DuplicateResourceException;
import com.pinchez.wallet.account.dto.OpenAccountRequest;
import com.pinchez.wallet.account.service.AccountService;
import com.pinchez.wallet.loadgen.config.LoadGeneratorProperties;
import com.pinchez.wallet.loadgen.dto.LoadGeneratorStatus;
import com.pinchez.wallet.merchant.dto.RegisterMerchantRequest;
import com.pinchez.wallet.merchant.service.MerchantService;
import com.pinchez.wallet.transaction.dto.TransactionRequest;
import com.pinchez.wallet.transaction.dto.TransactionRequest.MerchantPayment;
import com.pinchez.wallet.transaction.dto.TransactionRequest.P2pTransfer;
import com.pinchez.wallet.transaction.dto.TransactionRequest.TopUp;
import com.pinchez.wallet.transaction.enums.TransactionStatus;
import com.pinchez.wallet.transaction.enums.TransactionType;
import com.pinchez.wallet.transaction.service.TransactionService;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.random.RandomGenerator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class LoadGenerator {

    private final LoadGeneratorProperties properties;
    private final TaskScheduler scheduler;
    private final AccountService accountService;
    private final MerchantService merchantService;
    private final TransactionService transactionService;

    private final ReentrantLock tickLock = new ReentrantLock();

    private final List<UUID> accountIds = new ArrayList<>();
    private final List<String> tillNumbers = new ArrayList<>();

    private final LongAdder accountsOpened = new LongAdder();
    private final LongAdder merchantsRegistered = new LongAdder();
    private final EnumMap<TransactionType, LongAdder> submitted = new EnumMap<>(TransactionType.class);
    private final LongAdder declined = new LongAdder();
    private final LongAdder errors = new LongAdder();

    private @Nullable ScheduledFuture<?> task;

    {
        for (var type : TransactionType.values()) {
            submitted.put(type, new LongAdder());
        }
    }

    @EventListener(ApplicationReadyEvent.class)
    void startIfEnabled() {
        if (properties.enabled()) {
            start();
        }
    }

    public synchronized LoadGeneratorStatus start() {
        if (task == null) {
            log.info(
                    "Starting load generator: {} transaction(s) every {}",
                    properties.transactionsPerTick(),
                    properties.interval());
            task = scheduler.scheduleWithFixedDelay(this::tick, properties.interval());
        }
        return status();
    }

    public synchronized LoadGeneratorStatus stop() {
        if (task != null) {
            log.info("Stopping load generator");
            task.cancel(false);
            task = null;
        }
        return status();
    }

    public synchronized LoadGeneratorStatus status() {
        var submittedCounts = new EnumMap<TransactionType, Long>(TransactionType.class);
        submitted.forEach((type, count) -> submittedCounts.put(type, count.sum()));
        return new LoadGeneratorStatus(
                task != null,
                properties.interval(),
                properties.transactionsPerTick(),
                accountsOpened.sum(),
                merchantsRegistered.sum(),
                submittedCounts,
                declined.sum(),
                errors.sum());
    }

    private void tick() {
        if (!tickLock.tryLock()) {
            return;
        }
        try {
            var random = ThreadLocalRandom.current();
            ensureSeeded(random);
            if (random.nextDouble() < properties.newAccountProbability()) {
                openAccount(random);
            }
            for (int i = 0; i < properties.transactionsPerTick(); i++) {
                submit(nextRequest(random));
            }
        } catch (RuntimeException e) {
            // An escaping exception would silently cancel the scheduled task.
            errors.increment();
            log.warn("Load generator tick failed", e);
        } finally {
            tickLock.unlock();
        }
    }

    private void ensureSeeded(RandomGenerator random) {
        if (!accountIds.isEmpty()) {
            return;
        }
        accountIds.addAll(accountService.allIds());
        tillNumbers.addAll(merchantService.allTillNumbers());
        while (accountIds.size() < properties.seedAccounts()) {
            openAccount(random);
        }
        while (tillNumbers.size() < properties.seedMerchants()) {
            registerMerchant(random);
        }
        log.info("Load generator ready with {} accounts and {} merchants", accountIds.size(), tillNumbers.size());
    }

    private TransactionRequest nextRequest(RandomGenerator random) {
        return switch (nextType(random)) {
            case P2P -> {
                var from = FakeData.pick(accountIds, random);
                var to = otherAccountThan(from, random);
                yield new P2pTransfer(from, to, FakeData.amount(random, 400, 1.0));
            }
            case MERCHANT_PAYMENT ->
                new MerchantPayment(
                        FakeData.pick(accountIds, random),
                        FakeData.pick(tillNumbers, random),
                        FakeData.amount(random, 300, 0.9));
            case TOPUP ->
                new TopUp(
                        FakeData.pick(accountIds, random),
                        FakeData.agentNumber(random),
                        FakeData.amount(random, 1_000, 0.7));
        };
    }

    private TransactionType nextType(RandomGenerator random) {
        var mix = properties.mix();
        var roll = random.nextInt(Math.max(mix.total(), 1));
        if (roll < mix.p2p()) {
            return TransactionType.P2P;
        }
        return roll < mix.p2p() + mix.merchantPayment() ? TransactionType.MERCHANT_PAYMENT : TransactionType.TOPUP;
    }

    private UUID otherAccountThan(UUID accountId, RandomGenerator random) {
        UUID other;
        do {
            other = FakeData.pick(accountIds, random);
        } while (other.equals(accountId));
        return other;
    }

    private void submit(TransactionRequest request) {
        var response = transactionService.submit(request);
        submitted.get(response.type()).increment();
        if (response.status() == TransactionStatus.FAILED) {
            declined.increment();
        }
    }

    private void openAccount(RandomGenerator random) {
        try {
            var account = accountService.open(
                    new OpenAccountRequest(FakeData.phoneNumber(random), FakeData.fullName(random), null));
            accountIds.add(account.id());
            accountsOpened.increment();
        } catch (DuplicateResourceException _) {
        }
    }

    private void registerMerchant(RandomGenerator random) {
        try {
            var category = FakeData.merchantCategory(random);
            var merchant = merchantService.register(new RegisterMerchantRequest(
                    FakeData.tillNumber(random), FakeData.merchantName(category, random), category));
            tillNumbers.add(merchant.tillNumber());
            merchantsRegistered.increment();
        } catch (DuplicateResourceException _) {
        }
    }
}
