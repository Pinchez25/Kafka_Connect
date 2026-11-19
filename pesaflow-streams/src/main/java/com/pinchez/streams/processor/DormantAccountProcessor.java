package com.pinchez.streams.processor;

import com.pinchez.events.Balance;
import com.pinchez.events.DormantAccount;
import com.pinchez.events.LastActivity;
import com.pinchez.events.LedgerEntry;
import com.pinchez.streams.config.StreamsProperties;
import com.pinchez.streams.serde.AvroSerdeFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;

/**
 * Remembers each account's latest ledger entry and, on a punctuator, reports accounts that have been quiet for too
 * long. It reads the DSL's {@code balances} store to say how much money is sitting in the dormant account.
 */
public final class DormantAccountProcessor implements Processor<String, LedgerEntry, String, DormantAccount> {

    private final StreamsProperties.DormantAccount settings;
    private final String lastActivityStoreName;
    private final String balancesStoreName;
    private ProcessorContext<String, DormantAccount> context;
    private KeyValueStore<String, LastActivity> lastActivity;
    private TimestampedKeyValueStore<String, Balance> balances;

    private DormantAccountProcessor(
            StreamsProperties.DormantAccount settings, String lastActivityStoreName, String balancesStoreName) {
        this.settings = settings;
        this.lastActivityStoreName = lastActivityStoreName;
        this.balancesStoreName = balancesStoreName;
    }

    @Override
    public void init(ProcessorContext<String, DormantAccount> context) {
        this.context = context;
        this.lastActivity = context.getStateStore(lastActivityStoreName);
        this.balances = context.getStateStore(balancesStoreName);
        context.schedule(settings.scanInterval(), settings.punctuationType(), this::reportDormantAccounts);
    }

    @Override
    public void process(Record<String, LedgerEntry> record) {
        var entry = record.value();
        if (record.key() == null || entry == null) {
            return;
        }
        var current = lastActivity.get(record.key());
        if (current != null && current.getLastActivityAt().isAfter(entry.getOccurredAt())) {
            return;
        }
        lastActivity.put(
                record.key(),
                LastActivity.newBuilder()
                        .setLastTransactionId(entry.getTransactionId())
                        .setLastActivityAt(entry.getOccurredAt())
                        .build());
    }

    /**
     * {@code timestamp} is stream time or wall-clock time, depending on the punctuation type. Store updates are applied
     * after the scan rather than while the iterator is open.
     */
    private void reportDormantAccounts(long timestamp) {
        var now = Instant.ofEpochMilli(timestamp);
        var quietSince = now.minus(settings.inactivity());
        var reported = new ArrayList<KeyValue<String, LastActivity>>();
        try (var accounts = lastActivity.all()) {
            while (accounts.hasNext()) {
                var account = accounts.next();
                var activity = account.value;
                if (activity.getReportedDormantAt().isEmpty()
                        && !activity.getLastActivityAt().isAfter(quietSince)) {
                    context.forward(new Record<>(account.key, dormant(account.key, activity, now), timestamp));
                    reported.add(KeyValue.pair(
                            account.key,
                            LastActivity.newBuilder(activity)
                                    .setReportedDormantAt(now)
                                    .build()));
                }
            }
        }
        reported.forEach(account -> lastActivity.put(account.key, account.value));
    }

    private DormantAccount dormant(String accountId, LastActivity activity, Instant now) {
        var balance = balances.get(accountId);
        return DormantAccount.newBuilder()
                .setAccountId(UUID.fromString(accountId))
                .setLastActivityAt(activity.getLastActivityAt())
                .setDetectedAt(now)
                .setInactiveSeconds(
                        Duration.between(activity.getLastActivityAt(), now).toSeconds())
                .setBalance(balance == null ? null : balance.value().getAmount())
                .build();
    }

    public record Supplier(
            StreamsProperties.DormantAccount settings,
            String lastActivityStoreName,
            String balancesStoreName,
            AvroSerdeFactory serdes)
            implements ProcessorSupplier<String, LedgerEntry, String, DormantAccount> {

        @Override
        public Processor<String, LedgerEntry, String, DormantAccount> get() {
            return new DormantAccountProcessor(settings, lastActivityStoreName, balancesStoreName);
        }

        /** Only the store this processor owns. {@code balances} belongs to the DSL and is connected by name. */
        @Override
        public Set<StoreBuilder<?>> stores() {
            return Set.of(Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore(lastActivityStoreName),
                    Serdes.String(),
                    serdes.<LastActivity>valueSerde()));
        }
    }
}
