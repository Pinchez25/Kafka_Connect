package com.pinchez.streams.support;

import com.pinchez.cdc.Account;
import com.pinchez.cdc.Merchant;
import com.pinchez.cdc.Transaction;
import com.pinchez.streams.config.StreamsProperties;
import com.pinchez.streams.serde.AvroSerdeFactory;
import com.pinchez.streams.topology.PesaflowTopology;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;

/** Runs the whole PesaflowTopology in a TopologyTestDriver with an in-memory schema registry. */
public final class TopologyHarness implements AutoCloseable {

    public static final StreamsProperties.Topics TOPICS = new StreamsProperties.Topics(
            "cdc.accounts",
            "cdc.merchants",
            "cdc.transactions",
            "p2p",
            "top-ups",
            "merchant-payments",
            "enriched",
            "balances",
            "transaction-senders");

    private final String registryScope = "topology-harness-" + UUID.randomUUID();
    private final AvroSerdeFactory serdes = new AvroSerdeFactory("mock://" + registryScope);
    private final TopologyTestDriver driver;
    private final TestInputTopic<String, Account> accounts;
    private final TestInputTopic<String, Merchant> merchants;
    private final TestInputTopic<String, Transaction> transactions;

    private TopologyHarness(boolean versionedAccounts) {
        var properties = new StreamsProperties(
                "mock://" + registryScope,
                TOPICS,
                new StreamsProperties.OutputTopics(1, (short) 1),
                new StreamsProperties.AccountsTable(versionedAccounts, Duration.ofDays(30)));
        var builder = new StreamsBuilder();
        new PesaflowTopology(properties, serdes).addTo(builder);

        var config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-harness");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        driver = new TopologyTestDriver(builder.build(), config);

        accounts = input(TOPICS.cdcAccounts());
        merchants = input(TOPICS.cdcMerchants());
        transactions = input(TOPICS.cdcTransactions());
    }

    public static TopologyHarness withVersionedAccounts() {
        return new TopologyHarness(true);
    }

    public static TopologyHarness withLatestAccounts() {
        return new TopologyHarness(false);
    }

    public Account pipe(Account account, Instant timestamp) {
        accounts.pipeInput(account.getId().toString(), account, timestamp);
        return account;
    }

    public Account pipe(Account account) {
        return pipe(account, CdcRecords.OPENED_AT);
    }

    public void deleteAccount(UUID accountId, Instant timestamp) {
        accounts.pipeInput(accountId.toString(), null, timestamp);
    }

    public Merchant pipe(Merchant merchant) {
        merchants.pipeInput(merchant.getId().toString(), merchant, CdcRecords.OPENED_AT);
        return merchant;
    }

    public Transaction pipe(Transaction transaction, Instant timestamp) {
        transactions.pipeInput(transaction.getId().toString(), transaction, timestamp);
        return transaction;
    }

    public Transaction pipe(Transaction transaction) {
        return pipe(transaction, Instant.parse(transaction.getCreatedAt()));
    }

    public void pipeTombstone(String transactionKey) {
        transactions.pipeInput(transactionKey, null);
    }

    public <V extends SpecificRecord> TestOutputTopic<String, V> output(String topic) {
        return driver.createOutputTopic(
                topic, new StringDeserializer(), serdes.<V>valueSerde().deserializer());
    }

    public <V> KeyValueStore<String, V> store(String name) {
        return driver.getKeyValueStore(name);
    }

    @Override
    public void close() {
        driver.close();
        MockSchemaRegistry.dropScope(registryScope);
    }

    private <V extends SpecificRecord> TestInputTopic<String, V> input(String topic) {
        return driver.createInputTopic(
                topic, new StringSerializer(), serdes.<V>valueSerde().serializer());
    }
}
