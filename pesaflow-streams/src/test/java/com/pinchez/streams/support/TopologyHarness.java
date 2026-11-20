package com.pinchez.streams.support;

import com.pinchez.cdc.Account;
import com.pinchez.cdc.Merchant;
import com.pinchez.cdc.Transaction;
import com.pinchez.streams.config.StreamsProperties;
import com.pinchez.streams.handler.BestEffortProcessingExceptionHandler;
import com.pinchez.streams.handler.CdcDeserializationExceptionHandler;
import com.pinchez.streams.serde.AvroSerdeFactory;
import com.pinchez.streams.topology.PesaflowTopology;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Properties;
import java.util.UUID;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

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
            "transaction-senders",
            "fraud-alerts",
            "fraud-review",
            "dormant-accounts",
            "streams-dlq");

    public static final ZoneId NAIROBI = ZoneId.of("Africa/Nairobi");

    public static final StreamsProperties.Fraud FRAUD = new StreamsProperties.Fraud(
            NAIROBI,
            new StreamsProperties.Fraud.TransferVelocity(Duration.ofMinutes(1), Duration.ofSeconds(30), 5),
            new StreamsProperties.Fraud.RollingSpend(
                    Duration.ofMinutes(10), Duration.ofMinutes(1), new BigDecimal("0.5")),
            new StreamsProperties.Fraud.NewAccount(Duration.ofHours(24), new BigDecimal("10000")),
            new StreamsProperties.Fraud.NightBetting(
                    LocalTime.MIDNIGHT, LocalTime.of(5, 0), Duration.ofMinutes(10), Duration.ofMinutes(2), 3),
            new StreamsProperties.Fraud.RoundTrip(Duration.ofMinutes(5), Duration.ofMinutes(1), new BigDecimal("0.1")),
            new StreamsProperties.Fraud.UnusualAmount(10, new BigDecimal("10"), new BigDecimal("20")));

    /** Wall-clock time starts on the test data's day, so wall-clock punctuations don't depend on today's date. */
    public static final Instant WALL_CLOCK_START = CdcRecords.OPENED_AT;

    public static final Duration DEDUPLICATION_RETENTION = Duration.ofDays(7);
    public static final Duration DORMANCY = Duration.ofDays(30);

    private final String registryScope = "topology-harness-" + UUID.randomUUID();
    private final AvroSerdeFactory serdes = new AvroSerdeFactory("mock://" + registryScope);
    private final TopologyTestDriver driver;
    private final TestInputTopic<String, Account> accounts;
    private final TestInputTopic<String, Merchant> merchants;
    private final TestInputTopic<String, Transaction> transactions;

    private TopologyHarness(boolean versionedAccounts, PunctuationType dormancyScan) {
        var properties = new StreamsProperties(
                "mock://" + registryScope,
                TOPICS,
                new StreamsProperties.OutputTopics(1, (short) 1),
                new StreamsProperties.AccountsTable(versionedAccounts, Duration.ofDays(30)),
                new StreamsProperties.Activity(Duration.ofMinutes(1), Duration.ofSeconds(30), Duration.ofHours(1)),
                new StreamsProperties.Deduplication(DEDUPLICATION_RETENTION),
                new StreamsProperties.DormantAccount(DORMANCY, Duration.ofMinutes(1), dormancyScan),
                new StreamsProperties.Errors(3, Duration.ofMinutes(10)),
                FRAUD);
        var config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-harness");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        config.put(StreamsConfig.ENSURE_EXPLICIT_INTERNAL_RESOURCE_NAMING_CONFIG, true);
        config.put(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, TOPICS.deadLetters());
        config.put(
                StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, CdcDeserializationExceptionHandler.class);
        config.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, BestEffortProcessingExceptionHandler.class);

        var builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(config)));
        var pesaflow = new PesaflowTopology(properties, serdes);
        pesaflow.addTo(builder);
        var topology = builder.build(config);
        pesaflow.addTo(topology);

        driver = new TopologyTestDriver(topology, config, WALL_CLOCK_START);

        accounts = input(TOPICS.cdcAccounts());
        merchants = input(TOPICS.cdcMerchants());
        transactions = input(TOPICS.cdcTransactions());
    }

    public static TopologyHarness withVersionedAccounts() {
        return new TopologyHarness(true, PunctuationType.STREAM_TIME);
    }

    public static TopologyHarness withLatestAccounts() {
        return new TopologyHarness(false, PunctuationType.STREAM_TIME);
    }

    public static TopologyHarness withDormancyScan(PunctuationType punctuationType) {
        return new TopologyHarness(false, punctuationType);
    }

    /** Event time comes from the row's updated_at (see CdcTimestampExtractor). */
    public Account pipe(Account account) {
        accounts.pipeInput(account.getId().toString(), account, Instant.parse(account.getUpdatedAt()));
        return account;
    }

    public void deleteAccount(UUID accountId, Instant timestamp) {
        accounts.pipeInput(accountId.toString(), null, timestamp);
    }

    public Merchant pipe(Merchant merchant) {
        merchants.pipeInput(merchant.getId().toString(), merchant, CdcRecords.OPENED_AT);
        return merchant;
    }

    /** Event time comes from the row's created_at (see CdcTimestampExtractor). */
    public Transaction pipe(Transaction transaction) {
        transactions.pipeInput(transaction.getId().toString(), transaction, Instant.parse(transaction.getCreatedAt()));
        return transaction;
    }

    /** Bytes that are not Avro, as a broken producer might write them. */
    public void pipeRawTransaction(String key, byte[] value) {
        driver.createInputTopic(TOPICS.cdcTransactions(), new StringSerializer(), new ByteArraySerializer())
                .pipeInput(key, value, CdcRecords.OPENED_AT);
    }

    public TestOutputTopic<byte[], byte[]> deadLetters() {
        return driver.createOutputTopic(TOPICS.deadLetters(), new ByteArrayDeserializer(), new ByteArrayDeserializer());
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

    public <V> WindowStore<String, V> windowStore(String name) {
        return driver.getWindowStore(name);
    }

    public void advanceWallClock(Duration duration) {
        driver.advanceWallClockTime(duration);
    }

    public double droppedRecords() {
        return metricTotal("dropped-records-total");
    }

    public double metricTotal(String name) {
        return driver.metrics().entrySet().stream()
                .filter(metric -> metric.getKey().name().equals(name))
                .mapToDouble(metric -> ((Number) metric.getValue().metricValue()).doubleValue())
                .sum();
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
