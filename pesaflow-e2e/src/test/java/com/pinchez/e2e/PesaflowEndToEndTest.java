package com.pinchez.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.awaitility.Awaitility;
import org.awaitility.core.ThrowingRunnable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.node.ObjectNode;

/**
 * The whole loop, with nothing mocked: a transaction in the wallet's Postgres is captured by Debezium, processed by
 * Kafka Streams into a balance and a fraud alert, and the alert is posted back to the wallet by the HTTP sink, which
 * freezes the account; the freeze then flows back through CDC.
 */
class PesaflowEndToEndTest {

    private static final Path ROOT = Path.of(System.getProperty("pesaflow.e2e.root"));
    private static final Path LOGS = Path.of(System.getProperty("pesaflow.e2e.logs"));
    private static final Duration PIPELINE_TIMEOUT = Duration.ofMinutes(2);

    private static final Network NETWORK = Network.newNetwork();

    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine")
            .withNetwork(NETWORK)
            .withNetworkAliases("postgres")
            .withDatabaseName("pesaflow")
            .withUsername("pesaflow")
            .withPassword("pesaflow")
            .withCommand("postgres", "-c", "wal_level=logical");

    private static final KafkaContainer KAFKA =
            new KafkaContainer("apache/kafka:4.3.1").withNetwork(NETWORK).withListener("kafka:19092");

    private static final GenericContainer<?> SCHEMA_REGISTRY = new GenericContainer<>(
                    "confluentinc/cp-schema-registry:8.2.0")
            .withNetwork(NETWORK)
            .withNetworkAliases("schema-registry")
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:19092")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR", "1")
            .withExposedPorts(8081)
            .dependsOn(KAFKA)
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));

    private static final GenericContainer<?> CONNECT = new GenericContainer<>(
                    new ImageFromDockerfile("pesaflow/connect-e2e", false)
                            .withFileFromPath(".", ROOT.resolve("connect")))
            .withNetwork(NETWORK)
            .withEnv(Map.ofEntries(
                    Map.entry("BOOTSTRAP_SERVERS", "kafka:19092"),
                    Map.entry("GROUP_ID", "pesaflow-connect-e2e"),
                    Map.entry("CONFIG_STORAGE_TOPIC", "connect-configs"),
                    Map.entry("OFFSET_STORAGE_TOPIC", "connect-offsets"),
                    Map.entry("STATUS_STORAGE_TOPIC", "connect-status"),
                    Map.entry("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1"),
                    Map.entry("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1"),
                    Map.entry("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1"),
                    Map.entry("KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter"),
                    Map.entry("VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter"),
                    Map.entry("CONNECT_EXACTLY_ONCE_SOURCE_SUPPORT", "enabled"),
                    Map.entry("CONNECT_CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY", "All"),
                    Map.entry("CONNECT_CONFIG_PROVIDERS", "env"),
                    Map.entry(
                            "CONNECT_CONFIG_PROVIDERS_ENV_CLASS",
                            "org.apache.kafka.common.config.provider.EnvVarConfigProvider"),
                    Map.entry("PESAFLOW_DB_PASSWORD", "pesaflow")))
            .withExposedPorts(8083)
            .dependsOn(KAFKA, SCHEMA_REGISTRY, POSTGRES)
            .waitingFor(Wait.forHttp("/connectors").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(5)));

    private static final Http HTTP = new Http();
    private static int walletPort;
    private static int streamsPort;
    private static AppProcess wallet;
    private static AppProcess streams;

    @BeforeAll
    static void startEverything() throws Exception {
        walletPort = freePort();
        streamsPort = freePort();
        Testcontainers.exposeHostPorts(walletPort);
        Startables.deepStart(POSTGRES, KAFKA, SCHEMA_REGISTRY, CONNECT).join();

        wallet = AppProcess.start(
                "wallet-api",
                Path.of(System.getProperty("pesaflow.e2e.wallet-jar")),
                Map.of(
                        "SERVER_PORT", String.valueOf(walletPort),
                        "DB_URL", POSTGRES.getJdbcUrl(),
                        "DB_USERNAME", "pesaflow",
                        "DB_PASSWORD", "pesaflow",
                        "LOAD_GENERATOR_ENABLED", "false"),
                LOGS);
        wallet.awaitHealthy(HTTP, walletPort, Duration.ofMinutes(2));

        createCdcTopics();
        applyConnector("pesaflow-postgres-source", config -> {});
        applyConnector(
                "pesaflow-fraud-webhook",
                config -> config.put(
                                "http.url",
                                "http://host.testcontainers.internal:%d/webhooks/fraud-alerts".formatted(walletPort))
                        .put("consumer.override.auto.offset.reset", "earliest"));

        streams = AppProcess.start(
                "pesaflow-streams",
                Path.of(System.getProperty("pesaflow.e2e.streams-jar")),
                Map.of(
                        "SERVER_PORT", String.valueOf(streamsPort),
                        "KAFKA_BOOTSTRAP_SERVERS", KAFKA.getBootstrapServers(),
                        "SCHEMA_REGISTRY_URL", schemaRegistryUrl(),
                        "STREAMS_STATE_DIR",
                                Files.createTempDirectory("pesaflow-e2e-state").toString(),
                        "STREAMS_STANDBY_REPLICAS", "0"),
                LOGS);
        streams.awaitHealthy(HTTP, streamsPort, Duration.ofMinutes(2));

        // Only once Streams has created pesaflow.fraud.alerts: subscribing earlier lets the broker auto-create it with
        // one partition, and the sink would not see the partitions added later until its next metadata refresh.
        applyConnector(
                "pesaflow-fraud-webhook",
                config -> config.put(
                                "http.url",
                                "http://host.testcontainers.internal:%d/webhooks/fraud-alerts".formatted(walletPort))
                        .put("consumer.override.auto.offset.reset", "earliest"));
    }

    @AfterAll
    static void stopEverything() throws InterruptedException, IOException {
        if (CONNECT.isRunning()) {
            Files.writeString(LOGS.resolve("connect.log"), CONNECT.getLogs());
        }
        for (var app : Stream.of(streams, wallet).filter(Objects::nonNull).toList()) {
            app.close();
        }
    }

    @Test
    void aLargeTransferFromANewAccountIsCountedAlertedAndFrozenAllTheWayRound() throws Exception {
        var scenario = HTTP.post(walletPort, "/load-generator/scenarios/new-account-large-transfer");
        var sender = scenario.path("accountIds").get(0).asString();
        var recipient = scenario.path("accountIds").get(1).asString();

        await(
                "the recipient's balance in Kafka Streams",
                () -> assertThat(HTTP.get(streamsPort, "/balances/" + recipient)
                                .path("balance")
                                .decimalValue())
                        .isEqualByComparingTo("15000.00"));

        await(
                "the webhook to freeze the sender in the wallet",
                () -> assertThat(HTTP.get(walletPort, "/accounts/" + sender)
                                .path("status")
                                .asString())
                        .isEqualTo("FROZEN"));

        await(
                "the freeze to flow back through CDC into Kafka Streams",
                () -> assertThat(HTTP.get(streamsPort, "/accounts/" + sender + "/summary")
                                .path("status")
                                .asString())
                        .isEqualTo("FROZEN"));
    }

    private static void await(String what, ThrowingRunnable assertion) {
        Awaitility.await(what)
                .atMost(PIPELINE_TIMEOUT)
                .pollInterval(Duration.ofSeconds(1))
                .ignoreExceptions()
                .untilAsserted(assertion);
    }

    /** Debezium only creates a topic on its first record, but Kafka Streams needs its source topics to exist. */
    private static void createCdcTopics() throws Exception {
        var config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (var admin = Admin.create(config)) {
            var compacted = Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
            admin.createTopics(List.of(
                            new NewTopic("pesaflow.transactions", 3, (short) 1),
                            new NewTopic("pesaflow.accounts", 3, (short) 1).configs(compacted),
                            new NewTopic("pesaflow.merchants", 3, (short) 1).configs(compacted)))
                    .all()
                    .get();
        }
    }

    private static void applyConnector(String name, Consumer<ObjectNode> customise)
            throws IOException, InterruptedException {
        var config = (ObjectNode) HTTP.read(Files.readString(ROOT.resolve("connect/connectors/" + name + ".json")));
        customise.accept(config);
        var url = "http://%s:%d/connectors/%s/config".formatted(CONNECT.getHost(), CONNECT.getMappedPort(8083), name);
        HTTP.put(url, config.toString());
        Awaitility.await(name + " running")
                .atMost(Duration.ofMinutes(1))
                .ignoreExceptions()
                .until(() -> "RUNNING"
                        .equals(HTTP.get(url.replace("/config", "/status"))
                                .path("tasks")
                                .get(0)
                                .path("state")
                                .asString()));
    }

    private static String schemaRegistryUrl() {
        return "http://%s:%d".formatted(SCHEMA_REGISTRY.getHost(), SCHEMA_REGISTRY.getMappedPort(8081));
    }

    private static int freePort() throws IOException {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
