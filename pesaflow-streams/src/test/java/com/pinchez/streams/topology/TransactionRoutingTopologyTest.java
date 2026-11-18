package com.pinchez.streams.topology;

import static org.assertj.core.api.Assertions.assertThat;

import com.pinchez.cdc.Transaction;
import com.pinchez.events.CompletedTransaction;
import com.pinchez.events.TransactionType;
import com.pinchez.streams.config.StreamsProperties;
import com.pinchez.streams.serde.AvroSerdeFactory;
import com.pinchez.streams.support.CdcTransactions;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TransactionRoutingTopologyTest {

    private static final String REGISTRY_SCOPE = "transaction-routing-topology-test";
    private static final StreamsProperties.Topics TOPICS =
            new StreamsProperties.Topics("cdc.transactions", "p2p", "top-ups", "merchant-payments");

    private TopologyTestDriver driver;
    private TestInputTopic<String, Transaction> cdcTransactions;
    private TestOutputTopic<String, CompletedTransaction> p2pTransfers;
    private TestOutputTopic<String, CompletedTransaction> topUps;
    private TestOutputTopic<String, CompletedTransaction> merchantPayments;

    @BeforeEach
    void setUp() {
        var serdes = new AvroSerdeFactory("mock://" + REGISTRY_SCOPE);
        var cdcSerde = serdes.<Transaction>valueSerde();
        var eventSerde = serdes.<CompletedTransaction>valueSerde();

        var builder = new StreamsBuilder();
        new TransactionRoutingTopology(TOPICS, cdcSerde, eventSerde).addTo(builder);

        var config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "transaction-routing-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        driver = new TopologyTestDriver(builder.build(), config);

        cdcTransactions =
                driver.createInputTopic(TOPICS.cdcTransactions(), new StringSerializer(), cdcSerde.serializer());
        p2pTransfers =
                driver.createOutputTopic(TOPICS.p2pTransfers(), new StringDeserializer(), eventSerde.deserializer());
        topUps = driver.createOutputTopic(TOPICS.topUps(), new StringDeserializer(), eventSerde.deserializer());
        merchantPayments = driver.createOutputTopic(
                TOPICS.merchantPayments(), new StringDeserializer(), eventSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
        MockSchemaRegistry.dropScope(REGISTRY_SCOPE);
    }

    @Test
    void routesEachCompletedTransactionToTheTopicForItsType() {
        var p2p = pipe(CdcTransactions.completed("P2P").build());
        var topUp = pipe(CdcTransactions.completed("TOPUP").build());
        var payment = pipe(CdcTransactions.completed("MERCHANT_PAYMENT").build());

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
        pipe(CdcTransactions.failed("P2P", "DAILY_LIMIT_EXCEEDED").build());
        pipe(CdcTransactions.failed("TOPUP", "RECIPIENT_FROZEN").build());
        pipe(CdcTransactions.failed("MERCHANT_PAYMENT", "SENDER_FROZEN").build());

        assertThat(p2pTransfers.isEmpty()).isTrue();
        assertThat(topUps.isEmpty()).isTrue();
        assertThat(merchantPayments.isEmpty()).isTrue();
    }

    @Test
    void mapsTheCdcRowToACleanEvent() {
        var row = pipe(CdcTransactions.completed("MERCHANT_PAYMENT").build());

        var event = merchantPayments.readValue();

        assertThat(event.getTransactionId()).isEqualTo(row.getId());
        assertThat(event.getType()).isEqualTo(TransactionType.MERCHANT_PAYMENT);
        assertThat(event.getReference()).isEqualTo("TKL8XQ2M9A");
        assertThat(event.getFromAccountId()).isEqualTo(row.getFromAccountId());
        assertThat(event.getToAccountId()).isEmpty();
        assertThat(event.getMerchantId()).isEqualTo(row.getMerchantId());
        assertThat(event.getAmount()).isEqualByComparingTo(new BigDecimal("350.50"));
        assertThat(event.getCurrency()).isEqualTo("KES");
        assertThat(event.getOccurredAt()).isEqualTo(Instant.parse("2026-11-18T09:10:31.829078Z"));
    }

    @Test
    void ignoresTombstones() {
        cdcTransactions.pipeInput("some-id", null);

        assertThat(p2pTransfers.isEmpty()).isTrue();
        assertThat(topUps.isEmpty()).isTrue();
        assertThat(merchantPayments.isEmpty()).isTrue();
    }

    private Transaction pipe(Transaction row) {
        cdcTransactions.pipeInput(row.getId().toString(), row);
        return row;
    }
}
