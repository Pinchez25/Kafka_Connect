package com.pinchez.streams;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pinchez.events.CompletedTransaction;
import com.pinchez.streams.config.StreamsProperties;
import com.pinchez.streams.support.CdcRecords;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(
        properties = {
            "pesaflow.streams.schema-registry-url=" + StreamsApplicationIntegrationTest.REGISTRY,
            "spring.kafka.streams.state-dir=build/kafka-streams-test/${random.uuid}"
        })
@AutoConfigureMockMvc
@Import(StreamsApplicationIntegrationTest.Infrastructure.class)
class StreamsApplicationIntegrationTest {

    static final String REGISTRY = "mock://streams-application-integration-test";

    @Autowired
    KafkaConnectionDetails kafka;

    @Autowired
    StreamsProperties properties;

    @Autowired
    MockMvcTester mvc;

    @Test
    void routesTransactionsAndServesBalancesFromTheStateStore() {
        var wanjiku = UUID.randomUUID();
        var otieno = UUID.randomUUID();
        var topUp = CdcRecords.topUp(wanjiku, "1000.00").build();
        var transfer = CdcRecords.p2p(wanjiku, otieno, "250.00").build();
        var declined = CdcRecords.p2p(wanjiku, otieno, "999.00")
                .setStatus("FAILED")
                .setFailureReason("SENDER_FROZEN")
                .build();

        var topics = properties.topics();
        send(
                topics.cdcAccounts(),
                wanjiku.toString(),
                CdcRecords.account(wanjiku, "BASIC").build());
        for (var row : List.of(topUp, transfer, declined)) {
            send(topics.cdcTransactions(), row.getId().toString(), row);
        }

        assertThat(consume(topics.p2pTransfers(), 1))
                .extracting(CompletedTransaction::getTransactionId)
                .containsExactly(transfer.getId());
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var balance = mvc.get().uri("/balances/{accountId}", wanjiku);
            assertThat(balance).hasStatus(HttpStatus.OK);
            assertThat(balance).bodyJson().extractingPath("$.balance").isEqualTo(750.0);
        });
        // Downstream of a cached KTable, the join only sees the new balance once the cache flushes on commit.
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(mvc.get().uri("/accounts/{accountId}/summary", wanjiku))
                        .bodyJson()
                        .extractingPath("$.balance")
                        .isEqualTo(750.0));
    }

    @Test
    void unknownAccountIsAProblemDetail404() {
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(mvc.get().uri("/balances/{accountId}", UUID.randomUUID()))
                        .hasStatus(HttpStatus.NOT_FOUND)
                        .bodyJson()
                        .extractingPath("$.type")
                        .isEqualTo("urn:pesaflow:problem:resource-not-found"));
    }

    private void send(String topic, String key, SpecificRecord value) {
        try (var producer = new KafkaProducer<String, SpecificRecord>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                String.join(",", kafka.getBootstrapServers()),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class,
                KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                REGISTRY))) {
            producer.send(new ProducerRecord<>(topic, key, value));
        }
    }

    private List<CompletedTransaction> consume(String topic, int expected) {
        try (var consumer = new KafkaConsumer<String, CompletedTransaction>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                String.join(",", kafka.getBootstrapServers()),
                ConsumerConfig.GROUP_ID_CONFIG,
                "it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                KafkaAvroDeserializer.class,
                KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                REGISTRY,
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,
                true))) {
            consumer.subscribe(List.of(topic));
            var received = new ArrayList<CompletedTransaction>();
            await().atMost(Duration.ofSeconds(60)).until(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(record -> received.add(record.value()));
                return received.size() >= expected;
            });
            return received;
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class Infrastructure {

        @Bean
        @ServiceConnection
        KafkaContainer kafkaContainer() {
            return new KafkaContainer(DockerImageName.parse("apache/kafka-native:4.3.1"));
        }

        @Bean
        KafkaAdmin.NewTopics cdcTopics(StreamsProperties properties) {
            var topics = properties.topics();
            return new KafkaAdmin.NewTopics(
                    List.of(topics.cdcAccounts(), topics.cdcMerchants(), topics.cdcTransactions()).stream()
                            .map(name -> TopicBuilder.name(name)
                                    .partitions(3)
                                    .replicas(1)
                                    .build())
                            .toArray(NewTopic[]::new));
        }
    }
}
