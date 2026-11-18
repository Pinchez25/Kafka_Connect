package com.pinchez.streams;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pinchez.cdc.Transaction;
import com.pinchez.events.CompletedTransaction;
import com.pinchez.streams.config.StreamsProperties;
import com.pinchez.streams.support.CdcTransactions;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.TopicBuilder;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(properties = "pesaflow.streams.schema-registry-url=" + StreamsApplicationIntegrationTest.REGISTRY)
@Import(StreamsApplicationIntegrationTest.Infrastructure.class)
class StreamsApplicationIntegrationTest {

    static final String REGISTRY = "mock://streams-application-integration-test";

    @Autowired
    KafkaConnectionDetails kafka;

    @Autowired
    StreamsProperties properties;

    @Test
    void routesCompletedTransactionsFromTheCdcTopic() {
        var transfer = CdcTransactions.completed("P2P").build();
        var declined = CdcTransactions.failed("P2P", "SENDER_FROZEN").build();

        try (var producer = new KafkaProducer<String, Transaction>(producerConfig())) {
            for (var row : List.of(declined, transfer)) {
                producer.send(new ProducerRecord<>(
                        properties.topics().cdcTransactions(), row.getId().toString(), row));
            }
        }

        try (var consumer = new KafkaConsumer<String, CompletedTransaction>(consumerConfig())) {
            consumer.subscribe(List.of(properties.topics().p2pTransfers()));
            var received = new ArrayList<CompletedTransaction>();
            await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(record -> received.add(record.value()));
                assertThat(received)
                        .extracting(CompletedTransaction::getTransactionId)
                        .containsExactly(transfer.getId());
            });
        }
    }

    private Map<String, Object> producerConfig() {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                String.join(",", kafka.getBootstrapServers()),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class,
                KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                REGISTRY);
    }

    private Map<String, Object> consumerConfig() {
        return Map.of(
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
                true);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class Infrastructure {

        @Bean
        @ServiceConnection
        KafkaContainer kafkaContainer() {
            return new KafkaContainer(DockerImageName.parse("apache/kafka-native:4.3.1"));
        }

        @Bean
        NewTopic cdcTransactionsTopic(StreamsProperties properties) {
            return TopicBuilder.name(properties.topics().cdcTransactions())
                    .partitions(3)
                    .replicas(1)
                    .build();
        }
    }
}
