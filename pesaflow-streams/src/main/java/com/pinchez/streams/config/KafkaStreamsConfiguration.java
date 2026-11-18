package com.pinchez.streams.config;

import com.pinchez.cdc.Transaction;
import com.pinchez.events.CompletedTransaction;
import com.pinchez.streams.serde.AvroSerdeFactory;
import com.pinchez.streams.topology.TransactionRoutingTopology;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.List;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration(proxyBeanMethods = false)
@EnableKafkaStreams
class KafkaStreamsConfiguration {

    @Bean
    AvroSerdeFactory avroSerdeFactory(StreamsProperties properties) {
        return new AvroSerdeFactory(properties.schemaRegistryUrl());
    }

    @Bean
    SpecificAvroSerde<Transaction> cdcTransactionSerde(AvroSerdeFactory serdes) {
        return serdes.valueSerde();
    }

    @Bean
    SpecificAvroSerde<CompletedTransaction> completedTransactionSerde(AvroSerdeFactory serdes) {
        return serdes.valueSerde();
    }

    @Bean
    TransactionRoutingTopology transactionRoutingTopology(
            StreamsBuilder builder,
            StreamsProperties properties,
            SpecificAvroSerde<Transaction> cdcTransactionSerde,
            SpecificAvroSerde<CompletedTransaction> completedTransactionSerde) {
        var topology =
                new TransactionRoutingTopology(properties.topics(), cdcTransactionSerde, completedTransactionSerde);
        topology.addTo(builder);
        return topology;
    }

    @Bean
    KafkaAdmin.NewTopics outputTopics(StreamsProperties properties) {
        var topics = properties.topics();
        var output = properties.outputTopics();
        return new KafkaAdmin.NewTopics(
                List.of(topics.p2pTransfers(), topics.topUps(), topics.merchantPayments()).stream()
                        .map(name -> TopicBuilder.name(name)
                                .partitions(output.partitions())
                                .replicas(output.replicationFactor())
                                .build())
                        .toArray(NewTopic[]::new));
    }
}
