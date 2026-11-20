package com.pinchez.streams.config;

import com.pinchez.streams.handler.RestartLimitingUncaughtExceptionHandler;
import com.pinchez.streams.serde.AvroSerdeFactory;
import com.pinchez.streams.topology.PesaflowTopology;
import java.time.Clock;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.streams.KafkaStreamsInteractiveQueryService;

@Configuration(proxyBeanMethods = false)
@EnableKafkaStreams
class KafkaStreamsConfiguration {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    AvroSerdeFactory avroSerdeFactory(StreamsProperties properties) {
        return new AvroSerdeFactory(properties.schemaRegistryUrl());
    }

    @Bean
    PesaflowTopology pesaflowTopology(StreamsBuilder builder, StreamsProperties properties, AvroSerdeFactory serdes) {
        var topology = new PesaflowTopology(properties, serdes);
        topology.addTo(builder);
        return topology;
    }

    /** Lazy: the topology bean needs the StreamsBuilder, which this configurer helps create. */
    @Bean
    StreamsBuilderFactoryBeanConfigurer processorApiTopology(ObjectProvider<PesaflowTopology> pesaflowTopology) {
        return factoryBean -> factoryBean.setInfrastructureCustomizer(new KafkaStreamsInfrastructureCustomizer() {
            @Override
            public void configureTopology(Topology topology) {
                pesaflowTopology.getObject().addTo(topology);
            }
        });
    }

    /**
     * Kafka Streams normally stays in the group on close, so a planned stop waits out the session timeout before its
     * tasks move. With standby replicas a rebalance is cheap, so leaving at once is the better trade.
     */
    @Bean
    StreamsBuilderFactoryBeanConfigurer streamsLifecycle(StreamsProperties properties, Clock clock) {
        var errors = properties.errors();
        return factoryBean -> {
            factoryBean.setLeaveGroupOnClose(true);
            factoryBean.setStreamsUncaughtExceptionHandler(new RestartLimitingUncaughtExceptionHandler(
                    errors.maxThreadReplacements(), errors.threadReplacementWindow(), clock));
        };
    }

    @Bean
    KafkaStreamsInteractiveQueryService interactiveQueryService(StreamsBuilderFactoryBean streamsBuilder) {
        return new KafkaStreamsInteractiveQueryService(streamsBuilder);
    }

    @Bean
    KafkaAdmin.NewTopics outputTopics(StreamsProperties properties) {
        var topics = properties.topics();
        var streams = Stream.of(
                        topics.p2pTransfers(),
                        topics.topUps(),
                        topics.merchantPayments(),
                        topics.enrichedTransactions(),
                        topics.fraudAlerts(),
                        topics.fraudReview(),
                        topics.dormantAccounts(),
                        topics.deadLetters())
                .map(name -> topic(properties, name).build());
        var tables = Stream.of(topics.balances(), topics.transactionSenders())
                .map(name -> topic(properties, name)
                        .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
                        .build());
        return new KafkaAdmin.NewTopics(Stream.concat(streams, tables).toArray(NewTopic[]::new));
    }

    private static TopicBuilder topic(StreamsProperties properties, String name) {
        var output = properties.outputTopics();
        return TopicBuilder.name(name).partitions(output.partitions()).replicas(output.replicationFactor());
    }
}
