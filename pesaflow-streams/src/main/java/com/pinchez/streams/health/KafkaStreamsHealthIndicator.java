package com.pinchez.streams.health;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

/**
 * Part of the readiness group only. Ready means RUNNING: while restoring state from changelogs the client reports
 * REBALANCING, sometimes for minutes, and its stores cannot be queried. Restarting it would not help, so liveness
 * ignores this.
 */
@Component("kafkaStreams")
@RequiredArgsConstructor
public class KafkaStreamsHealthIndicator implements HealthIndicator {

    private final StreamsBuilderFactoryBean factoryBean;

    @Override
    public Health health() {
        var streams = factoryBean.getKafkaStreams();
        return of(streams == null ? null : streams.state());
    }

    static Health of(KafkaStreams.@Nullable State state) {
        if (state == null) {
            return Health.down().withDetail("state", "NOT_STARTED").build();
        }
        var health = state == KafkaStreams.State.RUNNING ? Health.up() : Health.down();
        return health.withDetail("state", state.name()).build();
    }
}
