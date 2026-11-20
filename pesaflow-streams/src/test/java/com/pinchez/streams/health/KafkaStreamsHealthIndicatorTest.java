package com.pinchez.streams.health;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.streams.KafkaStreams.State;
import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Status;

class KafkaStreamsHealthIndicatorTest {

    @Test
    void readyOnlyWhileRunning() {
        assertThat(KafkaStreamsHealthIndicator.of(State.RUNNING).getStatus()).isEqualTo(Status.UP);
    }

    @Test
    void notReadyWhileRestoringOrRebalancing() {
        assertThat(KafkaStreamsHealthIndicator.of(State.REBALANCING).getStatus())
                .isEqualTo(Status.DOWN);
    }

    @Test
    void notReadyBeforeStartOrAfterAFailure() {
        assertThat(KafkaStreamsHealthIndicator.of(null).getStatus()).isEqualTo(Status.DOWN);
        assertThat(KafkaStreamsHealthIndicator.of(State.CREATED).getStatus()).isEqualTo(Status.DOWN);
        assertThat(KafkaStreamsHealthIndicator.of(State.ERROR).getStatus()).isEqualTo(Status.DOWN);
        assertThat(KafkaStreamsHealthIndicator.of(State.PENDING_SHUTDOWN).getStatus())
                .isEqualTo(Status.DOWN);
    }
}
