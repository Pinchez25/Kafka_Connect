package com.pinchez.streams.handler;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.processor.TaskId;
import org.junit.jupiter.api.Test;

class ErrorHandlerPolicyTest {

    private static final Map<String, Object> CONFIG = Map.of(
            StreamsConfig.APPLICATION_ID_CONFIG, "pesaflow-streams",
            StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "pesaflow.dlq.streams");

    private static final byte[] VALUE = "bad".getBytes(StandardCharsets.UTF_8);

    @Test
    void externalTopicsAreDeadLetteredButInternalTopicsStopTheApplication() {
        var handler = new CdcDeserializationExceptionHandler();
        handler.configure(CONFIG);

        var external = handler.handleError(
                context("cdc-transactions", null, null),
                record("pesaflow.transactions"),
                new SerializationException("bad magic byte"));
        var internal = handler.handleError(
                context("ledger-entries-by-account-repartition-source", null, null),
                record("pesaflow-streams-ledger-entries-by-account-repartition"),
                new SerializationException("bad magic byte"));

        assertThat(external.result()).isEqualTo(DeserializationExceptionHandler.Result.RESUME);
        assertThat(external.deadLetterQueueRecords())
                .singleElement()
                .satisfies(dead -> assertThat(dead.topic()).isEqualTo("pesaflow.dlq.streams"));
        assertThat(internal.result()).isEqualTo(DeserializationExceptionHandler.Result.FAIL);
    }

    @Test
    void fraudRulesAreBestEffortButTheMoneyPathIsNot() {
        var handler = new BestEffortProcessingExceptionHandler();
        handler.configure(CONFIG);
        var boom = new IllegalStateException("boom");

        var fraudRule = handler.handleError(context("velocity-count", null, VALUE), null, boom);
        var ledger = handler.handleError(context("aggregate-balance", null, VALUE), null, boom);
        var punctuator = handler.handleError(context("detect-dormant-accounts", null, null), null, boom);

        assertThat(fraudRule.result()).isEqualTo(ProcessingExceptionHandler.Result.RESUME);
        assertThat(fraudRule.deadLetterQueueRecords()).hasSize(1);
        assertThat(ledger.result()).isEqualTo(ProcessingExceptionHandler.Result.FAIL);
        assertThat(punctuator.result()).isEqualTo(ProcessingExceptionHandler.Result.RESUME);
        assertThat(punctuator.deadLetterQueueRecords())
                .as("a punctuation has no source record to dead-letter")
                .isEmpty();
    }

    @Test
    void threadsAreReplacedUntilFailuresRepeatTooOften() {
        var clock = new MovableClock(Instant.parse("2026-11-19T09:00:00Z"));
        var handler = new RestartLimitingUncaughtExceptionHandler(2, Duration.ofMinutes(10), clock);
        var failure = new IllegalStateException("boom");

        assertThat(handler.handle(failure)).isEqualTo(StreamThreadExceptionResponse.REPLACE_THREAD);
        assertThat(handler.handle(failure)).isEqualTo(StreamThreadExceptionResponse.REPLACE_THREAD);
        assertThat(handler.handle(failure)).isEqualTo(StreamThreadExceptionResponse.SHUTDOWN_CLIENT);
    }

    @Test
    void oldFailuresStopCountingOnceTheWindowHasPassed() {
        var clock = new MovableClock(Instant.parse("2026-11-19T09:00:00Z"));
        var handler = new RestartLimitingUncaughtExceptionHandler(2, Duration.ofMinutes(10), clock);
        var failure = new IllegalStateException("boom");
        handler.handle(failure);
        handler.handle(failure);

        clock.advance(Duration.ofMinutes(11));

        assertThat(handler.handle(failure)).isEqualTo(StreamThreadExceptionResponse.REPLACE_THREAD);
    }

    private static final class MovableClock extends Clock {

        private Instant now;

        MovableClock(Instant now) {
            this.now = now;
        }

        void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }

    private static ConsumerRecord<byte[], byte[]> record(String topic) {
        return new ConsumerRecord<>(topic, 1, 42L, null, VALUE);
    }

    private static ErrorHandlerContext context(String processor, byte[] key, byte[] value) {
        return new ErrorHandlerContext() {
            @Override
            public String topic() {
                return "pesaflow.transactions";
            }

            @Override
            public int partition() {
                return 1;
            }

            @Override
            public long offset() {
                return 42L;
            }

            @Override
            public Headers headers() {
                return new RecordHeaders();
            }

            @Override
            public String processorNodeId() {
                return processor;
            }

            @Override
            public TaskId taskId() {
                return new TaskId(0, 1);
            }

            @Override
            public long timestamp() {
                return 0L;
            }

            @Override
            public byte[] sourceRawKey() {
                return key;
            }

            @Override
            public byte[] sourceRawValue() {
                return value;
            }
        };
    }
}
