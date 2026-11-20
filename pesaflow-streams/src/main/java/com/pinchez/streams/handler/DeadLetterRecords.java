package com.pinchez.streams.handler;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.jspecify.annotations.Nullable;

/** Dead-letter records carry the original bytes plus where they came from and what went wrong. */
final class DeadLetterRecords {

    static final String EXCEPTION = "__streams.errors.exception";
    static final String MESSAGE = "__streams.errors.message";
    static final String STACKTRACE = "__streams.errors.stacktrace";
    static final String TOPIC = "__streams.errors.topic";
    static final String PARTITION = "__streams.errors.partition";
    static final String OFFSET = "__streams.errors.offset";
    static final String PROCESSOR = "pesaflow-errors-processor";

    private DeadLetterRecords() {}

    static List<ProducerRecord<byte[], byte[]>> of(
            @Nullable String deadLetterTopic,
            byte @Nullable [] key,
            byte @Nullable [] value,
            String topic,
            int partition,
            long offset,
            @Nullable String processor,
            Exception exception) {
        if (deadLetterTopic == null || value == null) {
            return List.of();
        }
        var headers = new RecordHeaders();
        add(headers, EXCEPTION, exception.getClass().getName());
        add(headers, MESSAGE, String.valueOf(exception.getMessage()));
        add(headers, STACKTRACE, stackTrace(exception));
        add(headers, TOPIC, topic);
        add(headers, PARTITION, String.valueOf(partition));
        add(headers, OFFSET, String.valueOf(offset));
        if (processor != null) {
            add(headers, PROCESSOR, processor);
        }
        return List.of(new ProducerRecord<>(deadLetterTopic, null, key, value, headers));
    }

    private static void add(RecordHeaders headers, String name, String value) {
        headers.add(name, value.getBytes(StandardCharsets.UTF_8));
    }

    private static String stackTrace(Exception exception) {
        var writer = new StringWriter();
        exception.printStackTrace(new PrintWriter(writer));
        return writer.toString();
    }
}
