package com.pinchez.streams.handler;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;

/**
 * Records from outside the application (CDC, other teams) can be malformed, so they are skipped and dead-lettered. A
 * record on one of this application's own internal topics that won't deserialize means a bug or a corrupted topic, so
 * the application stops instead of silently losing state.
 */
@Slf4j
public class CdcDeserializationExceptionHandler implements DeserializationExceptionHandler {

    private String applicationId;
    private String deadLetterTopic;

    @Override
    public void configure(Map<String, ?> configs) {
        applicationId = String.valueOf(configs.get(StreamsConfig.APPLICATION_ID_CONFIG));
        var topic = configs.get(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG);
        deadLetterTopic = topic == null ? null : topic.toString();
    }

    @Override
    public Response handleError(
            ErrorHandlerContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {
        if (record.topic().startsWith(applicationId + "-")) {
            log.error(
                    "Cannot deserialize {}-{}@{} from an internal topic; stopping",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    exception);
            return Response.fail();
        }
        log.warn(
                "Skipping undeserializable record {}-{}@{} to {}: {}",
                record.topic(),
                record.partition(),
                record.offset(),
                deadLetterTopic,
                exception.getMessage());
        return Response.resume(DeadLetterRecords.of(
                deadLetterTopic,
                record.key(),
                record.value(),
                record.topic(),
                record.partition(),
                record.offset(),
                context.processorNodeId(),
                exception));
    }
}
