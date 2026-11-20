package com.pinchez.streams.handler;

import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.processor.api.Record;

/**
 * Alerting is best effort: a record that breaks a fraud rule is dead-lettered and the rule carries on. Everything else
 * (deduplication, ledger, balances, joins) moves money, and skipping a record there would leave balances silently
 * wrong, so those failures stop the application. Processors are told apart by the naming convention of the topology.
 */
@Slf4j
public class BestEffortProcessingExceptionHandler implements ProcessingExceptionHandler {

    static final Set<String> BEST_EFFORT_PREFIXES = Set.of(
            "velocity-",
            "rolling-spend-",
            "new-account-",
            "night-betting-",
            "round-trip-",
            "merge-",
            "fraud-alert-",
            "detect-",
            "account-activity-");

    private String deadLetterTopic;

    @Override
    public void configure(Map<String, ?> configs) {
        var topic = configs.get(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG);
        deadLetterTopic = topic == null ? null : topic.toString();
    }

    @Override
    public Response handleError(ErrorHandlerContext context, Record<?, ?> record, Exception exception) {
        var processor = context.processorNodeId();
        if (BEST_EFFORT_PREFIXES.stream().noneMatch(processor::startsWith)) {
            log.error(
                    "Processor {} failed on {}-{}@{}; stopping",
                    processor,
                    context.topic(),
                    context.partition(),
                    context.offset(),
                    exception);
            return Response.fail();
        }
        log.warn(
                "Processor {} failed on {}-{}@{}; skipping: {}",
                processor,
                context.topic(),
                context.partition(),
                context.offset(),
                exception.toString());
        return Response.resume(DeadLetterRecords.of(
                deadLetterTopic,
                context.sourceRawKey(),
                context.sourceRawValue(),
                context.topic(),
                context.partition(),
                context.offset(),
                processor,
                exception));
    }
}
