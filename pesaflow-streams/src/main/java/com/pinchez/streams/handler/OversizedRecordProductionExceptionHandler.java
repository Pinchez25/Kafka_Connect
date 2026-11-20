package com.pinchez.streams.handler;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

/**
 * A record too large for the broker will never fit, so it is dropped and logged (a dead letter would be just as large).
 * Any other send failure, and every serialization failure, stops the application.
 */
@Slf4j
public class OversizedRecordProductionExceptionHandler implements ProductionExceptionHandler {

    @Override
    public void configure(Map<String, ?> configs) {}

    @Override
    public Response handleError(
            ErrorHandlerContext context, ProducerRecord<byte[], byte[]> record, Exception exception) {
        if (exception instanceof RecordTooLargeException) {
            log.warn(
                    "Dropping a {}-byte record for {} produced by {}: {}",
                    record.value() == null ? 0 : record.value().length,
                    record.topic(),
                    context.processorNodeId(),
                    exception.getMessage());
            return Response.resume();
        }
        return Response.fail();
    }

    @Override
    public Response handleSerializationError(
            ErrorHandlerContext context,
            ProducerRecord record,
            Exception exception,
            SerializationExceptionOrigin origin) {
        return Response.fail();
    }
}
