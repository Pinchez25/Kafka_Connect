package com.pinchez.streams.processor;

import com.pinchez.events.FraudAlert;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

public final class AlertHeadersProcessor implements FixedKeyProcessor<String, FraudAlert, FraudAlert> {

    private FixedKeyProcessorContext<String, FraudAlert> context;

    @Override
    public void init(FixedKeyProcessorContext<String, FraudAlert> context) {
        this.context = context;
    }

    @Override
    public void process(FixedKeyRecord<String, FraudAlert> record) {
        context.forward(record.withHeaders(PesaflowHeaders.forAlert(record.value(), PesaflowHeaders.Severity.HIGH)));
    }
}
