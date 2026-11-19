package com.pinchez.streams.processor;

import com.pinchez.events.FraudAlert;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

public final class PesaflowHeaders {

    /** Added by Debezium's {@code ExtractNewRecordState} ({@code add.headers}). */
    public static final String DEBEZIUM_OP = "__op";

    public static final String DEBEZIUM_LSN = "__source_lsn";

    /** The CDC record a value came from, as {@code topic-partition@offset}. */
    public static final String SOURCE = "pesaflow-source";

    public static final String FRAUD_RULE = "pesaflow-fraud-rule";
    public static final String ALERT_ID = "pesaflow-alert-id";
    public static final String SEVERITY = "pesaflow-severity";

    private PesaflowHeaders() {}

    public static Optional<String> read(Headers headers, String name) {
        var header = headers.lastHeader(name);
        return header == null || header.value() == null
                ? Optional.empty()
                : Optional.of(new String(header.value(), StandardCharsets.UTF_8));
    }

    public static Headers with(Headers headers, String name, String value) {
        var copy = new RecordHeaders(headers.toArray());
        copy.remove(name);
        copy.add(name, value.getBytes(StandardCharsets.UTF_8));
        return copy;
    }

    /**
     * Fresh headers for an alert. Aggregation results inherit the headers of whichever record happened to trigger them,
     * so those are discarded rather than passed on.
     */
    public static Headers forAlert(FraudAlert alert, Severity severity) {
        var headers = new RecordHeaders();
        headers.add(FRAUD_RULE, alert.getRule().name().getBytes(StandardCharsets.UTF_8));
        headers.add(ALERT_ID, alert.getAlertId().toString().getBytes(StandardCharsets.UTF_8));
        headers.add(SEVERITY, severity.name().getBytes(StandardCharsets.UTF_8));
        return headers;
    }

    public enum Severity {
        HIGH,
        REVIEW
    }
}
