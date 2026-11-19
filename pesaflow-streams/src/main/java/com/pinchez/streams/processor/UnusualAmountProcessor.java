package com.pinchez.streams.processor;

import com.pinchez.events.EnrichedTransaction;
import com.pinchez.events.FraudAlert;
import com.pinchez.events.FraudRule;
import com.pinchez.events.SpendingBaseline;
import com.pinchez.streams.config.StreamsProperties;
import com.pinchez.streams.mapper.FraudAlertMapper;
import com.pinchez.streams.processor.PesaflowHeaders.Severity;
import com.pinchez.streams.topology.fraud.FraudRuleTopology;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Compares each outgoing amount with the account's own average so far. Depending on how far above it is, the alert is
 * forwarded to one named child (the fraud alerts sink) or the other (the review queue sink).
 */
public final class UnusualAmountProcessor implements Processor<String, EnrichedTransaction, String, FraudAlert> {

    private final StreamsProperties.Fraud.UnusualAmount settings;
    private final String storeName;
    private final String highSeverityChild;
    private final String reviewChild;
    private ProcessorContext<String, FraudAlert> context;
    private KeyValueStore<String, SpendingBaseline> baselines;

    public UnusualAmountProcessor(
            StreamsProperties.Fraud.UnusualAmount settings,
            String storeName,
            String highSeverityChild,
            String reviewChild) {
        this.settings = settings;
        this.storeName = storeName;
        this.highSeverityChild = highSeverityChild;
        this.reviewChild = reviewChild;
    }

    @Override
    public void init(ProcessorContext<String, FraudAlert> context) {
        this.context = context;
        this.baselines = context.getStateStore(storeName);
    }

    @Override
    public void process(Record<String, EnrichedTransaction> record) {
        var transaction = record.value();
        if (record.key() == null || transaction == null || !FraudRuleTopology.isOutgoing(transaction)) {
            return;
        }
        var baseline = baselines.get(record.key());
        if (baseline != null && baseline.getTransactionCount() >= settings.minHistory()) {
            var average = baseline.getTotalAmount()
                    .divide(BigDecimal.valueOf(baseline.getTransactionCount()), 2, RoundingMode.HALF_EVEN);
            if (average.signum() > 0) {
                var multiple = transaction.getAmount().divide(average, 1, RoundingMode.HALF_EVEN);
                if (multiple.compareTo(settings.highMultiplier()) >= 0) {
                    forward(record, alert(record.key(), transaction, average, multiple), Severity.HIGH);
                } else if (multiple.compareTo(settings.reviewMultiplier()) >= 0) {
                    forward(record, alert(record.key(), transaction, average, multiple), Severity.REVIEW);
                }
            }
        }
        baselines.put(record.key(), updated(baseline, transaction));
    }

    private void forward(Record<String, EnrichedTransaction> input, FraudAlert alert, Severity severity) {
        var child = severity == Severity.HIGH ? highSeverityChild : reviewChild;
        context.forward(
                new Record<>(input.key(), alert, input.timestamp(), PesaflowHeaders.forAlert(alert, severity)), child);
    }

    private static FraudAlert alert(
            String accountId, EnrichedTransaction transaction, BigDecimal average, BigDecimal multiple) {
        return FraudAlertMapper.alert(
                        FraudRule.UNUSUAL_AMOUNT,
                        accountId,
                        transaction.getOccurredAt(),
                        transaction.getTransactionId())
                .setDescription("Sent %s, %s× this account's average of %s"
                        .formatted(transaction.getAmount(), multiple, average))
                .setAmount(transaction.getAmount())
                .setTransactionIds(List.of(transaction.getTransactionId()))
                .build();
    }

    private static SpendingBaseline updated(SpendingBaseline baseline, EnrichedTransaction transaction) {
        var builder = baseline == null
                ? SpendingBaseline.newBuilder().setTransactionCount(0).setTotalAmount(BigDecimal.ZERO)
                : SpendingBaseline.newBuilder(baseline);
        return builder.setTransactionCount(builder.getTransactionCount() + 1)
                .setTotalAmount(builder.getTotalAmount().add(transaction.getAmount()))
                .build();
    }
}
