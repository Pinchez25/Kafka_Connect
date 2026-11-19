package com.pinchez.streams.topology.fraud;

import com.pinchez.cdc.Account;
import com.pinchez.events.EnrichedTransaction;
import com.pinchez.events.FraudAlert;
import com.pinchez.events.FraudRule;
import com.pinchez.events.RollingSpend;
import com.pinchez.streams.aggregator.RollingSpendAggregator;
import com.pinchez.streams.config.StreamsProperties;
import com.pinchez.streams.mapper.FraudAlertMapper;
import com.pinchez.streams.serde.AvroSerdeFactory;
import com.pinchez.streams.topology.CdcSources;
import com.pinchez.streams.topology.StoreNames;
import java.math.RoundingMode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.state.WindowStore;

/**
 * Sliding window + aggregate + emit-on-close + table join: an account sends a large share of its daily limit within a
 * short span. The limit comes from joining the accounts table.
 */
public final class RollingSpendRule implements FraudRuleTopology {

    private final StreamsProperties.Fraud.RollingSpend settings;
    private final AvroSerdeFactory serdes;

    public RollingSpendRule(StreamsProperties.Fraud.RollingSpend settings, AvroSerdeFactory serdes) {
        this.settings = settings;
        this.serdes = serdes;
    }

    @Override
    public KStream<String, FraudAlert> detect(KStream<String, EnrichedTransaction> enriched, CdcSources sources) {
        var spendSerde = serdes.<RollingSpend>valueSerde();

        return enriched.filter(
                        (_, transaction) -> FraudRuleTopology.isOutgoing(transaction),
                        Named.as("rolling-spend-outgoing-only"))
                .groupByKey(Grouped.with("rolling-spend-by-account", Serdes.String(), serdes.valueSerde()))
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(settings.window(), settings.grace()))
                .emitStrategy(EmitStrategy.onWindowClose())
                .aggregate(
                        RollingSpendAggregator::initial,
                        RollingSpendAggregator::add,
                        Named.as("rolling-spend-sum"),
                        Materialized.<String, RollingSpend, WindowStore<Bytes, byte[]>>as(StoreNames.ROLLING_SPEND)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(spendSerde))
                .toStream(Named.as("rolling-spend-windows"))
                .map(
                        (window, spend) -> KeyValue.pair(
                                window.key(),
                                RollingSpend.newBuilder(spend)
                                        .setWindowStart(window.window().startTime())
                                        .setWindowEnd(window.window().endTime())
                                        .build()),
                        Named.as("rolling-spend-by-account"))
                .join(
                        sources.accounts(),
                        (accountId, spend, account) -> alertIfOverLimit(accountId, spend, account),
                        Joined.<String, RollingSpend, Account>with(Serdes.String(), spendSerde, serdes.valueSerde())
                                .withName("rolling-spend-join-account"))
                .filter((_, alert) -> alert != null, Named.as("rolling-spend-exceeded"));
    }

    private FraudAlert alertIfOverLimit(String accountId, RollingSpend spend, Account account) {
        var threshold =
                account.getDailyLimit().multiply(settings.limitFraction()).setScale(2, RoundingMode.HALF_UP);
        if (spend.getAmount().compareTo(threshold) <= 0) {
            return null;
        }
        var windowEnd = spend.getWindowEnd().orElseThrow();
        return FraudAlertMapper.alert(FraudRule.ROLLING_SPEND, accountId, windowEnd, windowEnd)
                .setDescription("Sent %s within %s, over %s (%s of daily limit %s)"
                        .formatted(
                                spend.getAmount(),
                                FraudAlertMapper.humanize(settings.window()),
                                threshold,
                                settings.limitFraction(),
                                account.getDailyLimit()))
                .setWindowStart(spend.getWindowStart().orElse(null))
                .setWindowEnd(windowEnd)
                .setAmount(spend.getAmount())
                .setTransactionCount(spend.getTransactionCount())
                .build();
    }
}
