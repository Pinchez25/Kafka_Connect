package com.pinchez.streams.topology.fraud;

import com.pinchez.events.EnrichedTransaction;
import com.pinchez.events.FraudAlert;
import com.pinchez.events.FraudRule;
import com.pinchez.events.MerchantCategory;
import com.pinchez.streams.config.StreamsProperties;
import com.pinchez.streams.mapper.FraudAlertMapper;
import com.pinchez.streams.serde.AvroSerdeFactory;
import com.pinchez.streams.topology.CdcSources;
import com.pinchez.streams.topology.StoreNames;
import java.time.Instant;
import java.time.ZoneId;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.state.SessionStore;

/** Session window + count + suppress: a burst of betting payments during the night, ended by a quiet gap. */
public final class NightBettingBurstRule implements FraudRuleTopology {

    private final StreamsProperties.Fraud.NightBetting settings;
    private final ZoneId zoneId;
    private final AvroSerdeFactory serdes;

    public NightBettingBurstRule(
            StreamsProperties.Fraud.NightBetting settings, ZoneId zoneId, AvroSerdeFactory serdes) {
        this.settings = settings;
        this.zoneId = zoneId;
        this.serdes = serdes;
    }

    @Override
    public KStream<String, FraudAlert> detect(KStream<String, EnrichedTransaction> enriched, CdcSources sources) {
        return enriched.filter((_, transaction) -> isNightBet(transaction), Named.as("night-betting-only"))
                .groupByKey(Grouped.with("night-betting-by-account", Serdes.String(), serdes.valueSerde()))
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(settings.inactivityGap(), settings.grace()))
                .count(
                        Named.as("night-betting-count"),
                        Materialized.<String, Long, SessionStore<Bytes, byte[]>>as(StoreNames.NIGHT_BETTING)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long()))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded())
                        .withName("night-betting-final-count"))
                .toStream(Named.as("night-betting-sessions"))
                .filter((_, count) -> count != null && count >= settings.minPayments(), Named.as("night-betting-burst"))
                .map(
                        (session, count) -> KeyValue.pair(
                                session.key(),
                                FraudAlertMapper.alert(
                                                FraudRule.NIGHT_BETTING_BURST,
                                                session.key(),
                                                session.window().endTime(),
                                                session.window().start())
                                        .setDescription(
                                                "%d betting payments in one night-time session".formatted(count))
                                        .setWindowStart(session.window().startTime())
                                        .setWindowEnd(session.window().endTime())
                                        .setTransactionCount(count)
                                        .build()),
                        Named.as("night-betting-alert"));
    }

    private boolean isNightBet(EnrichedTransaction transaction) {
        return transaction
                        .getMerchantCategory()
                        .filter(MerchantCategory.BETTING::equals)
                        .isPresent()
                && isNight(transaction.getOccurredAt());
    }

    private boolean isNight(Instant instant) {
        var time = instant.atZone(zoneId).toLocalTime();
        var start = settings.start();
        var end = settings.end();
        return start.isBefore(end)
                ? !time.isBefore(start) && time.isBefore(end)
                : !time.isBefore(start) || time.isBefore(end);
    }
}
