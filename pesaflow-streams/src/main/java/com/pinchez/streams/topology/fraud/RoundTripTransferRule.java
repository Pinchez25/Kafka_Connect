package com.pinchez.streams.topology.fraud;

import com.pinchez.events.EnrichedTransaction;
import com.pinchez.events.FraudAlert;
import com.pinchez.events.FraudRule;
import com.pinchez.events.TransactionType;
import com.pinchez.streams.config.StreamsProperties;
import com.pinchez.streams.mapper.FraudAlertMapper;
import com.pinchez.streams.serde.AvroSerdeFactory;
import com.pinchez.streams.topology.CdcSources;
import com.pinchez.streams.topology.StoreNames;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.StreamJoined;

/**
 * Stream–stream windowed join: A sends to B and B sends back to A within the window. Each transfer is keyed by its
 * direction ("A|B") on one side and the reverse direction on the other, so a transfer meets its return leg.
 */
public final class RoundTripTransferRule implements FraudRuleTopology {

    private final StreamsProperties.Fraud.RoundTrip settings;
    private final AvroSerdeFactory serdes;

    public RoundTripTransferRule(StreamsProperties.Fraud.RoundTrip settings, AvroSerdeFactory serdes) {
        this.settings = settings;
        this.serdes = serdes;
    }

    @Override
    public KStream<String, FraudAlert> detect(KStream<String, EnrichedTransaction> enriched, CdcSources sources) {
        var transfers = enriched.filter(
                (_, transaction) -> transaction.getType() == TransactionType.P2P
                        && transaction.getCounterpartyAccountId().isPresent(),
                Named.as("round-trip-transfers-only"));
        var outbound = transfers.selectKey(
                (_, transfer) -> direction(transfer.getAccountId(), counterparty(transfer)),
                Named.as("round-trip-key-outbound"));
        var returning = transfers.selectKey(
                (_, transfer) -> direction(counterparty(transfer), transfer.getAccountId()),
                Named.as("round-trip-key-return"));

        return returning
                .join(
                        outbound,
                        this::alertIfReturnLeg,
                        JoinWindows.ofTimeDifferenceAndGrace(settings.window(), settings.grace()),
                        StreamJoined.<String, EnrichedTransaction, EnrichedTransaction>with(
                                        Serdes.String(), serdes.valueSerde(), serdes.valueSerde())
                                .withName(StoreNames.ROUND_TRIP)
                                .withStoreName(StoreNames.ROUND_TRIP))
                .filter((_, alert) -> alert != null, Named.as("round-trip-detected"))
                .selectKey((_, alert) -> alert.getAccountId().toString(), Named.as("round-trip-key-by-sender"));
    }

    /**
     * Both legs match each other once in each direction; only the later leg completes the round trip, and only if
     * roughly the same amount came back.
     */
    private FraudAlert alertIfReturnLeg(EnrichedTransaction returnLeg, EnrichedTransaction outboundLeg) {
        if (!returnLeg.getOccurredAt().isAfter(outboundLeg.getOccurredAt())
                || !similarAmounts(outboundLeg, returnLeg)) {
            return null;
        }
        var originalSender = outboundLeg.getAccountId().toString();
        return FraudAlertMapper.alert(
                        FraudRule.ROUND_TRIP_TRANSFER,
                        originalSender,
                        returnLeg.getOccurredAt(),
                        returnLeg.getTransactionId())
                .setDescription("Sent %s to %s and received %s back"
                        .formatted(outboundLeg.getAmount(), counterparty(outboundLeg), returnLeg.getAmount()))
                .setAmount(outboundLeg.getAmount())
                .setTransactionIds(List.of(outboundLeg.getTransactionId(), returnLeg.getTransactionId()))
                .build();
    }

    private boolean similarAmounts(EnrichedTransaction outboundLeg, EnrichedTransaction returnLeg) {
        var outbound = outboundLeg.getAmount();
        var difference = outbound.subtract(returnLeg.getAmount()).abs();
        return difference.compareTo(outbound.multiply(settings.maxAmountDifference())) <= 0;
    }

    private static UUID counterparty(EnrichedTransaction transfer) {
        return transfer.getCounterpartyAccountId().orElseThrow();
    }

    private static String direction(UUID from, UUID to) {
        return from + "|" + to;
    }
}
