package com.pinchez.streams.mapper;

import com.pinchez.events.FraudAlert;
import com.pinchez.events.FraudRule;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

public final class FraudAlertMapper {

    private FraudAlertMapper() {}

    public static String humanize(Duration duration) {
        if (duration.toHours() >= 1 && duration.toMinutesPart() == 0) {
            return duration.toHours() + " h";
        }
        return duration.toSecondsPart() == 0 ? duration.toMinutes() + " min" : duration.toSeconds() + " s";
    }

    /** Same rule, account and evidence always give the same alert id. */
    public static FraudAlert.Builder alert(FraudRule rule, String accountId, Instant detectedAt, Object evidenceKey) {
        var alertId = UUID.nameUUIDFromBytes(
                "%s|%s|%s".formatted(rule, accountId, evidenceKey).getBytes(StandardCharsets.UTF_8));
        return FraudAlert.newBuilder()
                .setAlertId(alertId)
                .setRule(rule)
                .setAccountId(UUID.fromString(accountId))
                .setDetectedAt(detectedAt);
    }
}
