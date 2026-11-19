package com.pinchez.streams.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalTime;
import java.time.ZoneId;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("pesaflow.streams")
public record StreamsProperties(
        @NotBlank String schemaRegistryUrl,
        @NotNull @Valid Topics topics,
        @NotNull @Valid OutputTopics outputTopics,
        @NotNull @Valid AccountsTable accountsTable,
        @NotNull @Valid Activity activity,
        @NotNull @Valid Deduplication deduplication,
        @NotNull @Valid Fraud fraud) {

    public record Topics(
            @NotBlank String cdcAccounts,
            @NotBlank String cdcMerchants,
            @NotBlank String cdcTransactions,
            @NotBlank String p2pTransfers,
            @NotBlank String topUps,
            @NotBlank String merchantPayments,
            @NotBlank String enrichedTransactions,
            @NotBlank String balances,
            @NotBlank String transactionSenders,
            @NotBlank String fraudAlerts) {}

    public record OutputTopics(
            @Positive int partitions, @Positive short replicationFactor) {}

    /**
     * @param versioned keep every version of each account so joins see the account as it was at the transaction's
     *     timestamp, instead of its latest state
     * @param historyRetention how far back versions are kept
     */
    public record AccountsTable(boolean versioned, @NotNull Duration historyRetention) {}

    public record Activity(
            @NotNull Duration window,
            @NotNull Duration grace,
            @NotNull Duration retention) {}

    /** Transaction ids are remembered for {@code retention} of stream time; older duplicates get through. */
    public record Deduplication(@NotNull Duration retention) {}

    public record Fraud(
            @NotNull ZoneId zoneId,
            @NotNull @Valid TransferVelocity transferVelocity,
            @NotNull @Valid RollingSpend rollingSpend,
            @NotNull @Valid NewAccount newAccount,
            @NotNull @Valid NightBetting nightBetting,
            @NotNull @Valid RoundTrip roundTrip) {

        public record TransferVelocity(
                @NotNull Duration window,
                @NotNull Duration grace,
                @Positive int maxTransfers) {}

        /** Alerts when an account sends more than {@code limitFraction} of its daily limit within {@code window}. */
        public record RollingSpend(
                @NotNull Duration window,
                @NotNull Duration grace,
                @DecimalMin("0.01") @DecimalMax("1.0") BigDecimal limitFraction) {}

        public record NewAccount(
                @NotNull Duration maxAge, @NotNull @Positive BigDecimal amountThreshold) {}

        public record NightBetting(
                @NotNull LocalTime start,
                @NotNull LocalTime end,
                @NotNull Duration inactivityGap,
                @NotNull Duration grace,
                @Positive int minPayments) {}

        /** Alerts when the return leg is within {@code maxAmountDifference} (a fraction) of the outbound amount. */
        public record RoundTrip(
                @NotNull Duration window,
                @NotNull Duration grace,
                @DecimalMin("0.0") @DecimalMax("1.0") BigDecimal maxAmountDifference) {}
    }
}
