package com.pinchez.streams.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("pesaflow.streams")
public record StreamsProperties(
        @NotBlank String schemaRegistryUrl,
        @NotNull @Valid Topics topics,
        @NotNull @Valid OutputTopics outputTopics,
        @NotNull @Valid AccountsTable accountsTable) {

    public record Topics(
            @NotBlank String cdcAccounts,
            @NotBlank String cdcMerchants,
            @NotBlank String cdcTransactions,
            @NotBlank String p2pTransfers,
            @NotBlank String topUps,
            @NotBlank String merchantPayments,
            @NotBlank String enrichedTransactions,
            @NotBlank String balances,
            @NotBlank String transactionSenders) {}

    public record OutputTopics(
            @Positive int partitions, @Positive short replicationFactor) {}

    /**
     * @param versioned keep every version of each account so joins see the account as it was at the transaction's
     *     timestamp, instead of its latest state
     * @param historyRetention how far back versions are kept
     */
    public record AccountsTable(boolean versioned, @NotNull Duration historyRetention) {}
}
